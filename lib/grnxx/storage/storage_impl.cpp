/*
  Copyright (C) 2012-2013  Brazil, Inc.

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public
  License as published by the Free Software Foundation; either
  version 2.1 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public
  License along with this library; if not, write to the Free Software
  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
*/
#include "grnxx/storage/storage_impl.hpp"

#include "grnxx/intrinsic.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/slice.hpp"
#include "grnxx/storage/chunk.hpp"
#include "grnxx/storage/chunk_index.hpp"
#include "grnxx/storage/file.hpp"
#include "grnxx/storage/header.hpp"
#include "grnxx/storage/node_header.hpp"
#include "grnxx/storage/path.hpp"

namespace grnxx {
namespace storage {
namespace {

constexpr uint32_t MAX_NODE_ID = STORAGE_INVALID_NODE_ID - 1;

constexpr uint64_t CHUNK_UNIT_SIZE   = 1 << 16;
constexpr uint64_t NODE_UNIT_SIZE    = 1 << 12;
constexpr uint64_t HEADER_CHUNK_SIZE = CHUNK_UNIT_SIZE;

constexpr uint16_t NODE_HEADER_CHUNK_UNIT_SIZE =
    CHUNK_UNIT_SIZE / NODE_HEADER_SIZE;

constexpr uint16_t MAX_NUM_NODE_HEADER_CHUNKS = 32;
constexpr uint16_t MAX_NUM_NODE_BODY_CHUNKS   =
    (HEADER_CHUNK_SIZE / CHUNK_INDEX_SIZE) - MAX_NUM_NODE_HEADER_CHUNKS;

static_assert(MAX_NUM_NODE_BODY_CHUNKS >= 2000,
              "MAX_NUM_NODE_BODY_CHUNKS < 2000");

// TODO: Define constant values.

}  // namespace

StorageImpl::StorageImpl()
    : Storage(),
      path_(),
      flags_(STORAGE_DEFAULT),
      header_(nullptr),
      node_header_chunk_indexes_(nullptr),
      node_body_chunk_indexes_(nullptr),
      files_(),
      header_chunk_(),
      node_header_chunks_(),
      node_body_chunks_(),
      clock_() {}

StorageImpl::~StorageImpl() {}

StorageImpl *StorageImpl::create(const char *path, StorageFlags flags,
                                 const StorageOptions &options) {
  if (!options.is_valid()) {
    GRNXX_ERROR() << "invalid argument: options = " << options;
    return nullptr;
  }
  std::unique_ptr<StorageImpl> storage(new (std::nothrow) StorageImpl);
  if (!storage) {
    GRNXX_ERROR() << "new grnxx::storage::StorageImpl failed";
    return nullptr;
  }
  if (path || (flags & STORAGE_TEMPORARY)) {
    if (!storage->create_file_backed_storage(path, flags, options)) {
      return nullptr;
    }
  } else {
    if (!storage->create_anonymous_storage(flags, options)) {
      return nullptr;
    }
  }
  return storage.release();
}

StorageImpl *StorageImpl::open(const char *path, StorageFlags flags) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = nullptr";
    return nullptr;
  }
  std::unique_ptr<StorageImpl> storage(new (std::nothrow) StorageImpl);
  if (!storage) {
    GRNXX_ERROR() << "new grnxx::storage::StorageImpl failed";
    return nullptr;
  }
  if (!storage->open_storage(path, flags)) {
    return nullptr;
  }
  return storage.release();
}

StorageImpl *StorageImpl::open_or_create(const char *path, StorageFlags flags,
                                         const StorageOptions &options) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = nullptr";
    return nullptr;
  }
  if (!options.is_valid()) {
    GRNXX_ERROR() << "invalid argument: options = " << options;
    return nullptr;
  }
  std::unique_ptr<StorageImpl> storage(new (std::nothrow) StorageImpl);
  if (!storage) {
    GRNXX_ERROR() << "new grnxx::storage::StorageImpl failed";
    return nullptr;
  }
  if (!storage->open_or_create_storage(path, flags, options)) {
    return nullptr;
  }
  return storage.release();
}

bool StorageImpl::exists(const char *path) {
  std::unique_ptr<Storage> storage(open(path, STORAGE_READ_ONLY));
  if (!storage) {
    return false;
  }
  return true;
}

bool StorageImpl::unlink(const char *path) {
  std::unique_ptr<Storage> storage(open(path, STORAGE_READ_ONLY));
  if (!storage) {
    return false;
  }
  // TODO: Remove files.
  return true;
}

StorageNode StorageImpl::create_node(uint32_t parent_node_id, uint64_t size) {
  if (parent_node_id >= header_->num_nodes) {
    GRNXX_ERROR() << "invalid argument: parent_node_id = " << parent_node_id
                  << ", num_nodes = " << header_->num_nodes;
    return StorageNode(nullptr, nullptr);
  } else if (size > header_->max_file_size) {
    GRNXX_ERROR() << "invalid argument: size = " << size
                  << ", max_file_size = " << header_->max_file_size;
    return StorageNode(nullptr, nullptr);
  }
  NodeHeader * const parent_node_header = get_node_header(parent_node_id);
  NodeHeader * const node_header = create_active_node(size);
  node_header->sibling_node_id = parent_node_header->child_node_id;
  parent_node_header->child_node_id = node_header->id;
  void * const body = get_node_body(node_header);
  if (!body) {
    return StorageNode(nullptr, nullptr);
  }
  return StorageNode(node_header, body);
}

StorageNode StorageImpl::open_node(uint32_t node_id) {
  NodeHeader * const node_header = get_node_header(node_id);
  if (!node_header) {
    return StorageNode(nullptr, nullptr);
  }
  void * const body = get_node_body(node_header);
  if (!body) {
    return StorageNode(nullptr, nullptr);
  }
  return StorageNode(node_header, body);
}

bool StorageImpl::unlink_node(uint32_t node_id) {
  if ((node_id == STORAGE_ROOT_NODE_ID) || (node_id >= header_->num_nodes)) {
    GRNXX_ERROR() << "invalid argument: node_id = " << node_id
                  << ", num_nodes = " << header_->num_nodes;
    return false;
  }
  // TODO
  return false;
}

bool StorageImpl::sweep(Duration lifetime) {
  // TODO
  return false;
}

const char *StorageImpl::path() const {
  return path_.get();
}

StorageFlags StorageImpl::flags() const {
  return flags_;
}

bool StorageImpl::create_file_backed_storage(const char *path,
                                             StorageFlags flags,
                                             const StorageOptions &options) {
  if (path) {
    path_.reset(Path::clone_path(path));
    if (!path_) {
      return false;
    }
  }
  if (flags & STORAGE_TEMPORARY) {
    flags_ |= STORAGE_TEMPORARY;
  }
  if (flags & STORAGE_HUGE_TLB) {
    flags_ |= STORAGE_HUGE_TLB;
  }
  FileFlags file_flags = FILE_DEFAULT;
  if (flags_ & STORAGE_TEMPORARY) {
    file_flags |= FILE_TEMPORARY;
  }
  std::unique_ptr<File> header_file(File::create(path, file_flags));
  if (!header_file) {
    return false;
  }
  if (!header_file->resize(HEADER_CHUNK_SIZE)) {
    return false;
  }
  std::unique_ptr<Chunk> header_chunk(
      create_chunk(header_file.get(), 0, HEADER_CHUNK_SIZE));
  header_ = static_cast<Header *>(header_chunk->address());
  *header_ = Header();
  header_->max_file_size = options.max_file_size & ~(CHUNK_UNIT_SIZE - 1);
  header_->max_num_files = options.max_num_files;
  header_->total_size = HEADER_CHUNK_SIZE;
  if (!prepare_pointers()) {
    return false;
  }
  prepare_indexes();
  files_[0] = std::move(header_file);
  header_chunk_ = std::move(header_chunk);
  if (!create_active_node(options.root_size)) {
    return false;
  }
  header_->validate();
  return true;
}

bool StorageImpl::create_anonymous_storage(StorageFlags flags,
                                           const StorageOptions &options) {
  flags_ |= STORAGE_ANONYMOUS;
  if (flags & STORAGE_HUGE_TLB) {
    flags_ |= STORAGE_HUGE_TLB;
  }
  std::unique_ptr<Chunk> header_chunk(
      create_chunk(nullptr, 0, HEADER_CHUNK_SIZE));
  header_ = static_cast<Header *>(header_chunk->address());
  *header_ = Header();
  header_->max_file_size = options.max_file_size & ~(CHUNK_UNIT_SIZE - 1);
  header_->max_num_files = options.max_num_files;
  header_->total_size = HEADER_CHUNK_SIZE;
  if (!prepare_pointers()) {
    return false;
  }
  prepare_indexes();
  header_chunk_ = std::move(header_chunk);
  if (!create_active_node(options.root_size)) {
    return false;
  }
  header_->validate();
  return true;
}

bool StorageImpl::open_storage(const char *path, StorageFlags flags) {
  path_.reset(Path::clone_path(path));
  if (!path_) {
    return false;
  }
  if (flags & STORAGE_READ_ONLY) {
    flags_ |= STORAGE_READ_ONLY;
  }
  if (flags & STORAGE_HUGE_TLB) {
    flags_ |= STORAGE_HUGE_TLB;
  }
  FileFlags file_flags = FILE_DEFAULT;
  if (flags_ & STORAGE_READ_ONLY) {
    file_flags |= FILE_READ_ONLY;
  }
  std::unique_ptr<File> header_file(File::open(path, file_flags));
  if (!header_file) {
    return false;
  }
  // TODO: If another thread or process is creating the storage?
  std::unique_ptr<Chunk> header_chunk(
      create_chunk(header_file.get(), 0, HEADER_CHUNK_SIZE));
  header_ = static_cast<Header *>(header_chunk->address());
  if (!header_->is_valid()) {
    return false;
  }
  if (!prepare_pointers()) {
    return false;
  }
  files_[0] = std::move(header_file);
  header_chunk_ = std::move(header_chunk);
  return true;
}

bool StorageImpl::open_or_create_storage(const char *path, StorageFlags flags,
                                         const StorageOptions &options) {
  path_.reset(Path::clone_path(path));
  if (!path_) {
    return false;
  }
  if (flags & STORAGE_HUGE_TLB) {
    flags_ |= STORAGE_HUGE_TLB;
  }
  // TODO
  return false;
}

bool StorageImpl::prepare_pointers() {
  node_header_chunk_indexes_ = reinterpret_cast<ChunkIndex *>(header_ + 1);
  node_body_chunk_indexes_ =
      node_header_chunk_indexes_ + MAX_NUM_NODE_HEADER_CHUNKS;
  if (~flags_ & STORAGE_ANONYMOUS) {
    files_.reset(
        new (std::nothrow) std::unique_ptr<File>[header_->max_num_files]);
    if (!files_) {
      GRNXX_ERROR() << "new std::unique_ptr<grnxx::storage::File>[] failed: "
                    << "size = " << header_->max_num_files;
      return false;
    }
  }
  node_header_chunks_.reset(
      new (std::nothrow) std::unique_ptr<Chunk>[MAX_NUM_NODE_HEADER_CHUNKS]);
  if (!node_header_chunks_) {
    GRNXX_ERROR() << "new std::unique_ptr<grnxx::storage::Chunk>[] failed: "
                  << "size = " << MAX_NUM_NODE_HEADER_CHUNKS;
    return false;
  }
  node_body_chunks_.reset(
      new (std::nothrow) std::unique_ptr<Chunk>[MAX_NUM_NODE_BODY_CHUNKS]);
  if (!node_header_chunks_) {
    GRNXX_ERROR() << "new std::unique_ptr<grnxx::storage::Chunk>[] failed: "
                  << "size = " << MAX_NUM_NODE_BODY_CHUNKS;
    return false;
  }
  return true;
}

void StorageImpl::prepare_indexes() {
  for (uint16_t i = 0; i < MAX_NUM_NODE_HEADER_CHUNKS; ++i) {
    node_header_chunk_indexes_[i] = ChunkIndex();
    node_header_chunk_indexes_[i].id = i;
  }
  for (uint16_t i = 0; i < MAX_NUM_NODE_BODY_CHUNKS; ++i) {
    node_body_chunk_indexes_[i] = ChunkIndex();
    node_body_chunk_indexes_[i].id = i;
  }
}

NodeHeader *StorageImpl::create_active_node(uint64_t size) {
  size = (size + NODE_UNIT_SIZE - 1) & ~(NODE_UNIT_SIZE - 1);
  NodeHeader *node_header = find_idle_node(size);
  if (!node_header) {
    node_header = create_idle_node(size);
    if (!node_header) {
      return nullptr;
    }
  }
  if (node_header->size > size) {
    if (!divide_idle_node(node_header, size)) {
      return nullptr;
    }
  }
  if (!activate_idle_node(node_header)) {
    return nullptr;
  }
  return node_header;
}

NodeHeader *StorageImpl::find_idle_node(uint64_t size) {
  for (size_t i = 0; i < NUM_IDLE_NODE_LISTS; ++i) {
    if (header_->oldest_idle_node_ids[i] != STORAGE_INVALID_NODE_ID) {
      NodeHeader * const node_header =
          get_node_header(header_->oldest_idle_node_ids[i]);
      if (node_header->size >= size) {
        return node_header;
      }
    }
  }
  return nullptr;
}

NodeHeader *StorageImpl::create_idle_node(uint64_t size) {
  NodeHeader *node_header;
  if (header_->latest_phantom_node_id != STORAGE_INVALID_NODE_ID) {
    node_header = get_node_header(header_->latest_phantom_node_id);
  } else {
    node_header = create_phantom_node();
  }
  if (!node_header) {
    return nullptr;
  }
  // TODO: Create a new body chunk.
  // TODO: Map the new chunk to the phantom node (-> idle).
  return nullptr;
}

bool StorageImpl::divide_idle_node(NodeHeader *node_header, uint64_t size) {
  // TODO
  return false;
}

bool StorageImpl::activate_idle_node(NodeHeader *node_header) {
  // TODO
  return false;
}

NodeHeader *StorageImpl::create_phantom_node() {
  const uint32_t node_id = header_->num_nodes;
  uint16_t remainder_file_id = 0;
  uint32_t remainder_offset = 0;
  uint64_t remainder_size = 0;
  if (header_->num_nodes == header_->max_num_nodes) {
    const uint8_t chunk_id =
        bit_scan_reverse(node_id + NODE_HEADER_CHUNK_UNIT_SIZE);
    const uint32_t chunk_size = 1U << chunk_id;
    const uint64_t chunk_size_in_bytes = NODE_HEADER_SIZE << chunk_id;
    uint16_t file_id = header_->total_size / header_->max_file_size;
    uint64_t offset = header_->total_size % header_->max_file_size;
    const uint64_t size_left = header_->max_file_size - offset;
    if (chunk_size_in_bytes > size_left) {
      if (header_->num_node_body_chunks == MAX_NUM_NODE_BODY_CHUNKS) {
        GRNXX_ERROR() << "too many chunks: "
                      << "next_chunk_id = " << header_->num_node_body_chunks
                      << ", max_num_chunks = " << MAX_NUM_NODE_BODY_CHUNKS;
        return nullptr;
      }
      remainder_file_id = file_id;
      remainder_offset = offset;
      remainder_size = size_left;
      header_->total_size += remainder_size;
      ++file_id;
      offset = 0;
      if (file_id == header_->max_num_files) {
        GRNXX_ERROR() << "too many files: next_file_id = " << file_id
                      << ", max_num_files = " << header_->max_num_files;
        return nullptr;
      }
    }
    node_header_chunk_indexes_[chunk_id].file_id = file_id;
    node_header_chunk_indexes_[chunk_id].offset = offset;
    node_header_chunk_indexes_[chunk_id].size = chunk_size_in_bytes;
    header_->total_size += chunk_size_in_bytes;
    header_->max_num_nodes += chunk_size;
  }
  NodeHeader * const node_header = get_node_header(node_id);
  if (!node_header) {
    return nullptr;
  }
  *node_header = NodeHeader();
  node_header->id = node_id;
  node_header->next_phantom_node_id = header_->latest_phantom_node_id;
  node_header->modified_time = clock_.now();
  ++header_->num_nodes;
  header_->latest_phantom_node_id = node_id;
  if (remainder_size != 0) {
    const uint16_t chunk_id = header_->num_node_body_chunks;
    node_body_chunk_indexes_[chunk_id].file_id = remainder_file_id;
    node_body_chunk_indexes_[chunk_id].offset = remainder_offset;
    node_body_chunk_indexes_[chunk_id].size = remainder_size;
    ++header_->num_node_body_chunks;
    NodeHeader * const remainder_node_header = create_phantom_node();
    if (!remainder_node_header) {
      // This error must not occur.
      GRNXX_ERROR() << "create_phantom_node() unexpectedly failed";
      return nullptr;
    }
    // TODO: Register the new node as an idle node.
    remainder_node_header->chunk_id = chunk_id;
    remainder_node_header->offset = 0;
    remainder_node_header->size = remainder_size;
  }
  return get_node_header(node_id);
}

NodeHeader *StorageImpl::get_node_header(uint32_t node_id) {
  // Note that the next node is acceptable.
  if (node_id > header_->num_nodes) {
    GRNXX_ERROR() << "invalid argument: node_id = " << node_id
                  << ", num_nodes = " << header_->num_nodes;
    return nullptr;
  }
  const uint8_t chunk_id =
      bit_scan_reverse(node_id + NODE_HEADER_CHUNK_UNIT_SIZE);
  Chunk * const chunk = get_node_header_chunk(chunk_id);
  if (!chunk) {
    return nullptr;
  }
  const uint32_t chunk_size = 1U << chunk_id;
  NodeHeader * const headers = static_cast<NodeHeader *>(chunk->address());
  return headers + (node_id & (chunk_size - 1));
}

void *StorageImpl::get_node_body(const NodeHeader *node_header) {
  Chunk * const chunk = get_node_body_chunk(node_header->chunk_id);
  if (!chunk) {
    return nullptr;
  }
  return static_cast<uint8_t *>(chunk->address()) + node_header->offset;
}

Chunk *StorageImpl::get_node_header_chunk(uint16_t chunk_id) {
  if (!node_header_chunks_[chunk_id]) {
    const ChunkIndex &chunk_index = node_header_chunk_indexes_[chunk_id];
    if (flags_ & STORAGE_ANONYMOUS) {
      node_header_chunks_[chunk_id].reset(
          create_chunk(nullptr, chunk_index.offset, chunk_index.size));
    } else {
      File * const file = get_file(chunk_index.file_id);
      if (!file) {
        return nullptr;
      }
      const int64_t required_size = chunk_index.offset + chunk_index.size;
      if (file->size() < required_size) {
        if (!file->resize(required_size)) {
          return nullptr;
        }
      }
      node_header_chunks_[chunk_id].reset(
          create_chunk(file, chunk_index.offset, chunk_index.size));
    }
  }
  return node_header_chunks_[chunk_id].get();
}

Chunk *StorageImpl::get_node_body_chunk(uint16_t chunk_id) {
  if (!node_body_chunks_[chunk_id]) {
    const ChunkIndex &chunk_index = node_body_chunk_indexes_[chunk_id];
    if (flags_ & STORAGE_ANONYMOUS) {
      node_body_chunks_[chunk_id].reset(
          create_chunk(nullptr, chunk_index.offset, chunk_index.size));
    } else {
      File * const file = get_file(chunk_index.file_id);
      if (!file) {
        return nullptr;
      }
      const int64_t required_size = chunk_index.offset + chunk_index.size;
      if (file->size() < required_size) {
        if (!file->resize(required_size)) {
          return nullptr;
        }
      }
      node_body_chunks_[chunk_id].reset(
          create_chunk(file, chunk_index.offset, chunk_index.size));
    }
  }
  return node_body_chunks_[chunk_id].get();
}

File *StorageImpl::get_file(uint16_t file_id) {
  if (!files_[file_id]) {
    FileFlags file_flags = FILE_DEFAULT;
    if (flags_ & STORAGE_READ_ONLY) {
      file_flags |= FILE_READ_ONLY;
      std::unique_ptr<char[]> path(generate_path(file_id));
      files_[file_id].reset(File::open(path.get(), file_flags));
    } else {
      if (flags_ & STORAGE_TEMPORARY) {
        file_flags |= FILE_TEMPORARY;
        files_[file_id].reset(File::create(path_.get(), file_flags));
      } else {
        std::unique_ptr<char[]> path(generate_path(file_id));
        files_[file_id].reset(File::open_or_create(path.get(), file_flags));
      }
    }
  }
  return files_[file_id].get();
}

char *StorageImpl::generate_path(uint16_t file_id) {
  // If path_ ends with ".grn", the result also ends with ".grn".
  // In this case, file_id is inserted before the ".grn".
  // Otherwise, file_id is appended as a suffix.
  const Slice prefix = path_.get();
  const bool has_extension = prefix.ends_with(".grn");
  const size_t path_size = prefix.size() + 5;
  char * const path = new (std::nothrow) char[path_size];
  if (!path) {
    GRNXX_ERROR() << "new char[] failed: size = " << path_size;
    return nullptr;
  }
  if (has_extension) {
    std::memcpy(path, prefix.ptr(), prefix.size() - 4);
    std::sprintf(path + prefix.size() - 4, "_%03d", file_id);
    std::strcpy(path + prefix.size(), ".grn");
  } else {
    std::memcpy(path, prefix.ptr(), prefix.size());
    std::sprintf(path + prefix.size(), "_%03d", file_id);
  }
  return path;
}

Chunk *StorageImpl::create_chunk(File *file, int64_t offset, int64_t size) {
  ChunkFlags chunk_flags = CHUNK_DEFAULT;
  if (flags_ & STORAGE_HUGE_TLB) {
    chunk_flags |= CHUNK_HUGE_TLB;
  }
  return Chunk::create(file, offset, size, chunk_flags);
}

}  // namespace storage
}  // namespace grnxx
