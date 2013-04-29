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
#include "grnxx/lock.hpp"
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

constexpr uint64_t CHUNK_UNIT_SIZE   = 1 << 16;
constexpr uint64_t NODE_UNIT_SIZE    = 1 << 12;

constexpr uint64_t HEADER_CHUNK_SIZE = CHUNK_UNIT_SIZE;
constexpr uint64_t HEADER_INDEX_SIZE = HEADER_CHUNK_SIZE - HEADER_SIZE;

constexpr uint16_t NODE_HEADER_CHUNK_UNIT_SIZE =
    CHUNK_UNIT_SIZE / NODE_HEADER_SIZE;

constexpr uint32_t MAX_NODE_ID =
    STORAGE_INVALID_NODE_ID - NODE_HEADER_CHUNK_UNIT_SIZE;

constexpr uint16_t MAX_NUM_NODE_HEADER_CHUNKS = 32;
constexpr uint16_t MAX_NUM_NODE_BODY_CHUNKS   =
    (HEADER_INDEX_SIZE / CHUNK_INDEX_SIZE) - MAX_NUM_NODE_HEADER_CHUNKS;

static_assert(MAX_NUM_NODE_BODY_CHUNKS >= 2000,
              "MAX_NUM_NODE_BODY_CHUNKS < 2000");

// A chunk for a remaining space can be smaller than this size.
constexpr uint64_t NODE_BODY_MIN_CHUNK_SIZE = 1 << 21;
constexpr double NODE_BODY_CHUNK_SIZE_RATIO = 1.0 / 64;

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
      mutex_(MUTEX_UNLOCKED),
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
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = nullptr";
    return false;
  }
  std::unique_ptr<Storage> storage(open(path, STORAGE_READ_ONLY));
  if (!storage) {
    return false;
  }
  return true;
}

bool StorageImpl::unlink(const char *path) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = nullptr";
    return false;
  }
  std::unique_ptr<StorageImpl> storage(open(path, STORAGE_READ_ONLY));
  if (!storage) {
    return false;
  }
  const uint16_t max_file_id = static_cast<uint16_t>(
      storage->header_->total_size / storage->header_->max_file_size);
  bool result = File::unlink(path);
  for (uint16_t i = 1; i <= max_file_id; ++i) {
    std::unique_ptr<char[]> numbered_path(storage->generate_path(i));
    result &= File::unlink(numbered_path.get());
  }
  return result;
}

StorageNode StorageImpl::create_node(uint32_t parent_node_id, uint64_t size) {
  if (parent_node_id >= header_->num_nodes) {
    GRNXX_ERROR() << "invalid argument: parent_node_id = " << parent_node_id
                  << ", num_nodes = " << header_->num_nodes;
    return StorageNode(nullptr);
  } else if (size > header_->max_file_size) {
    GRNXX_ERROR() << "invalid argument: size = " << size
                  << ", max_file_size = " << header_->max_file_size;
    return StorageNode(nullptr);
  }
  NodeHeader * const parent_node_header = get_node_header(parent_node_id);
  if (!parent_node_header) {
    return StorageNode(nullptr);
  }
  Lock lock(&header_->data_mutex);
  NodeHeader * const node_header = create_active_node(size);
  if (!node_header) {
    return StorageNode(nullptr);
  }
  node_header->sibling_node_id = parent_node_header->child_node_id;
  parent_node_header->child_node_id = node_header->id;
  void * const body = get_node_body(node_header);
  if (!body) {
    return StorageNode(nullptr);
  }
  return StorageNode(node_header, body);
}

StorageNode StorageImpl::open_node(uint32_t node_id) {
  NodeHeader * const node_header = get_node_header(node_id);
  if (!node_header) {
    return StorageNode(nullptr);
  }
  void * const body = get_node_body(node_header);
  if (!body) {
    return StorageNode(nullptr);
  }
  return StorageNode(node_header, body);
}

bool StorageImpl::unlink_node(uint32_t node_id) {
  if ((node_id == STORAGE_ROOT_NODE_ID) || (node_id >= header_->num_nodes)) {
    GRNXX_ERROR() << "invalid argument: node_id = " << node_id
                  << ", num_nodes = " << header_->num_nodes;
    return false;
  }
  NodeHeader * const node_header = get_node_header(node_id);
  if (!node_header) {
    return false;
  }
  if (node_header->status != STORAGE_NODE_ACTIVE) {
    GRNXX_WARNING() << "invalid argument: status = " << node_header->status;
    return false;
  }
  node_header->status = STORAGE_NODE_MARKED;
  node_header->modified_time = clock_.now();
  return true;
}

bool StorageImpl::sweep(Duration lifetime, uint32_t root_node_id) {
  Lock lock(&header_->data_mutex);
  NodeHeader * const node_header = get_node_header(root_node_id);
  if (!node_header) {
    return false;
  }
  return sweep_subtree(clock_.now() - lifetime, node_header);
}

const char *StorageImpl::path() const {
  return path_.get();
}

StorageFlags StorageImpl::flags() const {
  return flags_;
}

uint64_t StorageImpl::max_file_size() const {
  return header_->max_file_size;
}

uint16_t StorageImpl::max_num_files() const {
  return header_->max_num_files;
}

uint64_t StorageImpl::total_size() const {
  return header_->total_size;
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
  if (!header_chunk) {
    return false;
  }
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
  if (!header_chunk) {
    return false;
  }
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
  std::unique_ptr<File> header_file(File::open(path));
  if (header_file) {
    // Open an existing storage.
    std::unique_ptr<Chunk> header_chunk(
        create_chunk(header_file.get(), 0, HEADER_CHUNK_SIZE));
    if (!header_chunk) {
      return false;
    }
    header_ = static_cast<Header *>(header_chunk->address());
    if (!header_->is_valid()) {
      return false;
    }
    if (!prepare_pointers()) {
      return false;
    }
    files_[0] = std::move(header_file);
    header_chunk_ = std::move(header_chunk);
  } else {
    // Create a storage.
    header_file.reset(File::create(path));
    if (!header_file) {
      return false;
    }
    if (!header_file->resize(HEADER_CHUNK_SIZE)) {
      return false;
    }
    std::unique_ptr<Chunk> header_chunk(
        create_chunk(header_file.get(), 0, HEADER_CHUNK_SIZE));
    if (!header_chunk) {
      return false;
    }
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
  }
  return true;
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
  NodeHeader * const node_header = reserve_phantom_node();
  if (!node_header) {
    return nullptr;
  }
  ChunkIndex *remainder_chunk_index = nullptr;
  ChunkIndex * const chunk_index = create_node_body_chunk(size);
  if (!chunk_index) {
    return nullptr;
  }
  if (!associate_node_with_chunk(node_header, chunk_index)) {
    return nullptr;
  }
  if (remainder_chunk_index) {
    // Create an idle node for the remaining space.
    NodeHeader * const remainder_node_header = create_phantom_node();
    if (!remainder_node_header) {
      // This error must not occur.
      GRNXX_ERROR() << "create_phantom_node() unexpectedly failed";
      return nullptr;
    }
    // The following may fail but the requested node is ready.
    associate_node_with_chunk(remainder_node_header, remainder_chunk_index);
  }
  return node_header;
}

bool StorageImpl::divide_idle_node(NodeHeader *node_header, uint64_t size) {
  NodeHeader * const second_node_header = reserve_phantom_node();
  if (!second_node_header) {
    return false;
  }
  if (!unregister_idle_node(node_header)) {
    return false;
  }
  header_->latest_phantom_node_id = second_node_header->next_phantom_node_id;
  second_node_header->status = STORAGE_NODE_IDLE;
  second_node_header->chunk_id = node_header->chunk_id;
  second_node_header->offset = node_header->offset + size;
  second_node_header->size = node_header->size - size;
  second_node_header->prev_node_id = node_header->id;
  second_node_header->modified_time = clock_.now();
  node_header->size = size;
  node_header->next_node_id = second_node_header->id;
  second_node_header->modified_time = clock_.now();
  if (!register_idle_node(node_header) ||
      !register_idle_node(second_node_header)) {
    return false;
  }
  return true;
}

bool StorageImpl::activate_idle_node(NodeHeader *node_header) {
  if (!unregister_idle_node(node_header)) {
    return false;
  }
  node_header->status = STORAGE_NODE_ACTIVE;
  node_header->child_node_id = STORAGE_INVALID_NODE_ID;
  node_header->sibling_node_id = STORAGE_INVALID_NODE_ID;
  node_header->modified_time = clock_.now();
  return true;
}

NodeHeader *StorageImpl::reserve_phantom_node() {
  if (header_->latest_phantom_node_id != STORAGE_INVALID_NODE_ID) {
    return get_node_header(header_->latest_phantom_node_id);
  } else {
    return create_phantom_node();
  }
}

NodeHeader *StorageImpl::create_phantom_node() {
  const uint32_t node_id = header_->num_nodes;
  ChunkIndex *remainder_chunk_index = nullptr;
  if (node_id == header_->max_num_nodes) {
    if (!create_node_header_chunk(&remainder_chunk_index)) {
      return nullptr;
    }
  }
  // Create a phantom node.
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
  if (remainder_chunk_index) {
    // Create an idle node for the remaining space.
    NodeHeader * const remainder_node_header = create_phantom_node();
    if (!remainder_node_header) {
      // This error must not occur.
      GRNXX_ERROR() << "create_phantom_node() unexpectedly failed";
      return nullptr;
    }
    // The following may fail but the requested node is ready.
    associate_node_with_chunk(remainder_node_header, remainder_chunk_index);
  }
  return node_header;
}

bool StorageImpl::associate_node_with_chunk(NodeHeader *node_header,
                                            ChunkIndex *chunk_index) {
  if ((node_header->id != header_->latest_phantom_node_id) ||
      (node_header->status != STORAGE_NODE_PHANTOM)) {
    // This error must not occur.
    GRNXX_ERROR() << "invalid argument: id = " << node_header->id
                  << ", status = " << node_header->status
                  << ", num_nodes = " << header_->num_nodes
                  << ", latest_phantom_node_id = "
                  << header_->latest_phantom_node_id;
    return false;
  }
  header_->latest_phantom_node_id = node_header->next_phantom_node_id;
  node_header->status = STORAGE_NODE_IDLE;
  node_header->chunk_id = chunk_index->id;
  node_header->offset = 0;
  node_header->size = chunk_index->size;
  node_header->modified_time = clock_.now();
  if (!register_idle_node(node_header)) {
    // This error may rarely occur.
    return false;
  }
  ++header_->num_nodes;
  return true;
}

bool StorageImpl::sweep_subtree(Time threshold, NodeHeader *node_header) {
  if (node_header->status == STORAGE_NODE_MARKED) {
    if (node_header->modified_time <= threshold) {
      // TODO: Unlink the node.
      return true;
    }
  }
  uint32_t node_id = node_header->child_node_id;
  while (node_id != STORAGE_INVALID_NODE_ID) {
    node_header = get_node_header(node_id);
    if (!node_header) {
      return false;
    }
    if (!sweep_subtree(threshold, node_header)) {
      return false;
    }
    node_id = node_header->sibling_node_id;
  }
  return true;
}

ChunkIndex *StorageImpl::create_node_header_chunk(
    ChunkIndex **remainder_chunk_index) {
  if (header_->num_nodes > MAX_NODE_ID) {
    GRNXX_ERROR() << "too many nodes: "
                  << "num_nodes = " << header_->num_nodes
                  << ", max_node_id = " << MAX_NODE_ID;
    return nullptr;
  }
  const uint16_t chunk_id =
      bit_scan_reverse(header_->num_nodes + NODE_HEADER_CHUNK_UNIT_SIZE);
  const uint64_t size = static_cast<uint64_t>(NODE_HEADER_SIZE) << chunk_id;
  if (size > header_->max_file_size) {
    GRNXX_ERROR() << "too large chunk: size = " << size
                  << ", max_file_size = " << header_->max_file_size;
    return nullptr;
  }
  uint16_t file_id = header_->total_size / header_->max_file_size;
  uint64_t offset = header_->total_size % header_->max_file_size;
  uint64_t size_left = header_->max_file_size - offset;
  if (size_left < size) {
    *remainder_chunk_index = create_node_body_chunk(size_left);
    file_id = header_->total_size / header_->max_file_size;
    offset = header_->total_size % header_->max_file_size;
    size_left = header_->max_file_size - offset;
    if (size_left < size) {
      // This error must not occur.
      GRNXX_ERROR() << "too large chunk: size = " << size
                    << ", size_left = " << size_left;
      return nullptr;
    }
  }
  if (file_id == header_->max_num_files) {
    GRNXX_ERROR() << "too many files: file_id = " << file_id
                  << ", max_num_files = " << header_->max_num_files;
    return nullptr;
  }
  ChunkIndex * const chunk_index = &node_header_chunk_indexes_[chunk_id];
  chunk_index->file_id = file_id;
  chunk_index->offset = offset;
  chunk_index->size = size;
  header_->total_size += size;
  header_->max_num_nodes += size / NODE_HEADER_SIZE;
  return chunk_index;
}

ChunkIndex *StorageImpl::create_node_body_chunk(
    uint64_t size, ChunkIndex **remainder_chunk_index) {
  const uint64_t offset = header_->total_size % header_->max_file_size;
  const uint64_t size_left = header_->max_file_size - offset;
  if (size_left < size) {
    *remainder_chunk_index = create_node_body_chunk(size_left);
  }
  return create_node_body_chunk(size);
}

ChunkIndex *StorageImpl::create_node_body_chunk(uint64_t size) {
  const uint16_t chunk_id = header_->num_node_body_chunks;
  if (header_->num_node_body_chunks >= MAX_NUM_NODE_BODY_CHUNKS) {
    GRNXX_ERROR() << "too many chunks: "
                  << "num_chunks = " << header_->num_node_body_chunks
                  << ", max_num_chunks = " << MAX_NUM_NODE_BODY_CHUNKS;
    return nullptr;
  }
  const uint16_t file_id = header_->total_size / header_->max_file_size;
  const uint64_t offset = header_->total_size % header_->max_file_size;
  const uint64_t size_left = header_->max_file_size - offset;
  if (file_id >= header_->max_num_files) {
    GRNXX_ERROR() << "too many files: file_id = " << file_id
                  << ", max_num_files = " << header_->max_num_files;
    return nullptr;
  }
  if (size_left < size) {
    // This error must not occur.
    GRNXX_ERROR() << "too large chunk: size = " << size
                  << ", size_left = " << size_left;
    return nullptr;
  }
  if (size < NODE_BODY_MIN_CHUNK_SIZE) {
    size = NODE_BODY_MIN_CHUNK_SIZE;
  }
  const uint64_t expected_size = static_cast<uint64_t>(
      header_->total_size * NODE_BODY_CHUNK_SIZE_RATIO);
  if (size < expected_size) {
    size = expected_size;
  }
  if (size > size_left) {
    size = size_left;
  }
  ChunkIndex * const chunk_index = &node_body_chunk_indexes_[chunk_id];
  chunk_index->file_id = file_id;
  chunk_index->offset = offset;
  chunk_index->size = size;
  header_->total_size += size;
  ++header_->num_node_body_chunks;
  return chunk_index;
}

bool StorageImpl::register_idle_node(NodeHeader *node_header) {
  if (node_header->status != STORAGE_NODE_IDLE) {
    // This error must not occur.
    GRNXX_ERROR() << "invalid argument: status = " << node_header->status;
    return false;
  }
  const size_t list_id = bit_scan_reverse(node_header->size / NODE_UNIT_SIZE);
  if (header_->oldest_idle_node_ids[list_id] == STORAGE_INVALID_NODE_ID) {
    // The given node is appended to the empty list.
    node_header->next_idle_node_id = node_header->id;
    node_header->prev_idle_node_id = node_header->id;
    header_->oldest_idle_node_ids[list_id] = node_header->id;
  } else {
    // The given node is inserted as the new lastest idle node.
    NodeHeader * const oldest_idle_node_header =
        get_node_header(header_->oldest_idle_node_ids[list_id]);
    if (!oldest_idle_node_header) {
      return false;
    }
    NodeHeader * const latest_idle_node_header =
        get_node_header(oldest_idle_node_header->prev_idle_node_id);
    if (!latest_idle_node_header) {
      return false;
    }
    node_header->next_idle_node_id = oldest_idle_node_header->id;
    node_header->prev_idle_node_id = latest_idle_node_header->id;
    latest_idle_node_header->next_idle_node_id = node_header->id;
    oldest_idle_node_header->prev_idle_node_id = node_header->id;
  }
  return true;
}

bool StorageImpl::unregister_idle_node(NodeHeader *node_header) {
  if (node_header->status != STORAGE_NODE_IDLE) {
    // This error must not occur.
    GRNXX_ERROR() << "invalid argument: status = " << node_header->status;
    return false;
  }
  const size_t list_id = bit_scan_reverse(node_header->size / NODE_UNIT_SIZE);
  if (node_header->id == node_header->next_idle_node_id) {
    // The list becomes empty.
    header_->oldest_idle_node_ids[list_id] = STORAGE_INVALID_NODE_ID;
  } else {
    // The specified node is removed from the list.
    NodeHeader * const next_idle_node_header =
        get_node_header(node_header->next_idle_node_id);
    if (!next_idle_node_header) {
      return false;
    }
    NodeHeader * const prev_idle_node_header =
        get_node_header(node_header->prev_idle_node_id);
    if (!prev_idle_node_header) {
      return false;
    }
    next_idle_node_header->prev_idle_node_id = prev_idle_node_header->id;
    prev_idle_node_header->next_idle_node_id = next_idle_node_header->id;
    if (node_header->id == header_->oldest_idle_node_ids[list_id]) {
      header_->oldest_idle_node_ids[list_id] = next_idle_node_header->id;
    }
  }
  return true;
}

NodeHeader *StorageImpl::get_node_header(uint32_t node_id) {
  if (node_id >= header_->max_num_nodes) {
    GRNXX_ERROR() << "invalid argument: node_id = " << node_id
                  << ", max_num_nodes = " << header_->max_num_nodes;
    return nullptr;
  }
  const uint16_t chunk_id =
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
      Lock lock(&mutex_);
      if (!node_header_chunks_[chunk_id]) {
        node_header_chunks_[chunk_id].reset(
            create_chunk(nullptr, chunk_index.offset, chunk_index.size));
      }
    } else {
      File * const file = get_file(chunk_index.file_id);
      if (!file) {
        return nullptr;
      }
      const int64_t required_size = chunk_index.offset + chunk_index.size;
      if (file->size() < required_size) {
        Lock file_lock(&header_->file_mutex);
        if (file->size() < required_size) {
          if (!file->resize(required_size)) {
            return nullptr;
          }
        }
      }
      Lock lock(&mutex_);
      if (!node_header_chunks_[chunk_id]) {
        node_header_chunks_[chunk_id].reset(
            create_chunk(file, chunk_index.offset, chunk_index.size));
      }
    }
  }
  return node_header_chunks_[chunk_id].get();
}

Chunk *StorageImpl::get_node_body_chunk(uint16_t chunk_id) {
  if (!node_body_chunks_[chunk_id]) {
    const ChunkIndex &chunk_index = node_body_chunk_indexes_[chunk_id];
    if (flags_ & STORAGE_ANONYMOUS) {
      Lock lock(&mutex_);
      if (!node_body_chunks_[chunk_id]) {
        node_body_chunks_[chunk_id].reset(
            create_chunk(nullptr, chunk_index.offset, chunk_index.size));
      }
    } else {
      File * const file = get_file(chunk_index.file_id);
      if (!file) {
        return nullptr;
      }
      const int64_t required_size = chunk_index.offset + chunk_index.size;
      if (file->size() < required_size) {
        Lock file_lock(&header_->file_mutex);
        if (file->size() < required_size) {
          if (!file->resize(required_size)) {
            return nullptr;
          }
        }
      }
      Lock lock(&mutex_);
      if (!node_body_chunks_[chunk_id]) {
        node_body_chunks_[chunk_id].reset(
            create_chunk(file, chunk_index.offset, chunk_index.size));
      }
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
      Lock file_lock(&header_->file_mutex);
      if (!files_[file_id]) {
        if (flags_ & STORAGE_TEMPORARY) {
          file_flags |= FILE_TEMPORARY;
          files_[file_id].reset(File::create(path_.get(), file_flags));
        } else {
          std::unique_ptr<char[]> path(generate_path(file_id));
          files_[file_id].reset(File::open_or_create(path.get(), file_flags));
        }
      }
    }
  }
  return files_[file_id].get();
}

char *StorageImpl::generate_path(uint16_t file_id) {
  // If "path_" ends with ".grn", the result also ends with ".grn".
  // In this case, "file_id" is inserted before the ".grn".
  // Otherwise, "file_id" is appended as a suffix.
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
