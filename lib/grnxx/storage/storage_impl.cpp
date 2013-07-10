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

#include <cstdio>
#include <cstring>
#include <new>
#include <utility>

#include "grnxx/bytes.hpp"
#include "grnxx/exception.hpp"
#include "grnxx/intrinsic.hpp"
#include "grnxx/lock.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/storage/chunk.hpp"
#include "grnxx/storage/chunk_index.hpp"
#include "grnxx/storage/file.hpp"
#include "grnxx/storage/header.hpp"
#include "grnxx/storage/node_header.hpp"
#include "grnxx/storage/path.hpp"

namespace grnxx {
namespace storage {
namespace {

// The size of chunk must be a multiple of CHUNK_UNIT_SIZE (64KB).
constexpr uint64_t CHUNK_UNIT_SIZE = 1 << 16;
// The size of regular node must be a multiple of REGULAR_NODE_UNIT_SIZE (4KB).
constexpr uint64_t REGULAR_NODE_UNIT_SIZE = 1 << 12;
// The size of small node must be a multiple of SMALL_NODE_UNIT_SIZE (64 bytes).
constexpr uint64_t SMALL_NODE_UNIT_SIZE   = 1 << 6;
// A node larger than NODE_SIZE_THRESHOLD (2KB) is a regular node.
constexpr uint64_t NODE_SIZE_THRESHOLD    = 1 << 11;

// The chunk size for Header and ChunkIndexes.
constexpr uint64_t ROOT_CHUNK_SIZE = CHUNK_UNIT_SIZE;
// The size allocated to ChunkIndexes.
constexpr uint64_t ROOT_INDEX_SIZE = ROOT_CHUNK_SIZE - HEADER_SIZE;

// The number of NodeHeaders in the minimum header chunk.
constexpr uint32_t HEADER_CHUNK_MIN_SIZE = CHUNK_UNIT_SIZE / NODE_HEADER_SIZE;

// The maximum node ID.
constexpr uint32_t MAX_NODE_ID =
    STORAGE_INVALID_NODE_ID - HEADER_CHUNK_MIN_SIZE;

// The maximum number of chunks for NodeHeaders.
constexpr uint16_t MAX_NUM_HEADER_CHUNKS = 32;
// The maximum number of chunks for node bodies.
constexpr uint16_t MAX_NUM_BODY_CHUNKS   =
    (ROOT_INDEX_SIZE / CHUNK_INDEX_SIZE) - MAX_NUM_HEADER_CHUNKS;

static_assert(MAX_NUM_BODY_CHUNKS >= 2000, "MAX_NUM_BODY_CHUNKS < 2000");

// The minimum size of regular body chunks.
constexpr uint64_t REGULAR_BODY_CHUNK_MIN_SIZE = 1 << 21;
// The ratio of the next regular body chunk size to the storage total size.
constexpr double REGULAR_BODY_CHUNK_SIZE_RATIO = 1.0 / 64;
// The size of the minimum small body chunk.
constexpr uint64_t SMALL_BODY_CHUNK_MIN_SIZE = CHUNK_UNIT_SIZE;

}  // namespace

StorageImpl::StorageImpl()
    : Storage(),
      path_(),
      flags_(STORAGE_DEFAULT),
      header_(nullptr),
      header_chunk_indexes_(nullptr),
      body_chunk_indexes_(nullptr),
      files_(),
      root_chunk_(),
      header_chunks_(),
      body_chunks_(),
      mutex_(),
      clock_() {}

StorageImpl::~StorageImpl() {}

StorageImpl *StorageImpl::create(const char *path, StorageFlags flags,
                                 const StorageOptions &options) {
  if (!options) {
    GRNXX_ERROR() << "invalid argument: options = " << options;
    throw LogicError();
  }
  std::unique_ptr<StorageImpl> storage(new (std::nothrow) StorageImpl);
  if (!storage) {
    GRNXX_ERROR() << "new grnxx::storage::StorageImpl failed";
    throw MemoryError();
  }
  if (path || (flags & STORAGE_TEMPORARY)) {
    storage->create_file_backed_storage(path, flags, options);
  } else {
    storage->create_anonymous_storage(flags, options);
  }
  return storage.release();
}

StorageImpl *StorageImpl::open(const char *path, StorageFlags flags) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = nullptr";
    throw LogicError();
  }
  std::unique_ptr<StorageImpl> storage(new (std::nothrow) StorageImpl);
  if (!storage) {
    GRNXX_ERROR() << "new grnxx::storage::StorageImpl failed";
    throw MemoryError();
  }
  storage->open_storage(path, flags);
  return storage.release();
}

StorageImpl *StorageImpl::open_or_create(const char *path, StorageFlags flags,
                                         const StorageOptions &options) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = nullptr";
    throw LogicError();
  }
  if (!options) {
    GRNXX_ERROR() << "invalid argument: options = " << options;
    throw LogicError();
  }
  std::unique_ptr<StorageImpl> storage(new (std::nothrow) StorageImpl);
  if (!storage) {
    GRNXX_ERROR() << "new grnxx::storage::StorageImpl failed";
    throw MemoryError();
  }
  storage->open_or_create_storage(path, flags, options);
  return storage.release();
}

bool StorageImpl::exists(const char *path) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = nullptr";
    throw LogicError();
  }
  if (!File::exists(path)) {
    return false;
  }
  std::unique_ptr<Storage> storage(open(path, STORAGE_READ_ONLY));
  return true;
}

void StorageImpl::unlink(const char *path) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = nullptr";
    throw LogicError();
  }
  std::unique_ptr<StorageImpl> storage(open(path, STORAGE_READ_ONLY));
  storage->unlink_storage();
}

StorageNode StorageImpl::create_node(uint32_t parent_node_id, uint64_t size) {
  if (flags_ & STORAGE_READ_ONLY) {
    GRNXX_ERROR() << "invalid operation: flags = " << flags_;
    throw LogicError();
  }
  Lock data_lock(&header_->data_mutex);
  if (parent_node_id >= header_->num_nodes) {
    GRNXX_ERROR() << "invalid argument: parent_node_id = " << parent_node_id
                  << ", num_nodes = " << header_->num_nodes;
    throw LogicError();
  } else if (size > header_->max_file_size) {
    GRNXX_ERROR() << "invalid argument: size = " << size
                  << ", max_file_size = " << header_->max_file_size;
    throw LogicError();
  }
  NodeHeader * const parent_node_header = get_node_header(parent_node_id);
  if ((parent_node_header->status != STORAGE_NODE_ACTIVE) &&
      (parent_node_header->status != STORAGE_NODE_UNLINKED)) {
    GRNXX_ERROR() << "invalid argument: status = "
                  << parent_node_header->status;
    throw LogicError();
  }
  NodeHeader *child_node_header = nullptr;
  if (parent_node_header->child_node_id != STORAGE_INVALID_NODE_ID) {
    child_node_header = get_node_header(parent_node_header->child_node_id);
  }
  NodeHeader * const node_header = create_active_node(size);
  node_header->sibling_node_id = parent_node_header->child_node_id;
  node_header->from_node_id = parent_node_id;
  parent_node_header->child_node_id = node_header->id;
  if (child_node_header) {
    child_node_header->from_node_id = node_header->id;
  }
  void * const body = get_node_body(node_header);
  return StorageNode(node_header, body);
}

StorageNode StorageImpl::open_node(uint32_t node_id) {
  NodeHeader * const node_header = get_node_header(node_id);
  if ((node_header->status != STORAGE_NODE_ACTIVE) &&
      (node_header->status != STORAGE_NODE_UNLINKED)) {
    GRNXX_ERROR() << "invalid argument: status = " << node_header->status;
    throw LogicError();
  }
  void * const body = get_node_body(node_header);
  return StorageNode(node_header, body);
}

bool StorageImpl::unlink_node(uint32_t node_id) {
  if (flags_ & STORAGE_READ_ONLY) {
    GRNXX_ERROR() << "invalid operation: flags = " << flags_;
    throw LogicError();
  }
  Lock data_lock(&header_->data_mutex);
  if ((node_id == STORAGE_ROOT_NODE_ID) || (node_id >= header_->num_nodes)) {
    GRNXX_ERROR() << "invalid argument: node_id = " << node_id
                  << ", num_nodes = " << header_->num_nodes;
    throw LogicError();
  }
  NodeHeader * const node_header = get_node_header(node_id);
  if (node_header->status == STORAGE_NODE_UNLINKED) {
    // Already unlinked
    return false;
  }
  if (node_header->status != STORAGE_NODE_ACTIVE) {
    GRNXX_ERROR() << "invalid argument: status = " << node_header->status;
    throw LogicError();
  }
  NodeHeader * const from_node_header =
      get_node_header(node_header->from_node_id);
  if (node_id == from_node_header->child_node_id) {
    from_node_header->child_node_id = node_header->sibling_node_id;
  } else if (node_id == from_node_header->sibling_node_id) {
    from_node_header->sibling_node_id = node_header->sibling_node_id;
  } else {
    // This error must not occur.
    GRNXX_ERROR() << "broken link: node_id = " << node_id
                  << ", from_node_id = " << from_node_header->id
                  << ", child_node_id = " << from_node_header->child_node_id
                  << ", sibling_node_id = "
                  << from_node_header->sibling_node_id;
    throw LogicError();
  }
  if (node_header->sibling_node_id != STORAGE_INVALID_NODE_ID) {
    NodeHeader * const sibling_node_header =
        get_node_header(node_header->sibling_node_id);
    sibling_node_header->from_node_id = node_header->from_node_id;
  }
  NodeHeader *latest_node_header = nullptr;
  if (header_->latest_unlinked_node_id != STORAGE_INVALID_NODE_ID) {
    latest_node_header = get_node_header(header_->latest_unlinked_node_id);
  }
  node_header->status = STORAGE_NODE_UNLINKED;
  if (latest_node_header) {
    node_header->next_unlinked_node_id =
        latest_node_header->next_unlinked_node_id;
    latest_node_header->next_unlinked_node_id = node_id;
  } else {
    node_header->next_unlinked_node_id = node_id;
  }
  header_->latest_unlinked_node_id = node_id;
  node_header->modified_time = clock_.now();
  return true;
}

void StorageImpl::sweep(Duration lifetime) {
  if (flags_ & STORAGE_READ_ONLY) {
    GRNXX_ERROR() << "invalid operation: flags = " << flags_;
    throw LogicError();
  }
  Lock data_lock(&header_->data_mutex);
  if (header_->latest_unlinked_node_id == STORAGE_INVALID_NODE_ID) {
    // Nothing to do.
    return;
  }
  NodeHeader * const latest_node_header =
      get_node_header(header_->latest_unlinked_node_id);
  const Time threshold = clock_.now() - lifetime;
  do {
    NodeHeader * const oldest_node_header =
        get_node_header(latest_node_header->next_unlinked_node_id);
    if (oldest_node_header->status != STORAGE_NODE_UNLINKED) {
      // This error must not occur.
      GRNXX_ERROR() << "invalid argument: status = "
                    << oldest_node_header->status;
      throw LogicError();
    }
    if (oldest_node_header->modified_time > threshold) {
      // Remaining unlinked nodes are too early for reuse.
      break;
    }
    const uint32_t next_node_id = oldest_node_header->next_unlinked_node_id;
    sweep_subtree(oldest_node_header);
    if (oldest_node_header != latest_node_header) {
      latest_node_header->next_unlinked_node_id = next_node_id;
    } else {
      header_->latest_unlinked_node_id = STORAGE_INVALID_NODE_ID;
    }
  } while (header_->latest_unlinked_node_id != STORAGE_INVALID_NODE_ID);
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

uint32_t StorageImpl::num_nodes() const {
  return header_->num_active_or_unlinked_nodes;
}

uint16_t StorageImpl::num_chunks() const {
  return header_->num_body_chunks;
}

uint64_t StorageImpl::body_usage() const {
  return header_->body_usage;
}

uint64_t StorageImpl::body_size() const {
  return header_->body_size;
}

uint64_t StorageImpl::total_size() const {
  return header_->total_size;
}

void StorageImpl::create_file_backed_storage(const char *path,
                                             StorageFlags flags,
                                             const StorageOptions &options) {
  if (path) {
    path_.reset(Path::clone_path(path));
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
  try {
    header_file->resize(ROOT_CHUNK_SIZE);
    std::unique_ptr<Chunk> root_chunk(
        create_chunk(header_file.get(), 0, ROOT_CHUNK_SIZE));
    header_ = static_cast<Header *>(root_chunk->address());
    *header_ = Header();
    header_->max_file_size = options.max_file_size & ~(CHUNK_UNIT_SIZE - 1);
    header_->max_num_files = options.max_num_files;
    header_->total_size = ROOT_CHUNK_SIZE;
    prepare_pointers();
    prepare_indexes();
    files_[0] = std::move(header_file);
    root_chunk_ = std::move(root_chunk);
    create_active_node(options.root_size);
    header_->validate();
  } catch (...) {
    try {
      unlink_storage();
    } catch (...) {
      // Ignore an exception.
    }
    throw;
  }
}

void StorageImpl::create_anonymous_storage(StorageFlags flags,
                                           const StorageOptions &options) {
  flags_ |= STORAGE_ANONYMOUS;
  if (flags & STORAGE_HUGE_TLB) {
    flags_ |= STORAGE_HUGE_TLB;
  }
  std::unique_ptr<Chunk> root_chunk(create_chunk(nullptr, 0, ROOT_CHUNK_SIZE));
  header_ = static_cast<Header *>(root_chunk->address());
  *header_ = Header();
  header_->max_file_size = options.max_file_size & ~(CHUNK_UNIT_SIZE - 1);
  header_->max_num_files = options.max_num_files;
  header_->total_size = ROOT_CHUNK_SIZE;
  prepare_pointers();
  prepare_indexes();
  root_chunk_ = std::move(root_chunk);
  create_active_node(options.root_size);
  header_->validate();
}

void StorageImpl::open_storage(const char *path, StorageFlags flags) {
  path_.reset(Path::clone_path(path));
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
  std::unique_ptr<Chunk> root_chunk(
      create_chunk(header_file.get(), 0, ROOT_CHUNK_SIZE));
  header_ = static_cast<Header *>(root_chunk->address());
  if (!*header_) {
    GRNXX_ERROR() << "invalid format: path = " << path_.get();
    throw LogicError();
  }
  prepare_pointers();
  files_[0] = std::move(header_file);
  root_chunk_ = std::move(root_chunk);
}

void StorageImpl::open_or_create_storage(const char *path, StorageFlags flags,
                                         const StorageOptions &options) {
  path_.reset(Path::clone_path(path));
  if (flags & STORAGE_HUGE_TLB) {
    flags_ |= STORAGE_HUGE_TLB;
  }
  std::unique_ptr<File> header_file;
  if (File::exists(path)) {
    header_file.reset(File::open(path));
  }
  if (header_file) {
    // Open an existing storage.
    std::unique_ptr<Chunk> root_chunk(
        create_chunk(header_file.get(), 0, ROOT_CHUNK_SIZE));
    header_ = static_cast<Header *>(root_chunk->address());
    if (!*header_) {
      GRNXX_ERROR() << "invalid format: path = " << path_.get();
      throw LogicError();
    }
    prepare_pointers();
    files_[0] = std::move(header_file);
    root_chunk_ = std::move(root_chunk);
  } else {
    // Create a storage.
    header_file.reset(File::create(path));
    try {
      header_file->resize(ROOT_CHUNK_SIZE);
      std::unique_ptr<Chunk> root_chunk(
          create_chunk(header_file.get(), 0, ROOT_CHUNK_SIZE));
      header_ = static_cast<Header *>(root_chunk->address());
      *header_ = Header();
      header_->max_file_size = options.max_file_size & ~(CHUNK_UNIT_SIZE - 1);
      header_->max_num_files = options.max_num_files;
      header_->total_size = ROOT_CHUNK_SIZE;
      prepare_pointers();
      prepare_indexes();
      files_[0] = std::move(header_file);
      root_chunk_ = std::move(root_chunk);
      create_active_node(options.root_size);
      header_->validate();
    } catch (...) {
      try {
        unlink_storage();
      } catch (...) {
        // Ignore an exception.
      }
      throw;
    }
  }
}

void StorageImpl::unlink_storage() {
  if (flags_ & STORAGE_TEMPORARY) {
    // Nothing to do.
    return;
  }
  uint16_t max_file_id = 0;
  if (header_) {
    max_file_id = header_->total_size / header_->max_file_size;
  }
  File::unlink(path_.get());
  for (uint16_t i = 1; i <= max_file_id; ++i) {
    // Component files may be left if an error occurs.
    std::unique_ptr<char[]> numbered_path(generate_path(i));
    File::unlink(numbered_path.get());
  }
}

void StorageImpl::prepare_pointers() {
  header_chunk_indexes_ = reinterpret_cast<ChunkIndex *>(header_ + 1);
  body_chunk_indexes_ = header_chunk_indexes_ + MAX_NUM_HEADER_CHUNKS;
  if (~flags_ & STORAGE_ANONYMOUS) {
    files_.reset(
        new (std::nothrow) std::unique_ptr<File>[header_->max_num_files]);
    if (!files_) {
      GRNXX_ERROR() << "new std::unique_ptr<grnxx::storage::File>[] failed: "
                    << "size = " << header_->max_num_files;
      throw MemoryError();
    }
  }
  header_chunks_.reset(
      new (std::nothrow) std::unique_ptr<Chunk>[MAX_NUM_HEADER_CHUNKS]);
  if (!header_chunks_) {
    GRNXX_ERROR() << "new std::unique_ptr<grnxx::storage::Chunk>[] failed: "
                  << "size = " << MAX_NUM_HEADER_CHUNKS;
    throw MemoryError();
  }
  body_chunks_.reset(
      new (std::nothrow) std::unique_ptr<Chunk>[MAX_NUM_BODY_CHUNKS]);
  if (!header_chunks_) {
    GRNXX_ERROR() << "new std::unique_ptr<grnxx::storage::Chunk>[] failed: "
                  << "size = " << MAX_NUM_BODY_CHUNKS;
    throw MemoryError();
  }
}

void StorageImpl::prepare_indexes() {
  for (uint16_t i = 0; i < MAX_NUM_HEADER_CHUNKS; ++i) {
    header_chunk_indexes_[i] = ChunkIndex(i, HEADER_CHUNK);
  }
  for (uint16_t i = 0; i < MAX_NUM_BODY_CHUNKS; ++i) {
    body_chunk_indexes_[i] = ChunkIndex(i, REGULAR_BODY_CHUNK);
  }
}

NodeHeader *StorageImpl::create_active_node(uint64_t size) {
  if (size == 0) {
    size = SMALL_NODE_UNIT_SIZE;
  } else if (size <= NODE_SIZE_THRESHOLD) {
    size = (size + SMALL_NODE_UNIT_SIZE - 1) & ~(SMALL_NODE_UNIT_SIZE - 1);
  } else {
    size = (size + REGULAR_NODE_UNIT_SIZE - 1) & ~(REGULAR_NODE_UNIT_SIZE - 1);
  }
  NodeHeader *node_header = find_idle_node(size);
  if (!node_header) {
    node_header = create_idle_node(size);
  }
  if (node_header->size > size) {
    divide_idle_node(node_header, size);
  }
  activate_idle_node(node_header);
  return node_header;
}

NodeHeader *StorageImpl::find_idle_node(uint64_t size) {
  const size_t begin = bit_scan_reverse(size);
  const size_t end = (size <= NODE_SIZE_THRESHOLD) ?
      bit_scan_reverse(NODE_SIZE_THRESHOLD << 1) : NUM_IDLE_NODE_LISTS;
  for (size_t i = begin; i < end; ++i) {
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
  ChunkIndex *remainder_chunk_index = nullptr;
  ChunkIndex * const chunk_index =
      create_body_chunk(size, &remainder_chunk_index);
  associate_node_with_chunk(node_header, chunk_index);
  if (remainder_chunk_index) {
    // Create an idle node for the remaining space.
    NodeHeader * const remainder_node_header = create_phantom_node();
    // The following may fail but the requested node is ready.
    associate_node_with_chunk(remainder_node_header, remainder_chunk_index);
  }
  return node_header;
}

void StorageImpl::divide_idle_node(NodeHeader *node_header, uint64_t size) {
  NodeHeader *next_node_header = nullptr;
  if (node_header->next_node_id != STORAGE_INVALID_NODE_ID) {
    next_node_header = get_node_header(node_header->next_node_id);
  }
  NodeHeader * const second_node_header = reserve_phantom_node();
  unregister_idle_node(node_header);
  header_->latest_phantom_node_id = second_node_header->next_phantom_node_id;
  second_node_header->status = STORAGE_NODE_IDLE;
  second_node_header->chunk_id = node_header->chunk_id;
  second_node_header->offset = node_header->offset + size;
  second_node_header->size = node_header->size - size;
  second_node_header->next_node_id = node_header->next_node_id;
  second_node_header->prev_node_id = node_header->id;
  second_node_header->modified_time = clock_.now();
  if (next_node_header) {
    next_node_header->prev_node_id = second_node_header->id;
  }
  node_header->size = size;
  node_header->next_node_id = second_node_header->id;
  second_node_header->modified_time = clock_.now();
  register_idle_node(node_header);
  register_idle_node(second_node_header);
}

void StorageImpl::activate_idle_node(NodeHeader *node_header) {
  unregister_idle_node(node_header);
  node_header->status = STORAGE_NODE_ACTIVE;
  node_header->child_node_id = STORAGE_INVALID_NODE_ID;
  node_header->sibling_node_id = STORAGE_INVALID_NODE_ID;
  node_header->modified_time = clock_.now();
  ++header_->num_active_or_unlinked_nodes;
  header_->body_usage += node_header->size;
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
    create_header_chunk(&remainder_chunk_index);
  }
  // Create a phantom node.
  NodeHeader * const node_header = get_node_header(node_id);
  *node_header = NodeHeader(node_id);
  node_header->next_phantom_node_id = header_->latest_phantom_node_id;
  node_header->modified_time = clock_.now();
  ++header_->num_nodes;
  header_->latest_phantom_node_id = node_id;
  if (remainder_chunk_index) {
    // Create an idle node for the remaining space.
    NodeHeader * const remainder_node_header = create_phantom_node();
    // The following may fail but the requested node is ready.
    associate_node_with_chunk(remainder_node_header, remainder_chunk_index);
  }
  return node_header;
}

void StorageImpl::associate_node_with_chunk(NodeHeader *node_header,
                                            ChunkIndex *chunk_index) {
  if ((node_header->id != header_->latest_phantom_node_id) ||
      (node_header->status != STORAGE_NODE_PHANTOM)) {
    // This error must not occur.
    GRNXX_ERROR() << "invalid argument: id = " << node_header->id
                  << ", status = " << node_header->status
                  << ", num_nodes = " << header_->num_nodes
                  << ", latest_phantom_node_id = "
                  << header_->latest_phantom_node_id;
    throw LogicError();
  }
  header_->latest_phantom_node_id = node_header->next_phantom_node_id;
  node_header->status = STORAGE_NODE_IDLE;
  node_header->chunk_id = chunk_index->id;
  node_header->offset = 0;
  node_header->size = chunk_index->size;
  node_header->modified_time = clock_.now();
  register_idle_node(node_header);
}

void StorageImpl::sweep_subtree(NodeHeader *node_header) {
  uint32_t child_node_id = node_header->child_node_id;
  while (child_node_id != STORAGE_INVALID_NODE_ID) {
    NodeHeader * const child_node_header = get_node_header(child_node_id);
    child_node_id = child_node_header->sibling_node_id;
    sweep_subtree(child_node_header);
    node_header->child_node_id = child_node_id;
  }
  node_header->status = STORAGE_NODE_IDLE;
  node_header->modified_time = clock_.now();
  --header_->num_active_or_unlinked_nodes;
  header_->body_usage -= node_header->size;
  register_idle_node(node_header);
  if (node_header->next_node_id != STORAGE_INVALID_NODE_ID) {
    NodeHeader * const next_node_header =
        get_node_header(node_header->next_node_id);
    if (next_node_header->status == STORAGE_NODE_IDLE) {
      merge_idle_nodes(node_header, next_node_header);
    }
  }
  if (node_header->prev_node_id != STORAGE_INVALID_NODE_ID) {
    NodeHeader * const prev_node_header =
        get_node_header(node_header->prev_node_id);
    if (prev_node_header->status == STORAGE_NODE_IDLE) {
      merge_idle_nodes(prev_node_header, node_header);
    }
  }
}

void StorageImpl::merge_idle_nodes(NodeHeader *node_header,
                                   NodeHeader *next_node_header) {
  NodeHeader *next_next_node_header = nullptr;
  if (next_node_header->next_node_id != STORAGE_INVALID_NODE_ID) {
    next_next_node_header = get_node_header(next_node_header->next_node_id);
  }
  unregister_idle_node(node_header);
  unregister_idle_node(next_node_header);
  node_header->size += next_node_header->size;
  node_header->next_node_id = next_node_header->next_node_id;
  if (next_next_node_header) {
    next_next_node_header->prev_node_id = node_header->id;
  }
  *next_node_header = NodeHeader(next_node_header->id);
  next_node_header->next_phantom_node_id = header_->latest_phantom_node_id;
  next_node_header->modified_time = clock_.now();
  header_->latest_phantom_node_id = next_node_header->id;
  register_idle_node(node_header);
}

ChunkIndex *StorageImpl::create_header_chunk(
    ChunkIndex **remainder_chunk_index) {
  if (header_->num_nodes > MAX_NODE_ID) {
    GRNXX_ERROR() << "too many nodes: "
                  << "num_nodes = " << header_->num_nodes
                  << ", max_node_id = " << MAX_NODE_ID;
    throw LogicError();
  }
  const uint16_t chunk_id =
      bit_scan_reverse(header_->num_nodes + HEADER_CHUNK_MIN_SIZE);
  const uint64_t size = static_cast<uint64_t>(NODE_HEADER_SIZE) << chunk_id;
  if (size > header_->max_file_size) {
    GRNXX_ERROR() << "too large chunk: size = " << size
                  << ", max_file_size = " << header_->max_file_size;
    throw LogicError();
  }
  uint16_t file_id = header_->total_size / header_->max_file_size;
  uint64_t offset = header_->total_size % header_->max_file_size;
  uint64_t size_left = header_->max_file_size - offset;
  if (size_left < size) {
    *remainder_chunk_index = create_body_chunk(size_left);
    file_id = header_->total_size / header_->max_file_size;
    offset = header_->total_size % header_->max_file_size;
    size_left = header_->max_file_size - offset;
    if (size_left < size) {
      // This error must not occur.
      GRNXX_ERROR() << "too large chunk: size = " << size
                    << ", size_left = " << size_left;
      throw LogicError();
    }
  }
  if (file_id == header_->max_num_files) {
    GRNXX_ERROR() << "too many files: file_id = " << file_id
                  << ", max_num_files = " << header_->max_num_files;
    throw LogicError();
  }
  ChunkIndex * const chunk_index = &header_chunk_indexes_[chunk_id];
  chunk_index->file_id = file_id;
  chunk_index->offset = offset;
  chunk_index->size = size;
  header_->total_size += size;
  header_->max_num_nodes += size / NODE_HEADER_SIZE;
  return chunk_index;
}

ChunkIndex *StorageImpl::create_body_chunk(
    uint64_t size, ChunkIndex **remainder_chunk_index) {
  uint64_t chunk_size = size;
  if (size <= NODE_SIZE_THRESHOLD) {
    chunk_size = SMALL_BODY_CHUNK_MIN_SIZE << header_->num_small_body_chunks;
  }
  uint64_t offset = header_->total_size % header_->max_file_size;
  uint64_t size_left = header_->max_file_size - offset;
  if (size_left < chunk_size) {
    *remainder_chunk_index = create_body_chunk(size_left);
    size_left = header_->max_file_size;
  }
  if (size > NODE_SIZE_THRESHOLD) {
    chunk_size = static_cast<uint64_t>(
        header_->total_size * REGULAR_BODY_CHUNK_SIZE_RATIO);
    chunk_size &= ~(CHUNK_UNIT_SIZE - 1);
    if (size > chunk_size) {
      chunk_size = size;
    }
    if (chunk_size > size_left) {
      chunk_size = size_left;
    }
    if (chunk_size < REGULAR_BODY_CHUNK_MIN_SIZE) {
      chunk_size = REGULAR_BODY_CHUNK_MIN_SIZE;
    }
  }
  ChunkIndex * const chunk_index = create_body_chunk(chunk_size);
  if (size <= NODE_SIZE_THRESHOLD) {
    chunk_index->type = SMALL_BODY_CHUNK;
    ++header_->num_small_body_chunks;
  }
  return chunk_index;
}

ChunkIndex *StorageImpl::create_body_chunk(uint64_t size) {
  const uint16_t chunk_id = header_->num_body_chunks;
  if (header_->num_body_chunks >= MAX_NUM_BODY_CHUNKS) {
    GRNXX_ERROR() << "too many chunks: "
                  << "num_chunks = " << header_->num_body_chunks
                  << ", max_num_chunks = " << MAX_NUM_BODY_CHUNKS;
    throw LogicError();
  }
  const uint16_t file_id = header_->total_size / header_->max_file_size;
  const uint64_t offset = header_->total_size % header_->max_file_size;
  const uint64_t size_left = header_->max_file_size - offset;
  if (file_id >= header_->max_num_files) {
    GRNXX_ERROR() << "too many files: file_id = " << file_id
                  << ", max_num_files = " << header_->max_num_files;
    throw LogicError();
  }
  if (size_left < size) {
    // This error must not occur.
    GRNXX_ERROR() << "too large chunk: size = " << size
                  << ", size_left = " << size_left;
    throw LogicError();
  }
  ChunkIndex * const chunk_index = &body_chunk_indexes_[chunk_id];
  chunk_index->file_id = file_id;
  chunk_index->offset = offset;
  chunk_index->size = size;
  header_->body_size += size;
  header_->total_size += size;
  ++header_->num_body_chunks;
  return chunk_index;
}

void StorageImpl::register_idle_node(NodeHeader *node_header) {
  if (node_header->status != STORAGE_NODE_IDLE) {
    // This error must not occur.
    GRNXX_ERROR() << "invalid argument: status = " << node_header->status;
    throw LogicError();
  }
  size_t list_id = bit_scan_reverse(node_header->size);
  if (body_chunk_indexes_[node_header->chunk_id].type == SMALL_BODY_CHUNK) {
    if (list_id > bit_scan_reverse(NODE_SIZE_THRESHOLD)) {
      list_id = bit_scan_reverse(NODE_SIZE_THRESHOLD);
    }
  }
  if (header_->oldest_idle_node_ids[list_id] == STORAGE_INVALID_NODE_ID) {
    // The given node is appended to the empty list.
    node_header->next_idle_node_id = node_header->id;
    node_header->prev_idle_node_id = node_header->id;
    header_->oldest_idle_node_ids[list_id] = node_header->id;
  } else {
    // The given node is inserted as the new lastest idle node.
    NodeHeader * const oldest_idle_node_header =
        get_node_header(header_->oldest_idle_node_ids[list_id]);
    NodeHeader * const latest_idle_node_header =
        get_node_header(oldest_idle_node_header->prev_idle_node_id);
    node_header->next_idle_node_id = oldest_idle_node_header->id;
    node_header->prev_idle_node_id = latest_idle_node_header->id;
    latest_idle_node_header->next_idle_node_id = node_header->id;
    oldest_idle_node_header->prev_idle_node_id = node_header->id;
  }
}

void StorageImpl::unregister_idle_node(NodeHeader *node_header) {
  if (node_header->status != STORAGE_NODE_IDLE) {
    // This error must not occur.
    GRNXX_ERROR() << "invalid argument: status = " << node_header->status;
    throw LogicError();
  }
  size_t list_id = bit_scan_reverse(node_header->size);
  if (body_chunk_indexes_[node_header->chunk_id].type == SMALL_BODY_CHUNK) {
    if (list_id > bit_scan_reverse(NODE_SIZE_THRESHOLD)) {
      list_id = bit_scan_reverse(NODE_SIZE_THRESHOLD);
    }
  }
  if (node_header->id == node_header->next_idle_node_id) {
    // The list becomes empty.
    header_->oldest_idle_node_ids[list_id] = STORAGE_INVALID_NODE_ID;
  } else {
    // The specified node is removed from the list.
    NodeHeader * const next_idle_node_header =
        get_node_header(node_header->next_idle_node_id);
    NodeHeader * const prev_idle_node_header =
        get_node_header(node_header->prev_idle_node_id);
    next_idle_node_header->prev_idle_node_id = prev_idle_node_header->id;
    prev_idle_node_header->next_idle_node_id = next_idle_node_header->id;
    if (node_header->id == header_->oldest_idle_node_ids[list_id]) {
      header_->oldest_idle_node_ids[list_id] = next_idle_node_header->id;
    }
  }
}

NodeHeader *StorageImpl::get_node_header(uint32_t node_id) {
  if (node_id >= header_->max_num_nodes) {
    GRNXX_ERROR() << "invalid argument: node_id = " << node_id
                  << ", max_num_nodes = " << header_->max_num_nodes;
    throw LogicError();
  }
  const uint16_t chunk_id =
      bit_scan_reverse(node_id + HEADER_CHUNK_MIN_SIZE);
  Chunk * const chunk = get_header_chunk(chunk_id);
  const uint32_t chunk_size = 1U << chunk_id;
  NodeHeader * const headers = static_cast<NodeHeader *>(chunk->address());
  return headers + (node_id & (chunk_size - 1));
}

void *StorageImpl::get_node_body(const NodeHeader *node_header) {
  Chunk * const chunk = get_body_chunk(node_header->chunk_id);
  return static_cast<uint8_t *>(chunk->address()) + node_header->offset;
}

Chunk *StorageImpl::get_header_chunk(uint16_t chunk_id) {
  if (!header_chunks_[chunk_id]) {
    const ChunkIndex &chunk_index = header_chunk_indexes_[chunk_id];
    File *file = nullptr;
    if (~flags_ & STORAGE_ANONYMOUS) {
      file = reserve_file(chunk_index.file_id,
                          chunk_index.offset + chunk_index.size);
    }
    Lock lock(&mutex_);
    if (!header_chunks_[chunk_id]) {
      header_chunks_[chunk_id].reset(
          create_chunk(file, chunk_index.offset, chunk_index.size));
    }
  }
  return header_chunks_[chunk_id].get();
}

Chunk *StorageImpl::get_body_chunk(uint16_t chunk_id) {
  if (!body_chunks_[chunk_id]) {
    const ChunkIndex &chunk_index = body_chunk_indexes_[chunk_id];
    File *file = nullptr;
    if (~flags_ & STORAGE_ANONYMOUS) {
      file = reserve_file(chunk_index.file_id,
                          chunk_index.offset + chunk_index.size);
    }
    Lock lock(&mutex_);
    if (!body_chunks_[chunk_id]) {
      body_chunks_[chunk_id].reset(
          create_chunk(file, chunk_index.offset, chunk_index.size));
    }
  }
  return body_chunks_[chunk_id].get();
}

File *StorageImpl::reserve_file(uint16_t file_id, uint64_t size) {
  if (!files_[file_id]) {
    // Create a file if missing.
    Lock file_lock(&header_->file_mutex);
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
  }
  // Expand the file if its size is not enough 
  uint64_t file_size = files_[file_id]->get_size();
  if (file_size < size) {
    Lock file_lock(&header_->file_mutex);
    file_size = files_[file_id]->get_size();
    if (file_size < size) {
      files_[file_id]->resize(size);
    }
  }
  return files_[file_id].get();
}

char *StorageImpl::generate_path(uint16_t file_id) {
  // If "path_" ends with ".grn", the generated path also ends with ".grn".
  // In this case, "file_id" is inserted before the ".grn".
  // Otherwise, "file_id" is appended as a suffix.
  const Bytes prefix = path_.get();
  const bool has_extension = prefix.ends_with(".grn");
  const size_t path_size = prefix.size() + 5;
  char * const path = new (std::nothrow) char[path_size];
  if (!path) {
    GRNXX_ERROR() << "new char[] failed: size = " << path_size;
    throw MemoryError();
  }
  if (has_extension) {
    std::memcpy(path, prefix.data(), prefix.size() - 4);
    std::sprintf(path + prefix.size() - 4, "_%03d", file_id);
    std::strcpy(path + prefix.size(), ".grn");
  } else {
    std::memcpy(path, prefix.data(), prefix.size());
    std::sprintf(path + prefix.size(), "_%03d", file_id);
  }
  return path;
}

Chunk *StorageImpl::create_chunk(File *file, uint64_t offset, uint64_t size) {
  ChunkFlags chunk_flags = CHUNK_DEFAULT;
  if (flags_ & STORAGE_HUGE_TLB) {
    chunk_flags |= CHUNK_HUGE_TLB;
  }
  return Chunk::create(file, offset, size, chunk_flags);
}

}  // namespace storage
}  // namespace grnxx
