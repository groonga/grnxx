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

#include "grnxx/logger.hpp"
#include "grnxx/storage/chunk.hpp"
#include "grnxx/storage/chunk_index.hpp"
#include "grnxx/storage/file.hpp"
#include "grnxx/storage/header.hpp"
#include "grnxx/storage/path.hpp"

namespace grnxx {
namespace storage {
namespace {

constexpr uint32_t MAX_NODE_ID = INVALID_NODE_ID - 1;

constexpr uint16_t MAX_NUM_NODE_HEADER_CHUNKS = 32;
constexpr uint16_t MAX_NUM_NODE_BODY_CHUNKS   = 2048;

constexpr uint64_t CHUNK_UNIT_SIZE        = 1 << 16;
constexpr size_t   CHUNK_INDEX_TOTAL_SIZE =
    CHUNK_INDEX_SIZE * (MAX_NUM_NODE_HEADER_CHUNKS + MAX_NUM_NODE_BODY_CHUNKS);

constexpr uint64_t HEADER_CHUNK_SIZE = CHUNK_UNIT_SIZE;

static_assert(HEADER_CHUNK_SIZE <= (HEADER_SIZE + CHUNK_INDEX_TOTAL_SIZE),
              "HEADER_CHUNK_SIZE > (HEADER_SIZE + CHUNK_INDEX_TOTAL_SIZE)");

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
      node_body_chunks_() {}

StorageImpl::~StorageImpl() {}

StorageImpl *StorageImpl::create(const char *path,
                                 StorageFlags flags,
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

StorageImpl *StorageImpl::open(const char *path,
                               StorageFlags flags) {
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

StorageImpl *StorageImpl::open_or_create(const char *path,
                                         StorageFlags flags,
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
  // TODO
  return StorageNode(nullptr, nullptr);
}

StorageNode StorageImpl::open_node(uint32_t node_id) {
  if (node_id >= header_->num_nodes) {
    GRNXX_ERROR() << "invalid argument: node_id = " << node_id
                  << ", num_nodes = " << header_->num_nodes;
    return StorageNode(nullptr, nullptr);
  }
  // TODO
  return StorageNode(nullptr, nullptr);
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
  // TODO: Create the root node.
  // TODO: Initialize the header.
  header_->validate();
  if (!prepare_pointers()) {
    return false;
  }
  files_[0] = std::move(header_file);
  header_chunk_ = std::move(header_chunk);
  return false;
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
  // TODO: Create the root node.
  // TODO: Initialize the header.
  header_->validate();
  if (!prepare_pointers()) {
    return false;
  }
  header_chunk_ = std::move(header_chunk);
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

bool StorageImpl::open_or_create_storage(const char *path,
                                         StorageFlags flags,
                                         const StorageOptions &options) {
  path_.reset(Path::clone_path(path));
  if (!path_) {
    return false;
  }
  if (flags & STORAGE_HUGE_TLB) {
    flags_ |= STORAGE_HUGE_TLB;
  }
  // TODO
  if (!prepare_pointers()) {
    return false;
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
  node_header_chunk_indexes_ = reinterpret_cast<ChunkIndex *>(header_ + 1);
  node_body_chunk_indexes_ =
      node_header_chunk_indexes_ + MAX_NUM_NODE_HEADER_CHUNKS;
}

bool StorageImpl::prepare_files_and_chunks(uint16_t max_num_files) {
  if (~flags_ & STORAGE_ANONYMOUS) {
    files_.reset(new (std::nothrow) std::unique_ptr<File>[max_num_files]);
    if (!files_) {
      GRNXX_ERROR() << "new std::unique_ptr<grnxx::storage::File>[] failed: "
                    << "size = " << max_num_files;
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

Chunk *StorageImpl::create_chunk(File *file, int64_t offset, int64_t size) {
  ChunkFlags chunk_flags = CHUNK_DEFAULT;
  if (flags_ & STORAGE_HUGE_TLB) {
    chunk_flags |= CHUNK_HUGE_TLB;
  }
  return Chunk::create(file, offset, size, chunk_flags);
}

}  // namespace storage
}  // namespace grnxx
