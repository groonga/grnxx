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

constexpr uint32_t MAX_NUM_NODE_HEADER_CHUNKS = 32;
constexpr uint32_t MAX_NUM_NODE_BODY_CHUNKS   = 2048;

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
  if (path && (~flags & STORAGE_TEMPORARY)) {
    if (!storage->create_persistent_storage(path, flags, options)) {
      return nullptr;
    }
  } else if (flags & STORAGE_TEMPORARY) {
    if (!storage->create_temporary_storage(path, flags, options)) {
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
  if (node_id >= header_->num_nodes) {
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

bool StorageImpl::create_persistent_storage(const char *path,
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
  return false;
}

bool StorageImpl::create_temporary_storage(const char *path,
                                           StorageFlags flags,
                                           const StorageOptions &options) {
  if (path) {
    path_.reset(Path::clone_path(path));
    if (!path_) {
      return false;
    }
  }
  flags_ |= STORAGE_TEMPORARY;
  if (flags & STORAGE_HUGE_TLB) {
    flags_ |= STORAGE_HUGE_TLB;
  }
  // TODO
  return false;
}

bool StorageImpl::create_anonymous_storage(StorageFlags flags,
                                           const StorageOptions &options) {
  flags_ |= STORAGE_ANONYMOUS;
  if (flags & STORAGE_HUGE_TLB) {
    flags_ |= STORAGE_HUGE_TLB;
  }
  // TODO
  return false;
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
  // TODO
  return false;
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
  return false;
}

bool StorageImpl::prepare_files_and_chunks(const StorageOptions &options) {
  files_.reset(
      new (std::nothrow) std::unique_ptr<File>[options.max_num_files]);
  if (!files_) {
    GRNXX_ERROR() << "new std::unique_ptr<grnxx::storage::File>[] failed: "
                  << "size = " << options.max_num_files;
    return false;
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

}  // namespace storage
}  // namespace grnxx
