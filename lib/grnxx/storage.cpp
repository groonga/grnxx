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
#include "grnxx/storage.hpp"

#include "grnxx/storage/node_header.hpp"
#include "grnxx/storage/storage_impl.hpp"
#include "grnxx/string_builder.hpp"

namespace grnxx {
namespace {

constexpr uint64_t MAX_FILE_SIZE_LOWER_LIMIT = 1ULL << 30;  // 1GB.
constexpr uint64_t MAX_FILE_SIZE_UPPER_LIMIT = 1ULL << 63;  // 8EB.
constexpr uint64_t MAX_FILE_SIZE_DEFAULT     = 1ULL << 40;  // 1TB.
constexpr uint16_t MAX_NUM_FILES_LOWER_LIMIT = 1;
constexpr uint16_t MAX_NUM_FILES_UPPER_LIMIT = 1000;
constexpr uint16_t MAX_NUM_FILES_DEFAULT     = MAX_NUM_FILES_UPPER_LIMIT;
constexpr uint64_t ROOT_SIZE_DEFAULT         = 1ULL << 12;  // 4KB.

}  // namespace

#define GRNXX_FLAGS_WRITE(flag) do { \
  if (flags & flag) { \
    if (!is_first) { \
      builder << " | "; \
    } \
    builder << #flag; \
    is_first = false; \
  } \
} while (false)

StringBuilder &operator<<(StringBuilder &builder, StorageFlags flags) {
  bool is_first = true;
  GRNXX_FLAGS_WRITE(STORAGE_ANONYMOUS);
  GRNXX_FLAGS_WRITE(STORAGE_HUGE_TLB);
  GRNXX_FLAGS_WRITE(STORAGE_READ_ONLY);
  GRNXX_FLAGS_WRITE(STORAGE_TEMPORARY);
  if (is_first) {
    builder << "STORAGE_DEFAULT";
  }
  return builder;
}

#define GRNXX_STATUS_CASE(status) \
  case status: { \
    return builder << #status; \
  }

StringBuilder &operator<<(StringBuilder &builder, StorageNodeStatus status) {
  switch (status) {
    GRNXX_STATUS_CASE(STORAGE_NODE_PHANTOM)
    GRNXX_STATUS_CASE(STORAGE_NODE_ACTIVE)
    GRNXX_STATUS_CASE(STORAGE_NODE_UNLINKED)
    GRNXX_STATUS_CASE(STORAGE_NODE_IDLE)
    default: {
      return builder << "n/a";
    }
  }
}

StorageOptions::StorageOptions()
    : max_file_size(MAX_FILE_SIZE_DEFAULT),
      max_num_files(MAX_NUM_FILES_DEFAULT),
      root_size(ROOT_SIZE_DEFAULT) {}

bool StorageOptions::is_valid() const {
  if ((max_file_size < MAX_FILE_SIZE_LOWER_LIMIT) ||
      (max_file_size > MAX_FILE_SIZE_UPPER_LIMIT)) {
    return false;
  }
  if ((max_num_files < MAX_NUM_FILES_LOWER_LIMIT) ||
      (max_num_files > MAX_NUM_FILES_UPPER_LIMIT)) {
    return false;
  }
  if (root_size > max_file_size) {
    return false;
  }
  return true;
}

StringBuilder &operator<<(StringBuilder &builder,
                          const StorageOptions &options) {
  if (!builder) {
    return builder;
  }
  return builder << "{ max_num_files = " << options.max_num_files
                 << ", max_file_size = " << options.max_file_size
                 << ", root_size = " << options.root_size << " }";
}

uint32_t StorageNode::id() const {
  return header_->id;
}

StorageNodeStatus StorageNode::status() const {
  return header_->status;
}

uint64_t StorageNode::size() const {
  return header_->size;
}

Time StorageNode::modified_time() const {
  return header_->modified_time;
}

void *StorageNode::user_data() const {
  return header_->user_data;
}

Storage::Storage() {}
Storage::~Storage() {}

Storage *Storage::create(const char *path,
                         StorageFlags flags,
                         const StorageOptions &options) {
  return StorageImpl::create(path, flags, options);
}

Storage *Storage::open(const char *path,
                       StorageFlags flags) {
  return StorageImpl::open(path, flags);
}

Storage *Storage::open_or_create(const char *path,
                                 StorageFlags flags,
                                 const StorageOptions &options) {
  return StorageImpl::open_or_create(path, flags, options);
}

bool Storage::exists(const char *path) {
  return StorageImpl::exists(path);
}

bool Storage::unlink(const char *path) {
  return StorageImpl::unlink(path);
}

}  // namespace grnxx
