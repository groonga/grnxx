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
    GRNXX_STATUS_CASE(STORAGE_NODE_MARKED)
    GRNXX_STATUS_CASE(STORAGE_NODE_UNLINKED)
    GRNXX_STATUS_CASE(STORAGE_NODE_IDLE)
    default: {
      return builder << "n/a";
    }
  }
}

StorageOptions::StorageOptions()
    : max_num_files(1000),
      max_file_size(1ULL << 40),
      root_size(4096) {}

uint32_t StorageNode::id() const {
  return header_->id;
}

StorageNodeStatus StorageNode::status() const {
  return header_->status;
}

uint64_t StorageNode::size() const {
  return header_->size << header_->bits;
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
