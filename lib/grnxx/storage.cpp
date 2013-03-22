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

#include <ostream>

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
  if (flags) {
    bool is_first = true;
    GRNXX_FLAGS_WRITE(STORAGE_ANONYMOUS);
    GRNXX_FLAGS_WRITE(STORAGE_CREATE);
    GRNXX_FLAGS_WRITE(STORAGE_HUGE_TLB);
    GRNXX_FLAGS_WRITE(STORAGE_OPEN);
    GRNXX_FLAGS_WRITE(STORAGE_READ_ONLY);
    GRNXX_FLAGS_WRITE(STORAGE_TEMPORARY);
    return builder;
  } else {
    return builder << "0";
  }
}

std::ostream &operator<<(std::ostream &stream, StorageFlags flags) {
  char buf[256];
  StringBuilder builder(buf);
  builder << flags;
  return stream.write(builder.c_str(), builder.length());
}

StorageOptions::StorageOptions()
  : max_num_files(1000),
    max_file_size(1ULL << 40),
    chunk_size_ratio(1.0 / 64) {}

StorageNodeInfo::StorageNodeInfo()
  : id(0),
    status(STORAGE_PHANTOM),
    bits(0),
    chunk_id(0),
    offset(0),
    size(0),
    next_id(0),
    prev_id(0),
    next_phantom_id(0),
    sibling_id(0),
    modified_time(0),
    reserved{},
    user_data{} {}

Storage::Storage() {}
Storage::~Storage() {}

Storage *Storage::open(StorageFlags flags, const char *path,
                       const StorageOptions &options) {
  // TODO
  return nullptr;
}

bool Storage::exists(const char *path) {
  // TODO
  return false;
}

void Storage::unlink(const char *path) {
  // TODO
}

}  // namespace grnxx
