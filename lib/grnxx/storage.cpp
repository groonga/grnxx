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

namespace grnxx {

StorageOptions::StorageOptions()
  : max_num_files(1000),
    max_file_size(1ULL << 40),
    chunk_size_ratio(1.0 / 64),
    root_size(4096) {}

StorageNodeHeader::StorageNodeHeader()
  : id(0),
    status(STORAGE_PHANTOM),
    bits(0),
    chunk_id(0),
    offset(0),
    size(0),
    next_node_id(0),
    prev_node_id(0),
    next_phantom_node_id(0),
    sibling_node_id(0),
    last_modified_time(0),
    reserved{},
    user_data{} {}

Storage::Storage() {}
Storage::~Storage() {}

Storage *Storage::create(const char *path,
                         StorageFlags flags,
                         const StorageOptions &options) {
  // TODO
  return nullptr;
}

Storage *Storage::open(const char *path,
                       StorageFlags flags) {
  // TODO
  return nullptr;
}

Storage *Storage::open_or_create(const char *path,
                                 StorageFlags flags,
                                 const StorageOptions &options) {
  // TODO
  return nullptr;
}

bool Storage::exists(const char *path) {
  std::unique_ptr<Storage> storage(open(path, STORAGE_READ_ONLY));
  if (!storage) {
    // TODO: Error: memory allocation failed.
    return false;
  }
  return true;
}

bool Storage::unlink(const char *path) {
  std::unique_ptr<Storage> storage(open(path, STORAGE_READ_ONLY));
  if (!storage) {
    // TODO: Error: memory allocation failed.
    return false;
  }
  // TODO: Remove files.
  return true;
}

}  // namespace grnxx
