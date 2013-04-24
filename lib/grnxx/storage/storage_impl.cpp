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

namespace grnxx {
namespace storage {

StorageImpl::StorageImpl() : Storage() {}
StorageImpl::~StorageImpl() {}

StorageImpl *StorageImpl::create(const char *path,
                                 StorageFlags flags,
                                 const StorageOptions &options) {
  // TODO
  return nullptr;
}

StorageImpl *StorageImpl::open(const char *path,
                               StorageFlags flags) {
  // TODO
  return nullptr;
}

StorageImpl *StorageImpl::open_or_create(const char *path,
                                         StorageFlags flags,
                                         const StorageOptions &options) {
  // TODO
  return nullptr;
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
  // TODO
  return StorageNode(nullptr, nullptr);
}

StorageNode StorageImpl::open_node(uint32_t node_id) {
  // TODO
  return StorageNode(nullptr, nullptr);
}

bool StorageImpl::unlink_node(uint32_t node_id) {
  // TODO
  return false;
}

bool StorageImpl::sweep(Duration lifetime) {
  // TODO
  return false;
}

const char *StorageImpl::path() const {
  // TODO
  return nullptr;
}

StorageFlags StorageImpl::flags() const {
  // TODO
  return STORAGE_DEFAULT;
}

}  // namespace storage
}  // namespace grnxx
