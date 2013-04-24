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
#ifndef GRNXX_STORAGE_STORAGE_IMPL_HPP
#define GRNXX_STORAGE_STORAGE_IMPL_HPP

#include "grnxx/storage.hpp"

namespace grnxx {
namespace storage {

class StorageImpl : public Storage {
 public:
  StorageImpl();
  ~StorageImpl();

  static StorageImpl *create(const char *path,
                             StorageFlags flags,
                             const StorageOptions &options);
  static StorageImpl *open(const char *path,
                           StorageFlags flags);
  static StorageImpl *open_or_create(const char *path,
                                     StorageFlags flags,
                                     const StorageOptions &options);

  static bool exists(const char *path);
  static bool unlink(const char *path);

  StorageNode create_node(uint32_t parent_node_id, uint64_t size);
  StorageNode open_node(uint32_t node_id);

  bool unlink_node(uint32_t node_id);

  bool sweep(Duration lifetime);

  const char *path() const;
  StorageFlags flags() const;

  // TODO: Member functions to get details, such as total size, #nodes, etc.

 private:
};

}  // namespace storage
}  // namespace grnxx

#endif  // GRNXX_STORAGE_STORAGE_IMPL_HPP
