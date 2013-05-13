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
#ifndef GRNXX_ARRAY_ARRAY_3D_HPP
#define GRNXX_ARRAY_ARRAY_3D_HPP

#include "grnxx/mutex.hpp"
#include "grnxx/storage.hpp"
#include "grnxx/traits.hpp"

namespace grnxx {

struct Array3DHeader;

class Array3D {
  using FillPage = void (*)(void *page, const void *value);

 public:
  Array3D();
  ~Array3D();

  static Array3D *create(Storage *storage, uint32_t storage_node_id,
                         uint64_t value_size, uint64_t page_size,
                         uint64_t table_size, uint64_t secondary_table_size,
                         const void *default_value = nullptr,
                         FillPage fill_page = nullptr);

  static Array3D *open(Storage *storage, uint32_t storage_node_id,
                       uint64_t value_size, uint64_t page_size,
                       uint64_t table_size, uint64_t secondary_table_size,
                       FillPage fill_page);

  static bool unlink(Storage *storage, uint32_t storage_node_id,
                     uint64_t value_size, uint64_t page_size,
                     uint64_t table_size, uint64_t secondary_table_size);

  uint32_t storage_node_id() const {
    return storage_node_.id();
  }

  template <typename T, uint64_t TABLE_SIZE, uint64_t SECONDARY_TABLE_SIZE>
  T *get_page(uint64_t page_id) {
    const uint64_t table_id = page_id / TABLE_SIZE;
    page_id %= TABLE_SIZE;
    if (!table_caches_[table_id] || !table_caches_[table_id][page_id]) {
      initialize_page(table_id, page_id);
    }
    return static_cast<T *>(table_caches_[table_id][page_id]);
  }

  template <typename T, uint64_t TABLE_SIZE, uint64_t SECONDARY_TABLE_SIZE>
  T *get_page_nothrow(uint64_t page_id) {
    const uint64_t table_id = page_id / TABLE_SIZE;
    page_id %= TABLE_SIZE;
    if (!table_caches_[table_id] || !table_caches_[table_id][page_id]) {
      if (!initialize_page_nothrow(table_id, page_id)) {
        return nullptr;
      }
    }
    return static_cast<T *>(table_caches_[table_id][page_id]);
  }

 private:
  Storage *storage_;
  StorageNode storage_node_;
  Array3DHeader *header_;
  void *default_value_;
  FillPage fill_page_;
  uint32_t *secondary_table_;
  std::unique_ptr<std::unique_ptr<void *[]>[]> table_caches_;
  Mutex page_mutex_;
  Mutex table_mutex_;
  Mutex secondary_table_mutex_;

  bool create_array(Storage *storage, uint32_t storage_node_id,
                    uint64_t value_size, uint64_t page_size,
                    uint64_t table_size, uint64_t secondary_table_size,
                    const void *default_value, FillPage fill_page);
  bool open_array(Storage *storage, uint32_t storage_node_id,
                  uint64_t value_size, uint64_t page_size,
                  uint64_t table_size, uint64_t secondary_table_size,
                  FillPage fill_page);

  void initialize_page(uint64_t table_id, uint64_t page_id);
  bool initialize_page_nothrow(uint64_t table_id, uint64_t page_id);
  bool initialize_table(uint64_t table_id);
  bool initialize_secondary_table();
};

}  // namespace grnxx

#endif  // GRNXX_ARRAY_ARRAY_3D_HPP
