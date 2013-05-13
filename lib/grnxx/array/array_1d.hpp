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
#ifndef GRNXX_ARRAY_ARRAY_1D_HPP
#define GRNXX_ARRAY_ARRAY_1D_HPP

#include "grnxx/storage.hpp"
#include "grnxx/traits.hpp"

namespace grnxx {

struct Array1DHeader;

class Array1D {
  using FillPage = void (*)(void *page, const void *value);

 public:
  Array1D();
  ~Array1D();

  bool create(Storage *storage, uint32_t storage_node_id,
              uint64_t value_size, uint64_t page_size,
              const void *default_value = nullptr,
              FillPage fill_page = nullptr);
  bool open(Storage *storage, uint32_t storage_node_id,
            uint64_t value_size, uint64_t page_size);

  static bool unlink(Storage *storage, uint32_t storage_node_id,
                     uint64_t value_size, uint64_t page_size);

  uint32_t storage_node_id() const {
    return storage_node_.id();
  }

  template <typename T>
  T *get_page() {
    return static_cast<T *>(page_);
  }

 private:
  StorageNode storage_node_;
  Array1DHeader *header_;
  void *page_;
};

}  // namespace grnxx

#endif  // GRNXX_ARRAY_ARRAY_1D_HPP
