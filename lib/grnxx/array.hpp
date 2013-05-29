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
#ifndef GRNXX_ARRAY_HPP
#define GRNXX_ARRAY_HPP

#include "grnxx/features.hpp"

#include <cstring>
#include <memory>
#include <new>

#include "grnxx/array_impl.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Storage;

constexpr uint64_t ARRAY_DEFAULT_PAGE_SIZE            = 1ULL << 16;
constexpr uint64_t ARRAY_DEFAULT_TABLE_SIZE           = 1ULL << 12;
constexpr uint64_t ARRAY_DEFAULT_SECONDARY_TABLE_SIZE = 1ULL << 12;

template <typename T,
          uint64_t PAGE_SIZE = ARRAY_DEFAULT_PAGE_SIZE,
          uint64_t TABLE_SIZE = ARRAY_DEFAULT_TABLE_SIZE,
          uint64_t SECONDARY_TABLE_SIZE = ARRAY_DEFAULT_SECONDARY_TABLE_SIZE>
class Array {
  static_assert((PAGE_SIZE > 0) && ((PAGE_SIZE & (PAGE_SIZE - 1)) == 0),
                "PAGE_SIZE must be a power of two");
  static_assert((TABLE_SIZE > 0) && ((TABLE_SIZE & (TABLE_SIZE - 1)) == 0),
                "TABLE_SIZE must be a power of two");
  static_assert(SECONDARY_TABLE_SIZE > 0, "SECONDARY_TABLE_SIZE <= 0");

  using ArrayImpl = ArrayImpl<T, PAGE_SIZE, TABLE_SIZE, SECONDARY_TABLE_SIZE>;

 public:
  using Value = typename ArrayImpl::Value;
  using ValueArg = typename ArrayImpl::ValueArg;

  ~Array() {}

  // Create an array.
  static Array *create(Storage *storage, uint32_t storage_node_id) {
    std::unique_ptr<Array> array(create_instance());
    if (!array) {
      return nullptr;
    }
    if (!array->impl_.create(storage, storage_node_id)) {
      return nullptr;
    }
    return array.release();
  }

  // Create an array with the default value.
  static Array *create(Storage *storage, uint32_t storage_node_id,
                       ValueArg default_value) {
    std::unique_ptr<Array> array(create_instance());
    if (!array) {
      return nullptr;
    }
    if (!array->impl_.create(storage, storage_node_id, default_value)) {
      return nullptr;
    }
    return array.release();
  }

  // Open an array.
  static Array *open(Storage *storage, uint32_t storage_node_id) {
    std::unique_ptr<Array> array(create_instance());
    if (!array) {
      return nullptr;
    }
    if (!array->impl_.open(storage, storage_node_id)) {
      return nullptr;
    }
    return array.release();
  }

  // Unlink an array.
  static bool unlink(Storage *storage, uint32_t storage_node_id) {
    return ArrayImpl::unlink(storage, storage_node_id);
  }

  // Return the number of values in each page.
  static constexpr uint64_t page_size() {
    return ArrayImpl::page_size();
  }
  // Return the number of pages in each table.
  static constexpr uint64_t table_size() {
    return ArrayImpl::table_size();
  }
  // Return the number of tables in each secondary table.
  static constexpr uint64_t secondary_table_size() {
    return ArrayImpl::secondary_table_size();
  }
  // Return the number of values in Array.
  static constexpr uint64_t size() {
    return ArrayImpl::size();
  }

  // Return the storage node ID.
  uint32_t storage_node_id() const {
    return impl_.storage_node_id();
  }

  // Get a value and return true on success.
  // The value is assigned to "*value" iff "value" != nullptr.
  bool get(uint64_t value_id, Value *value) {
    return impl_.get(value_id, value);
  }
  // Set a value and return true on success.
  bool set(uint64_t value_id, ValueArg value) {
    return impl_.set(value_id, value);
  }
  // Get a value and return its address on success.
  Value *get_pointer(uint64_t value_id) {
    return impl_.get_pointer(value_id);
  }
  // Get a page and return its starting address on success.
  Value *get_page(uint64_t page_id) {
    return impl_.get_page(page_id);
  }

 private:
  ArrayImpl impl_;

  Array() : impl_() {}

  static Array *create_instance() {
    Array * const array = new (std::nothrow) Array;
    if (!array) {
      GRNXX_ERROR() << "new grnxx::Array failed: "
                    << "value_size = " << sizeof(Value)
                    << ", page_size = " << PAGE_SIZE
                    << ", table_size = " << TABLE_SIZE
                    << ", secondary_table_size = " << SECONDARY_TABLE_SIZE;
    }
    return array;
  }
};

}  // namespace grnxx

#endif  // GRNXX_ARRAY_HPP
