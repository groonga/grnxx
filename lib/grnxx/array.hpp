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

#include "grnxx/array/array_1d.hpp"
#include "grnxx/array/array_2d.hpp"
#include "grnxx/array/array_3d.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/traits.hpp"

namespace grnxx {

class Storage;

constexpr uint64_t ARRAY_DEFAULT_PAGE_SIZE            = 1ULL << 16;
constexpr uint64_t ARRAY_DEFAULT_TABLE_SIZE           = 1ULL << 12;
constexpr uint64_t ARRAY_DEFAULT_SECONDARY_TABLE_SIZE = 1ULL << 12;

template <typename T,
          uint64_t PAGE_SIZE = ARRAY_DEFAULT_PAGE_SIZE,
          uint64_t TABLE_SIZE = 1,
          uint64_t SECONDARY_TABLE_SIZE = 1>
class Array;

// 1D array.
template <typename T, uint64_t PAGE_SIZE>
class Array<T, PAGE_SIZE, 1, 1> {
  static_assert(PAGE_SIZE > 0, "PAGE_SIZE <= 0");

  using Value = typename Traits<T>::Type;
  using ValueArg = typename Traits<T>::ArgumentType;

 public:
  ~Array() {}

  // Create an array.
  static Array *create(Storage *storage, uint32_t storage_node_id) {
    std::unique_ptr<Array> array(new (std::nothrow) Array);
    if (!array) {
      GRNXX_ERROR() << "new grnxx::Array failed: PAGE_SIZE = " << PAGE_SIZE;
      return nullptr;
    }
    if (!array->impl_.create(storage, storage_node_id, sizeof(Value),
                             PAGE_SIZE)) {
      return nullptr;
    }
    return array.release();
  }

  // Create an array with the default value.
  static Array *create(Storage *storage, uint32_t storage_node_id,
                       ValueArg default_value) {
    std::unique_ptr<Array> array(new (std::nothrow) Array);
    if (!array) {
      GRNXX_ERROR() << "new grnxx::Array failed: PAGE_SIZE = " << PAGE_SIZE;
      return nullptr;
    }
    if (!array->impl_.create(storage, storage_node_id, sizeof(Value),
                             PAGE_SIZE, &default_value, fill_page)) {
      return nullptr;
    }
    return array.release();
  }

  // Open an array.
  static Array *open(Storage *storage, uint32_t storage_node_id) {
    std::unique_ptr<Array> array(new (std::nothrow) Array);
    if (!array) {
      GRNXX_ERROR() << "new grnxx::Array failed: PAGE_SIZE = " << PAGE_SIZE;
      return nullptr;
    }
    if (!array->impl_.open(storage, storage_node_id, sizeof(Value),
                           PAGE_SIZE)) {
      return nullptr;
    }
    return array.release();
  }

  // Unlink an array.
  static bool unlink(Storage *storage, uint32_t storage_node_id) {
    return Array1D::unlink(storage, storage_node_id, sizeof(Value), PAGE_SIZE);
  }

  // Return the number of values in each page.
  static constexpr uint64_t page_size() {
    return PAGE_SIZE;
  }
  // Return the number of pages in each table.
  static constexpr uint64_t table_size() {
    return 1;
  }
  // Return the number of tables in each secondary table.
  static constexpr uint64_t secondary_table_size() {
    return 1;
  }
  // Return the number of values in Array.
  static constexpr uint64_t size() {
    return page_size() * table_size() * secondary_table_size();
  }

  // Return the storage node ID.
  uint32_t storage_node_id() const {
    return impl_.storage_node_id();
  }

  // Get a reference to a value.
  Value &operator[](uint64_t value_id) {
    Value * const page = get_page(value_id / PAGE_SIZE);
    return page[value_id % PAGE_SIZE];
  }

  // Get a value and return true.
  // The value is assigned to "*value".
  bool get(uint64_t value_id, Value *value) {
    const Value * const page = get_page(value_id / PAGE_SIZE);
    *value = page[value_id % PAGE_SIZE];
    return true;
  }

  // Set a value and return true.
  bool set(uint64_t value_id, ValueArg value) {
    Value * const page = get_page(value_id / PAGE_SIZE);
    page[value_id % PAGE_SIZE] = value;
    return true;
  }

  // Get a page and return its starting address.
  Value *get_page(uint64_t) {
    return impl_.get_page<Value>();
  }

 private:
  Array1D impl_;

  Array() : impl_() {}

  // This function is used to fill a new page with the default value.
  static void fill_page(void *page, const void *value) {
    Value *values = static_cast<Value *>(page);
    for (uint64_t i = 0; i < PAGE_SIZE; ++i) {
      std::memcpy(&values[i], value, sizeof(Value));
    }
  }
};

// 2D array.
template <typename T, uint64_t PAGE_SIZE, uint64_t TABLE_SIZE>
class Array<T, PAGE_SIZE, TABLE_SIZE, 1> {
  static_assert((PAGE_SIZE > 0) && ((PAGE_SIZE & (PAGE_SIZE - 1)) == 0),
                "PAGE_SIZE must be a power of two");
  static_assert(TABLE_SIZE > 0, "TABLE_SIZE <= 0");

  using Value = typename Traits<T>::Type;
  using ValueArg = typename Traits<T>::ArgumentType;

 public:
  ~Array() {}

  // Create an array.
  static Array *create(Storage *storage, uint32_t storage_node_id) {
    std::unique_ptr<Array> array(new (std::nothrow) Array);
    if (!array) {
      GRNXX_ERROR() << "new grnxx::Array failed: PAGE_SIZE = " << PAGE_SIZE
                    << ", TABLE_SIZE = " << TABLE_SIZE;
      return nullptr;
    }
    if (!array->impl_.create(storage, storage_node_id, sizeof(Value),
                             PAGE_SIZE, TABLE_SIZE)) {
      return nullptr;
    }
    return array.release();
  }

  // Create an array with the default value.
  static Array *create(Storage *storage, uint32_t storage_node_id,
                       ValueArg default_value) {
    std::unique_ptr<Array> array(new (std::nothrow) Array);
    if (!array) {
      GRNXX_ERROR() << "new grnxx::Array failed: PAGE_SIZE = " << PAGE_SIZE
                    << ", TABLE_SIZE = " << TABLE_SIZE;
      return nullptr;
    }
    if (!array->impl_.create(storage, storage_node_id, sizeof(Value),
                             PAGE_SIZE, TABLE_SIZE,
                             &default_value, fill_page)) {
      return nullptr;
    }
    return array.release();
  }

  // Open an array.
  static Array *open(Storage *storage, uint32_t storage_node_id) {
    std::unique_ptr<Array> array(new (std::nothrow) Array);
    if (!array) {
      GRNXX_ERROR() << "new grnxx::Array failed: PAGE_SIZE = " << PAGE_SIZE
                    << ", TABLE_SIZE = " << TABLE_SIZE;
      return nullptr;
    }
    if (!array->impl_.open(storage, storage_node_id, sizeof(Value),
                           PAGE_SIZE, TABLE_SIZE)) {
      return nullptr;
    }
    return array.release();
  }

  // Unlink an array.
  static bool unlink(Storage *storage, uint32_t storage_node_id) {
    return Array2D::unlink(storage, storage_node_id, sizeof(Value),
                           PAGE_SIZE, TABLE_SIZE);
  }

  // Return the number of values in each page.
  static constexpr uint64_t page_size() {
    return PAGE_SIZE;
  }
  // Return the number of pages in each table.
  static constexpr uint64_t table_size() {
    return TABLE_SIZE;
  }
  // Return the number of tables in each secondary table.
  static constexpr uint64_t secondary_table_size() {
    return 1;
  }
  // Return the number of values in Array.
  static constexpr uint64_t size() {
    return page_size() * table_size() * secondary_table_size();
  }

  // Return the storage node ID.
  uint32_t storage_node_id() const {
    return impl_.storage_node_id();
  }

  // Get a reference to a value.
  // This function throws an exception on failure.
  Value &operator[](uint64_t value_id) {
    Value * const page =
        impl_.get_page<Value, TABLE_SIZE>(value_id / PAGE_SIZE);
    return page[value_id % PAGE_SIZE];
  }

  // Get a value and return true on success.
  // The value is assigned to "*value".
  bool get(uint64_t value_id, Value *value) {
    const Value * const page = get_page(value_id / PAGE_SIZE);
    if (!page) {
      return false;
    }
    *value = page[value_id % PAGE_SIZE];
    return true;
  }

  // Set a value and return true on success.
  bool set(uint64_t value_id, ValueArg value) {
    Value * const page = get_page(value_id / PAGE_SIZE);
    if (!page) {
      return false;
    }
    page[value_id % PAGE_SIZE] = value;
    return true;
  }

  // Get a page and return its starting address on success.
  Value *get_page(uint64_t page_id) {
    return impl_.get_page_nothrow<Value, TABLE_SIZE>(page_id);
  }

 private:
  Array2D impl_;

  Array() : impl_() {}

  // This function is used to fill a new page with the default value.
  static void fill_page(void *page, const void *value) {
    Value *values = static_cast<Value *>(page);
    for (uint64_t i = 0; i < PAGE_SIZE; ++i) {
      std::memcpy(&values[i], value, sizeof(Value));
    }
  }
};

// 3D array.
template <typename T,
          uint64_t PAGE_SIZE,
          uint64_t TABLE_SIZE,
          uint64_t SECONDARY_TABLE_SIZE>
class Array {
  static_assert((PAGE_SIZE > 0) && ((PAGE_SIZE & (PAGE_SIZE - 1)) == 0),
                "PAGE_SIZE must be a power of two");
  static_assert((TABLE_SIZE > 0) && ((TABLE_SIZE & (TABLE_SIZE - 1)) == 0),
                "TABLE_SIZE must be a power of two");
  static_assert(SECONDARY_TABLE_SIZE > 0, "SECONDARY_TABLE_SIZE <= 0");

  using Value = typename Traits<T>::Type;
  using ValueArg = typename Traits<T>::ArgumentType;

 public:
  ~Array() {}

  // Create an array.
  static Array *create(Storage *storage, uint32_t storage_node_id) {
    std::unique_ptr<Array> array(new (std::nothrow) Array);
    if (!array) {
      GRNXX_ERROR() << "new grnxx::Array failed: PAGE_SIZE = " << PAGE_SIZE
                    << ", TABLE_SIZE = " << TABLE_SIZE
                    << ", SECONDARY_TABLE_SIZE = " << SECONDARY_TABLE_SIZE;
      return nullptr;
    }
    if (!array->impl_.create(storage, storage_node_id, sizeof(Value),
                             PAGE_SIZE, TABLE_SIZE, SECONDARY_TABLE_SIZE)) {
      return nullptr;
    }
    return array.release();
  }

  // Create an array with the default value.
  static Array *create(Storage *storage, uint32_t storage_node_id,
                       ValueArg default_value) {
    std::unique_ptr<Array> array(new (std::nothrow) Array);
    if (!array) {
      GRNXX_ERROR() << "new grnxx::Array failed: PAGE_SIZE = " << PAGE_SIZE
                    << ", TABLE_SIZE = " << TABLE_SIZE
                    << ", SECONDARY_TABLE_SIZE = " << SECONDARY_TABLE_SIZE;
      return nullptr;
    }
    if (!array->impl_.create(storage, storage_node_id, sizeof(Value),
                             PAGE_SIZE, TABLE_SIZE, SECONDARY_TABLE_SIZE,
                             &default_value, fill_page)) {
      return nullptr;
    }
    return array.release();
  }

  // Open an array.
  static Array *open(Storage *storage, uint32_t storage_node_id) {
    std::unique_ptr<Array> array(new (std::nothrow) Array);
    if (!array) {
      GRNXX_ERROR() << "new grnxx::Array failed: PAGE_SIZE = " << PAGE_SIZE
                    << ", TABLE_SIZE = " << TABLE_SIZE
                    << ", SECONDARY_TABLE_SIZE = " << SECONDARY_TABLE_SIZE;
      return nullptr;
    }
    if (!array->impl_.open(storage, storage_node_id, sizeof(Value),
                           PAGE_SIZE, TABLE_SIZE, SECONDARY_TABLE_SIZE)) {
      return nullptr;
    }
    return array.release();
  }

  // Unlink an array.
  static bool unlink(Storage *storage, uint32_t storage_node_id) {
    return Array3D::unlink(storage, storage_node_id, sizeof(Value),
                           PAGE_SIZE, TABLE_SIZE, SECONDARY_TABLE_SIZE);
  }

  // Return the number of values in each page.
  static constexpr uint64_t page_size() {
    return PAGE_SIZE;
  }
  // Return the number of pages in each table.
  static constexpr uint64_t table_size() {
    return TABLE_SIZE;
  }
  // Return the number of tables in each secondary table.
  static constexpr uint64_t secondary_table_size() {
    return SECONDARY_TABLE_SIZE;
  }
  // Return the number of values in Array.
  static constexpr uint64_t size() {
    return page_size() * table_size() * secondary_table_size();
  }

  // Return the storage node ID.
  uint32_t storage_node_id() const {
    return impl_.storage_node_id();
  }

  // Get a reference to a value.
  // This function throws an exception on failure.
  Value &operator[](uint64_t value_id) {
    Value * const page =
        impl_.get_page<Value, TABLE_SIZE,
                       SECONDARY_TABLE_SIZE>(value_id / PAGE_SIZE);
    return page[value_id % PAGE_SIZE];
  }

  // Get a value and return true on success.
  // The value is assigned to "*value".
  bool get(uint64_t value_id, Value *value) {
    const Value * const page = get_page(value_id / PAGE_SIZE);
    if (!page) {
      return false;
    }
    *value = page[value_id % PAGE_SIZE];
    return true;
  }

  // Set a value and return true on success.
  bool set(uint64_t value_id, ValueArg value) {
    Value * const page = get_page(value_id / PAGE_SIZE);
    if (!page) {
      return false;
    }
    page[value_id % PAGE_SIZE] = value;
    return true;
  }

  // Get a page and return its starting address on success.
  Value *get_page(uint64_t page_id) {
    return impl_.get_page_nothrow<Value, TABLE_SIZE,
                                  SECONDARY_TABLE_SIZE>(page_id);
  }

 private:
  Array3D impl_;

  Array() : impl_() {}

  // This function is used to fill a new page with the default value.
  static void fill_page(void *page, const void *value) {
    Value *values = static_cast<Value *>(page);
    for (uint64_t i = 0; i < PAGE_SIZE; ++i) {
      std::memcpy(&values[i], value, sizeof(Value));
    }
  }
};

}  // namespace grnxx

#endif  // GRNXX_ARRAY_HPP
