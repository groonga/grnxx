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
#ifndef GRNXX_ARRAY_IMPL_HPP
#define GRNXX_ARRAY_IMPL_HPP

#include "grnxx/features.hpp"

#include <cstring>
#include <memory>

#include "grnxx/mutex.hpp"
#include "grnxx/traits.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Storage;

struct ArrayHeader;

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
    return storage_node_id_;
  }

  template <typename T>
  T *get_page() {
    return static_cast<T *>(page_);
  }

 private:
  uint32_t storage_node_id_;
  ArrayHeader *header_;
  void *page_;
};

class Array2D {
  using FillPage = void (*)(void *page, const void *value);

 public:
  Array2D();
  ~Array2D();

  bool create(Storage *storage, uint32_t storage_node_id,
              uint64_t value_size, uint64_t page_size,
              uint64_t table_size,
              const void *default_value = nullptr,
              FillPage fill_page = nullptr);
  bool open(Storage *storage, uint32_t storage_node_id,
            uint64_t value_size, uint64_t page_size,
            uint64_t table_size, FillPage fill_page);

  static bool unlink(Storage *storage, uint32_t storage_node_id,
                     uint64_t value_size, uint64_t page_size,
                     uint64_t table_size);

  uint32_t storage_node_id() const {
    return storage_node_id_;
  }

  template <typename T, uint64_t TABLE_SIZE>
  T *get_page(uint64_t page_id) {
    if (!table_cache_[page_id]) {
      if (!initialize_page(page_id)) {
        return nullptr;
      }
    }
    return static_cast<T *>(table_cache_[page_id]);
  }

 private:
  Storage *storage_;
  uint32_t storage_node_id_;
  ArrayHeader *header_;
  void *default_value_;
  FillPage fill_page_;
  uint32_t *table_;
  std::unique_ptr<void *[]> table_cache_;
  Mutex mutex_;

  bool initialize_page(uint64_t page_id);
};

class Array3D {
  using FillPage = void (*)(void *page, const void *value);

 public:
  Array3D();
  ~Array3D();

  bool create(Storage *storage, uint32_t storage_node_id,
               uint64_t value_size, uint64_t page_size,
               uint64_t table_size, uint64_t secondary_table_size,
               const void *default_value = nullptr,
               FillPage fill_page = nullptr);

  bool open(Storage *storage, uint32_t storage_node_id,
            uint64_t value_size, uint64_t page_size,
            uint64_t table_size, uint64_t secondary_table_size,
            FillPage fill_page);

  static bool unlink(Storage *storage, uint32_t storage_node_id,
                     uint64_t value_size, uint64_t page_size,
                     uint64_t table_size, uint64_t secondary_table_size);

  uint32_t storage_node_id() const {
    return storage_node_id_;
  }

  template <typename T, uint64_t TABLE_SIZE, uint64_t SECONDARY_TABLE_SIZE>
  T *get_page(uint64_t page_id) {
    const uint64_t table_id = page_id / TABLE_SIZE;
    page_id %= TABLE_SIZE;
    if (!table_caches_[table_id] || !table_caches_[table_id][page_id]) {
      if (!initialize_page(table_id, page_id)) {
        return nullptr;
      }
    }
    return static_cast<T *>(table_caches_[table_id][page_id]);
  }

 private:
  Storage *storage_;
  uint32_t storage_node_id_;
  ArrayHeader *header_;
  void *default_value_;
  FillPage fill_page_;
  uint32_t *secondary_table_;
  std::unique_ptr<std::unique_ptr<void *[]>[]> table_caches_;
  Mutex page_mutex_;
  Mutex table_mutex_;
  Mutex secondary_table_mutex_;

  bool initialize_page(uint64_t table_id, uint64_t page_id);
  bool initialize_table(uint64_t table_id);
  bool initialize_secondary_table();
};

template <typename T,
          uint64_t PAGE_SIZE,
          uint64_t TABLE_SIZE,
          uint64_t SECONDARY_TABLE_SIZE>
class ArrayImpl;

// 1D array.
template <typename T, uint64_t PAGE_SIZE>
class ArrayImpl<T, PAGE_SIZE, 1, 1> {
  static_assert(PAGE_SIZE > 0, "PAGE_SIZE <= 0");

 public:
  using Value = typename Traits<T>::Type;
  using ValueArg = typename Traits<T>::ArgumentType;

  ArrayImpl() : impl_() {}
  ~ArrayImpl() {}

  // Return true iff the array is valid.
  explicit operator bool() const {
    return static_cast<bool>(impl_);
  }

  // Create an array.
  bool create(Storage *storage, uint32_t storage_node_id) {
    return impl_.create(storage, storage_node_id, sizeof(Value), PAGE_SIZE);
  }
  // Create an array with the default value.
  bool create(Storage *storage, uint32_t storage_node_id,
              ValueArg default_value) {
    return impl_.create(storage, storage_node_id, sizeof(Value), PAGE_SIZE,
                        &default_value, fill_page);
  }
  // Open an array.
  bool open(Storage *storage, uint32_t storage_node_id) {
    return impl_.open(storage, storage_node_id, sizeof(Value), PAGE_SIZE);
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

  // Get a value and return true.
  // The value is assigned to "*value" iff "value" != nullptr.
  bool get(uint64_t value_id, Value *value) {
    const Value * const page = get_page(value_id / PAGE_SIZE);
    if (value) {
      *value = page[value_id % PAGE_SIZE];
    }
    return true;
  }

  // Set a value and return true.
  bool set(uint64_t value_id, ValueArg value) {
    Value * const page = get_page(value_id / PAGE_SIZE);
    page[value_id % PAGE_SIZE] = value;
    return true;
  }

  // Get a value and return its address.
  Value *get_pointer(uint64_t value_id) {
    Value * const page = get_page(value_id / PAGE_SIZE);
    return &page[value_id % PAGE_SIZE];
  }

  // Get a page and return its starting address.
  Value *get_page(uint64_t) {
    return impl_.get_page<Value>();
  }

 private:
  Array1D impl_;

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
class ArrayImpl<T, PAGE_SIZE, TABLE_SIZE, 1> {
  static_assert((PAGE_SIZE > 0) && ((PAGE_SIZE & (PAGE_SIZE - 1)) == 0),
                "PAGE_SIZE must be a power of two");
  static_assert(TABLE_SIZE > 0, "TABLE_SIZE <= 0");

 public:
  using Value = typename Traits<T>::Type;
  using ValueArg = typename Traits<T>::ArgumentType;

  ArrayImpl() : impl_() {}
  ~ArrayImpl() {}

  // Return true iff the array is valid.
  explicit operator bool() const {
    return static_cast<bool>(impl_);
  }

  // Create an array.
  bool create(Storage *storage, uint32_t storage_node_id) {
    return impl_.create(storage, storage_node_id, sizeof(Value), PAGE_SIZE,
                        TABLE_SIZE);
  }
  // Create an array with the default value.
  bool create(Storage *storage, uint32_t storage_node_id,
              ValueArg default_value) {
    return impl_.create(storage, storage_node_id, sizeof(Value), PAGE_SIZE,
                        TABLE_SIZE, &default_value, fill_page);
  }
  // Open an array.
  bool open(Storage *storage, uint32_t storage_node_id) {
    return impl_.open(storage, storage_node_id, sizeof(Value), PAGE_SIZE,
                      TABLE_SIZE, fill_page);
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

  // Get a value and return true on success.
  // The value is assigned to "*value" iff "value" != nullptr.
  bool get(uint64_t value_id, Value *value) {
    const Value * const page = get_page(value_id / PAGE_SIZE);
    if (!page) {
      return false;
    }
    if (value) {
      *value = page[value_id % PAGE_SIZE];
    }
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

  // Get a value and return its address on success.
  Value *get_pointer(uint64_t value_id) {
    Value * const page = get_page(value_id / PAGE_SIZE);
    if (!page) {
      return nullptr;
    }
    return &page[value_id % PAGE_SIZE];
  }

  // Get a page and return its starting address on success.
  Value *get_page(uint64_t page_id) {
    return impl_.get_page<Value, TABLE_SIZE>(page_id);
  }

 private:
  Array2D impl_;

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
class ArrayImpl {
  static_assert((PAGE_SIZE > 0) && ((PAGE_SIZE & (PAGE_SIZE - 1)) == 0),
                "PAGE_SIZE must be a power of two");
  static_assert((TABLE_SIZE > 0) && ((TABLE_SIZE & (TABLE_SIZE - 1)) == 0),
                "TABLE_SIZE must be a power of two");
  static_assert(SECONDARY_TABLE_SIZE > 0, "SECONDARY_TABLE_SIZE <= 0");

 public:
  using Value = typename Traits<T>::Type;
  using ValueArg = typename Traits<T>::ArgumentType;

  ArrayImpl() : impl_() {}
  ~ArrayImpl() {}

  // Return true iff the array is valid.
  explicit operator bool() const {
    return static_cast<bool>(impl_);
  }

  // Create an array.
  bool create(Storage *storage, uint32_t storage_node_id) {
    return impl_.create(storage, storage_node_id, sizeof(Value), PAGE_SIZE,
                        TABLE_SIZE, SECONDARY_TABLE_SIZE);
  }
  // Create an array with the default value.
  bool create(Storage *storage, uint32_t storage_node_id,
              ValueArg default_value) {
    return impl_.create(storage, storage_node_id, sizeof(Value), PAGE_SIZE,
                        TABLE_SIZE, SECONDARY_TABLE_SIZE, &default_value,
                        fill_page);
  }
  // Open an array.
  bool open(Storage *storage, uint32_t storage_node_id) {
    return impl_.open(storage, storage_node_id, sizeof(Value), PAGE_SIZE,
                      TABLE_SIZE, SECONDARY_TABLE_SIZE, fill_page);
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

  // Get a value and return true on success.
  // The value is assigned to "*value" iff "value" != nullptr.
  bool get(uint64_t value_id, Value *value) {
    const Value * const page = get_page(value_id / PAGE_SIZE);
    if (!page) {
      return false;
    }
    if (value) {
      *value = page[value_id % PAGE_SIZE];
    }
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

  // Get a value and return its address on success.
  Value *get_pointer(uint64_t value_id) {
    Value * const page = get_page(value_id / PAGE_SIZE);
    if (!page) {
      return nullptr;
    }
    return &page[value_id % PAGE_SIZE];
  }

  // Get a page and return its starting address on success.
  Value *get_page(uint64_t page_id) {
    return impl_.get_page<Value, TABLE_SIZE, SECONDARY_TABLE_SIZE>(page_id);
  }

 private:
  Array3D impl_;

  // This function is used to fill a new page with the default value.
  static void fill_page(void *page, const void *value) {
    Value *values = static_cast<Value *>(page);
    for (uint64_t i = 0; i < PAGE_SIZE; ++i) {
      std::memcpy(&values[i], value, sizeof(Value));
    }
  }
};

}  // namespace grnxx

#endif  // GRNXX_ARRAY_IMPL_HPP
