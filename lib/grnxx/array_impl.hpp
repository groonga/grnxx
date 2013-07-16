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

#include <memory>

#include "grnxx/mutex.hpp"
#include "grnxx/traits.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Storage;

struct ArrayHeader;

// Fill "page" with "value".
using ArrayFillPage = void (*)(void *page, uint64_t page_size,
                               const void *value);

class Array1D {
 public:
  Array1D();
  ~Array1D();

  // Create an array.
  void create(Storage *storage, uint32_t storage_node_id,
              uint64_t value_size, uint64_t page_size,
              uint64_t table_size, uint64_t size,
              const void *default_value, ArrayFillPage fill_page);
  // Open an array.
  void open(Storage *storage, uint32_t storage_node_id,
            uint64_t value_size, uint64_t page_size,
            uint64_t table_size, ArrayFillPage fill_page);

  // Unlink an array.
  static void unlink(Storage *storage, uint32_t storage_node_id,
                     uint64_t value_size, uint64_t page_size,
                     uint64_t table_size);

  // Return the storage node ID.
  uint32_t storage_node_id() const {
    return storage_node_id_;
  }
  // Return the number of values.
  uint64_t size() const {
    return size_;
  }

  // Return a reference to a value.
  template <typename T, uint64_t, uint64_t>
  T &get_value(uint64_t value_id) {
    return static_cast<T *>(page_)[value_id];
  }

 private:
  void *page_;
  uint64_t size_;
  uint32_t storage_node_id_;
};

class Array2D {
 public:
  Array2D();
  ~Array2D();

  // Create an array.
  void create(Storage *storage, uint32_t storage_node_id,
              uint64_t value_size, uint64_t page_size,
              uint64_t table_size, uint64_t size,
              const void *default_value, ArrayFillPage fill_page);
  // Open an array.
  void open(Storage *storage, uint32_t storage_node_id,
            uint64_t value_size, uint64_t page_size,
            uint64_t table_size, ArrayFillPage fill_page);

  // Unlink an array.
  static void unlink(Storage *storage, uint32_t storage_node_id,
                     uint64_t value_size, uint64_t page_size,
                     uint64_t table_size);

  // Return the storage node ID.
  uint32_t storage_node_id() const {
    return storage_node_id_;
  }
  // Return the number of values.
  uint64_t size() const {
    return size_;
  }

  // Return a reference to a value.
  template <typename T, uint64_t PAGE_SIZE, uint64_t TABLE_SIZE>
  T &get_value(uint64_t value_id) {
    const uint64_t page_id = value_id / PAGE_SIZE;
    if (pages_[page_id] == invalid_page()) {
      reserve_page(page_id);
    }
    return static_cast<T *>(pages_[page_id])[value_id];
  }

 private:
  std::unique_ptr<void *[]> pages_;
  uint64_t size_;
  Storage *storage_;
  uint32_t storage_node_id_;
  ArrayHeader *header_;
  ArrayFillPage fill_page_;
  uint32_t *table_;
  Mutex mutex_;

  // Initialize "pages_".
  void reserve_pages();
  // Open or create a page.
  void reserve_page(uint64_t page_id);

  // Return a pointer to an invalid page.
  static void *invalid_page() {
    return static_cast<char *>(nullptr) - 1;
  }
};

class Array3D {
  using ArrayPageFiller = void (*)(void *page, const void *value);

 public:
  Array3D();
  ~Array3D();

  // Create an array.
  void create(Storage *storage, uint32_t storage_node_id,
              uint64_t value_size, uint64_t page_size,
              uint64_t table_size, uint64_t size,
              const void *default_value, ArrayFillPage fill_page);
  // Open an array.
  void open(Storage *storage, uint32_t storage_node_id,
            uint64_t value_size, uint64_t page_size,
            uint64_t table_size, ArrayFillPage fill_page);

  // Unlink an array.
  static void unlink(Storage *storage, uint32_t storage_node_id,
                     uint64_t value_size, uint64_t page_size,
                     uint64_t table_size);

  // Return the storage node ID.
  uint32_t storage_node_id() const {
    return storage_node_id_;
  }
  // Return the number of values.
  uint64_t size() const {
    return size_;
  }

  // Return a reference to a value.
  template <typename T, uint64_t PAGE_SIZE, uint64_t TABLE_SIZE>
  T &get_value(uint64_t value_id) {
    const uint64_t table_id = value_id / (PAGE_SIZE * TABLE_SIZE);
    const uint64_t page_id = value_id / PAGE_SIZE;
    if (tables_[table_id][page_id] == invalid_page()) {
      reserve_page(page_id);
    }
    return static_cast<T *>(tables_[table_id][page_id])[value_id];
  }

  // Return a pointer to an invalid page.
  static void *invalid_page() {
    return static_cast<char *>(nullptr) + 1;
  }

 private:
  std::unique_ptr<void **[]> tables_;
  uint64_t size_;
  Storage *storage_;
  uint32_t storage_node_id_;
  ArrayHeader *header_;
  ArrayFillPage fill_page_;
  uint32_t *secondary_table_;
  void **dummy_table_;
  Mutex page_mutex_;
  Mutex table_mutex_;

  // Initialize "tables_".
  void reserve_tables();
  // Open or create a page.
  void reserve_page(uint64_t page_id);
  // Open or create a table.
  void reserve_table(uint64_t table_id);
};

template <uint64_t PAGE_SIZE, uint64_t TABLE_SIZE>
struct ArrayImplSelector;

// Use Array1D.
template <>
struct ArrayImplSelector<0, 0> {
  using Type = Array1D;
};

// Use Array2D.
template <uint64_t PAGE_SIZE>
struct ArrayImplSelector<PAGE_SIZE, 0> {
  using Type = Array2D;
};

// Use Array3D.
template <uint64_t PAGE_SIZE, uint64_t TABLE_SIZE>
struct ArrayImplSelector {
  using Type = Array3D;
};

template <typename T>
struct ArrayPageFiller {
  // Fill "page" with "value".
  // This function is used to initialize a page.
  static void fill_page(void *page, uint64_t page_size, const void *value) {
    for (uint64_t i = 0; i < page_size; ++i) {
      static_cast<T *>(page)[i] = *static_cast<const T *>(value);
    }
  }
};

template <typename T, uint64_t PAGE_SIZE, uint64_t TABLE_SIZE>
class ArrayImpl {
  // Test template parameters.
  static_assert((PAGE_SIZE != 0) || (TABLE_SIZE == 0),
                "TABLE_SIZE must be zero if PAGE_SIZE is zero");
  static_assert((PAGE_SIZE & (PAGE_SIZE - 1)) == 0,
                "PAGE_SIZE must be zero or a power of two");
  static_assert((TABLE_SIZE & (TABLE_SIZE - 1)) == 0,
                "TABLE_SIZE must be zero or a power of two");

  using Impl = typename ArrayImplSelector<PAGE_SIZE, TABLE_SIZE>::Type;

 public:
  using Value    = typename Traits<T>::Type;
  using ValueArg = typename Traits<T>::ArgumentType;
  using ValueRef = Value &;
  using Unit     = Value;

  ArrayImpl() : impl_() {}
  ~ArrayImpl() {}

  // Create an array.
  void create(Storage *storage, uint32_t storage_node_id, uint64_t size) {
    impl_.create(storage, storage_node_id,
                 sizeof(Value), PAGE_SIZE, TABLE_SIZE, size,
                 nullptr, ArrayPageFiller<Value>::fill_page);
  }
  // Create an array with the default value.
  void create(Storage *storage, uint32_t storage_node_id, uint64_t size,
              ValueArg default_value) {
    impl_.create(storage, storage_node_id,
                 sizeof(Value), PAGE_SIZE, TABLE_SIZE, size,
                 &default_value, ArrayPageFiller<Value>::fill_page);
  }
  // Open an array.
  void open(Storage *storage, uint32_t storage_node_id) {
    impl_.open(storage, storage_node_id,
               sizeof(Value), PAGE_SIZE, TABLE_SIZE,
               ArrayPageFiller<Value>::fill_page);
  }

  // Unlink an array.
  static void unlink(Storage *storage, uint32_t storage_node_id) {
    Impl::unlink(storage, storage_node_id,
                 sizeof(Value), PAGE_SIZE, TABLE_SIZE);
  }

  // Return the storage node ID.
  uint32_t storage_node_id() const {
    return impl_.storage_node_id();
  }
  // Return the number of values.
  uint64_t size() const {
    return impl_.size();
  }

  // Return a reference to a value.
  ValueRef get_value(uint64_t value_id) {
    return impl_. template get_value<Value, PAGE_SIZE, TABLE_SIZE>(value_id);
  }
  // Return a reference to a unit.
  Unit &get_unit(uint64_t unit_id) {
    return get_value(unit_id);
  }

 private:
  Impl impl_;
};

// A reference to a bit.
class ArrayBitRef {
 public:
  using Unit = uint64_t;

  // Create a reference to a bit.
  ArrayBitRef(Unit &unit, Unit mask) : unit_(unit), mask_(mask) {}

  // Get a bit.
  operator bool() const {
    return (unit_ & mask_) != 0;
  }

  // Set a bit.
  ArrayBitRef &operator=(bool value) {
    if (value) {
      unit_ |= mask_;
    } else {
      unit_ &= ~mask_;
    }
    return *this;
  }

  // Copy a bit.
  ArrayBitRef &operator=(const ArrayBitRef &rhs) {
    return *this = bool(rhs);
  }

  // Compare bits.
  bool operator==(bool rhs) const {
    return bool(*this) == rhs;
  }
  bool operator!=(bool rhs) const {
    return bool(*this) != rhs;
  }

 private:
  Unit &unit_;
  Unit mask_;
};

// An array of bits.
template <uint64_t PAGE_SIZE_IN_BITS, uint64_t TABLE_SIZE>
class ArrayImpl<bool, PAGE_SIZE_IN_BITS, TABLE_SIZE> {
  static constexpr uint64_t UNIT_SIZE = sizeof(ArrayBitRef::Unit) * 8;
  static constexpr uint64_t PAGE_SIZE = PAGE_SIZE_IN_BITS / UNIT_SIZE;

  // Test template parameters.
  static_assert((PAGE_SIZE_IN_BITS % UNIT_SIZE) == 0,
                "(PAGE_SIZE_IN_BITS % UNIT_SIZE) != 0");
  static_assert((PAGE_SIZE != 0) || (TABLE_SIZE == 0),
                "TABLE_SIZE must be zero if PAGE_SIZE is zero");
  static_assert((PAGE_SIZE & (PAGE_SIZE - 1)) == 0,
                "PAGE_SIZE must be zero or a power of two");
  static_assert((TABLE_SIZE & (TABLE_SIZE - 1)) == 0,
                "TABLE_SIZE must be zero or a power of two");

  using Impl = typename ArrayImplSelector<PAGE_SIZE, TABLE_SIZE>::Type;

 public:
  using Value    = typename Traits<bool>::Type;
  using ValueArg = typename Traits<bool>::ArgumentType;
  using ValueRef = ArrayBitRef;
  using Unit     = ArrayBitRef::Unit;

  ArrayImpl() : impl_() {}
  ~ArrayImpl() {}

  // Create an array.
  void create(Storage *storage, uint32_t storage_node_id, uint64_t size) {
    impl_.create(storage, storage_node_id,
                 sizeof(Unit), PAGE_SIZE, TABLE_SIZE, size / UNIT_SIZE,
                 nullptr, ArrayPageFiller<Unit>::fill_page);
  }
  // Create an array with the default value.
  void create(Storage *storage, uint32_t storage_node_id, uint64_t size,
              ValueArg default_value) {
    const Unit default_unit = default_value ? ~(Unit)0 : (Unit)0;
    impl_.create(storage, storage_node_id,
                 sizeof(Unit), PAGE_SIZE, TABLE_SIZE, size / UNIT_SIZE,
                 &default_unit, ArrayPageFiller<Unit>::fill_page);
  }
  // Open an array.
  void open(Storage *storage, uint32_t storage_node_id) {
    impl_.open(storage, storage_node_id,
               sizeof(Unit), PAGE_SIZE, TABLE_SIZE,
               ArrayPageFiller<Unit>::fill_page);
  }

  // Unlink an array.
  static void unlink(Storage *storage, uint32_t storage_node_id) {
    Impl::unlink(storage, storage_node_id,
                 sizeof(Unit), PAGE_SIZE, TABLE_SIZE);
  }

  // Return the storage node ID.
  uint32_t storage_node_id() const {
    return impl_.storage_node_id();
  }
  // Return the number of values.
  uint64_t size() const {
    return impl_.size() * UNIT_SIZE;
  }

  // Return a reference to a value.
  ValueRef get_value(uint64_t value_id) {
    return ValueRef(get_unit(value_id / UNIT_SIZE),
                    Unit(1) << (value_id % UNIT_SIZE));
  }
  // Return a reference to a unit.
  Unit &get_unit(uint64_t unit_id) {
    return impl_. template get_value<Unit, PAGE_SIZE, TABLE_SIZE>(unit_id);
  }

 private:
  Impl impl_;
};

}  // namespace grnxx

#endif  // GRNXX_ARRAY_IMPL_HPP
