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
#ifndef GRNXX_BIT_ARRAY_HPP
#define GRNXX_BIT_ARRAY_HPP

#include "grnxx/features.hpp"

#include "grnxx/array.hpp"
#include "grnxx/traits.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Storage;

// Bit array.
template <uint64_t PAGE_SIZE_IN_BITS,
          uint64_t TABLE_SIZE,
          uint64_t SECONDARY_TABLE_SIZE>
class BitArray {
 public:
  // Internal type to store bits.
  using Unit = uint64_t;

 private:
  static constexpr uint64_t UNIT_SIZE = sizeof(Unit) * 8;
  static constexpr uint64_t PAGE_SIZE = PAGE_SIZE_IN_BITS / UNIT_SIZE;

  static_assert((PAGE_SIZE_IN_BITS % UNIT_SIZE) == 0,
                "(PAGE_SIZE_IN_BITS % UNIT_SIZE) != 0");
  using ArrayImpl = Array<Unit, PAGE_SIZE, TABLE_SIZE, SECONDARY_TABLE_SIZE>;

 public:
  using Value = typename Traits<bool>::Type;
  using ValueArg = typename Traits<bool>::ArgumentType;

  BitArray() : impl_() {}
  ~BitArray() {}

  // Return true iff the array is valid.
  explicit operator bool() const {
    return static_cast<bool>(impl_);
  }

  // Create an array.
  bool create(Storage *storage, uint32_t storage_node_id) {
    return impl_.create(storage, storage_node_id);
  }

  // Create an array with the default value.
  bool create(Storage *storage, uint32_t storage_node_id,
              ValueArg default_value) {
    return impl_.create(storage, storage_node_id,
                        default_value ? ~Unit(0) : Unit(0));
  }

  // Open an array.
  bool open(Storage *storage, uint32_t storage_node_id) {
    return impl_.open(storage, storage_node_id);
  }

  // Unlink an array.
  static bool unlink(Storage *storage, uint32_t storage_node_id) {
    return ArrayImpl::unlink(storage, storage_node_id);
  }

  // Return the number of values in each unit.
  static constexpr uint64_t unit_size() {
    return UNIT_SIZE;
  }
  // Return the number of values in each page.
  static constexpr uint64_t page_size() {
    return PAGE_SIZE_IN_BITS;
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

  // Get a value.
  // This function throws an exception on failure.
  Value operator[](uint64_t value_id) {
    return (impl_[value_id / UNIT_SIZE] &
            (Unit(1) << (value_id % UNIT_SIZE))) != 0;
  }

  // Get a value and return true on success.
  // The value is assigned to "*value" iff "value" != nullptr.
  bool get(uint64_t value_id, Value *value) {
    const uint64_t unit_id = value_id / UNIT_SIZE;
    const Unit * const page = get_page(unit_id / PAGE_SIZE);
    if (!page) {
      return false;
    }
    if (value) {
      *value = (page[unit_id % PAGE_SIZE] &
                (Unit(1) << (value_id % UNIT_SIZE))) != 0;
    }
    return true;
  }

  // Set a value and return true on success.
  // Note that if bits in the same byte are set at the same time, the result is
  // undefined.
  bool set(uint64_t value_id, ValueArg value) {
    const uint64_t unit_id = value_id / UNIT_SIZE;
    Unit * const page = get_page(unit_id / PAGE_SIZE);
    if (!page) {
      return false;
    }
    if (value) {
      page[unit_id % PAGE_SIZE] |= Unit(1) << (value_id % UNIT_SIZE);
    } else {
      page[unit_id % PAGE_SIZE] &= ~(Unit(1) << (value_id % UNIT_SIZE));
    }
    return true;
  }

  // Get a unit and return its address on success.
  Unit *get_unit(uint64_t unit_id) {
    return impl_.get_value(unit_id);
  }

  // Get a page and return its starting address on success.
  Unit *get_page(uint64_t page_id) {
    return impl_.get_page(page_id);
  }

 private:
  ArrayImpl impl_;
};

}  // namespace grnxx

#endif  // GRNXX_BIT_ARRAY_HPP
