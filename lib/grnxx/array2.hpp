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
#ifndef GRNXX_ARRAY2_HPP
#define GRNXX_ARRAY2_HPP

#include "grnxx/features.hpp"

#include <memory>
#include <new>

#include "grnxx/array2_impl.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Storage;

namespace alpha {

struct ArrayErrorHandler {
  static void throw_memory_error();
};

template <typename T, uint64_t PAGE_SIZE = 0, uint64_t TABLE_SIZE = 0>
class Array {
  // Test template parameters.
  static_assert((PAGE_SIZE != 0) || (TABLE_SIZE == 0),
                "TABLE_SIZE must be zero if PAGE_SIZE is zero");
  static_assert((PAGE_SIZE & (PAGE_SIZE - 1)) == 0,
                "PAGE_SIZE must be zero or a power of two");
  static_assert((TABLE_SIZE & (TABLE_SIZE - 1)) == 0,
                "TABLE_SIZE must be zero or a power of two");

  using Impl = ArrayImpl<T, PAGE_SIZE, TABLE_SIZE>;

 public:
  using Value = typename Impl::Value;
  using ValueArg = typename Impl::ValueArg;

  ~Array() {}

  // Create an array.
  static Array *create(Storage *storage, uint32_t storage_node_id,
                       uint64_t size) {
    std::unique_ptr<Array> array(create_instance());
    array->impl_.create(storage, storage_node_id, size);
    return array.release();
  }
  // Create an array with default value.
  static Array *create(Storage *storage, uint32_t storage_node_id,
                       uint64_t size, ValueArg default_value) {
    std::unique_ptr<Array> array(create_instance());
    array->impl_.create(storage, storage_node_id, size, default_value);
    return array.release();
  }

  // Open an array.
  static Array *open(Storage *storage, uint32_t storage_node_id) {
    std::unique_ptr<Array> array(create_instance());
    array->impl_.open(storage, storage_node_id);
    return array.release();
  }

  // Unlink an array.
  static void unlink(Storage *storage, uint32_t storage_node_id) {
    Impl::unlink(storage, storage_node_id);
  }

  // Return the storage node ID.
  uint32_t storage_node_id() const {
    return impl_.storage_node_id();
  }
  // Return the number of values.
  uint64_t size() const {
    return impl_.size();
  }

  // Get a reference to a value.
  Value &operator[](uint64_t value_id) {
    return *impl_.get_value(value_id);
  }
  // Get a value.
  Value get(uint64_t value_id) {
    return *impl_.get_value(value_id);
  }
  // Set a value.
  void set(uint64_t value_id, ValueArg value) {
    *impl_.get_value(value_id) = value;
  }

 private:
  Impl impl_;

  Array() : impl_() {}

  // Create an instance or throw an exception on failure.
  static Array *create_instance() {
    Array * const array = new (std::nothrow) Array;
    if (!array) {
      ArrayErrorHandler::throw_memory_error();
    }
    return array;
  }
};

}  // namespace alpha
}  // namespace grnxx

#endif  // GRNXX_ARRAY2_HPP
