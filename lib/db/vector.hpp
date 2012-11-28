/*
  Copyright (C) 2012  Brazil, Inc.

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
#ifndef GRNXX_DB_VECTOR_HPP
#define GRNXX_DB_VECTOR_HPP

#include "vector_base.hpp"

namespace grnxx {
namespace db {

const uint64_t VECTOR_PAGE_SIZE_MIN     = uint64_t(1) << 0;
const uint64_t VECTOR_PAGE_SIZE_MAX     = uint64_t(1) << 20;
const uint64_t VECTOR_PAGE_SIZE_DEFAULT = uint64_t(1) << 16;

const uint64_t VECTOR_TABLE_SIZE_MIN     = uint64_t(1) << 10;
const uint64_t VECTOR_TABLE_SIZE_MAX     = uint64_t(1) << 20;
const uint64_t VECTOR_TABLE_SIZE_DEFAULT = uint64_t(1) << 12;

const uint64_t VECTOR_SECONDARY_TABLE_SIZE_MIN     = uint64_t(1) << 10;
const uint64_t VECTOR_SECONDARY_TABLE_SIZE_MAX     = uint64_t(1) << 20;
const uint64_t VECTOR_SECONDARY_TABLE_SIZE_DEFAULT = uint64_t(1) << 12;

template <typename T,
          uint64_t PAGE_SIZE = VECTOR_PAGE_SIZE_DEFAULT,
          uint64_t TABLE_SIZE = VECTOR_TABLE_SIZE_DEFAULT,
          uint64_t SECONDARY_TABLE_SIZE = VECTOR_SECONDARY_TABLE_SIZE_DEFAULT>
class Vector {
 public:
  typedef T Value;

  static_assert(PAGE_SIZE >= VECTOR_PAGE_SIZE_MIN, "too small PAGE_SIZE");
  static_assert(PAGE_SIZE <= VECTOR_PAGE_SIZE_MAX, "too large PAGE_SIZE");

  static_assert(TABLE_SIZE >= VECTOR_TABLE_SIZE_MIN, "too small TABLE_SIZE");
  static_assert(TABLE_SIZE <= VECTOR_TABLE_SIZE_MAX, "too large TABLE_SIZE");

  static_assert(SECONDARY_TABLE_SIZE >= VECTOR_SECONDARY_TABLE_SIZE_MIN,
                "too small SECONDARY_TABLE_SIZE");
  static_assert(SECONDARY_TABLE_SIZE <= VECTOR_SECONDARY_TABLE_SIZE_MAX,
                "too large SECONDARY_TABLE_SIZE");

  static_assert((PAGE_SIZE & (PAGE_SIZE - 1)) == 0,
                "PAGE_SIZE must be a power of two");
  static_assert((TABLE_SIZE & (TABLE_SIZE - 1)) == 0,
                "TABLE_SIZE must be a power of two");
  static_assert((SECONDARY_TABLE_SIZE & (SECONDARY_TABLE_SIZE - 1)) == 0,
                "SECONDARY_TABLE_SIZE must be a power of two");

  Vector() : base_() {}
  ~Vector() {}

  Vector(Vector &&vector) : base_(std::move(vector.base_)) {}
  Vector &operator=(Vector &&vector) {
    base_ = std::move(vector.base_);
    return *this;
  }

  bool is_open() const {
    return base_.is_open();
  }

  void create(io::Pool *pool) {
    base_.create(pool, sizeof(Value),
                 PAGE_SIZE, TABLE_SIZE, SECONDARY_TABLE_SIZE,
                 nullptr, fill_page);
  }
  void create(io::Pool *pool, const Value &default_value) {
    base_.create(pool, sizeof(Value),
                 PAGE_SIZE, TABLE_SIZE, SECONDARY_TABLE_SIZE,
                 &default_value, fill_page);
  }
  void open(io::Pool *pool, uint32_t block_id) {
    base_.open(pool, block_id, sizeof(Value),
               PAGE_SIZE, TABLE_SIZE, SECONDARY_TABLE_SIZE, fill_page);
  }
  void close() {
    if (!is_open()) {
      GRNXX_ERROR() << "failed to close vector";
      GRNXX_THROW();
    }
    base_ = VectorBase();
  }

  Value *get_value_address(uint64_t id) {
    return base_.get_value_address<Value, PAGE_SIZE, TABLE_SIZE,
                                   SECONDARY_TABLE_SIZE>(id);
  }
  Value &get_value(uint64_t id) {
    return *get_value_address(id);
  }
  Value &operator[](uint64_t id) {
    return get_value(id);
  }

  uint32_t block_id() const {
    return base_.block_id();
  }

  void swap(Vector &vector) {
    base_.swap(vector.base_);
  }

  StringBuilder &write_to(StringBuilder &builder) const {
    return base_.write_to(builder);
  }

  static uint64_t value_size() {
    return sizeof(Value);
  }
  static uint64_t page_size() {
    return PAGE_SIZE;
  }
  static uint64_t table_size() {
    return TABLE_SIZE;
  }
  static uint64_t secondary_table_size() {
    return SECONDARY_TABLE_SIZE;
  }
  static uint64_t id_max() {
    return (PAGE_SIZE * TABLE_SIZE * SECONDARY_TABLE_SIZE) - 1;
  }

  static void unlink(io::Pool *pool, uint32_t block_id) {
    VectorBase::unlink(pool, block_id, sizeof(Value),
                       PAGE_SIZE, TABLE_SIZE, SECONDARY_TABLE_SIZE);
  }

 private:
  VectorBase base_;

  static void fill_page(void *page_address, const void *value) {
    Value *values = static_cast<Value *>(page_address);
    for (uint64_t i = 0; i < PAGE_SIZE; ++i) {
      std::memcpy(&values[i], value, sizeof(Value));
    }
  }
};

template <typename T>
inline void swap(Vector<T> &lhs, Vector<T> &rhs) {
  lhs.swap(rhs);
}

template <typename T>
inline StringBuilder &operator<<(StringBuilder &builder,
                                 const Vector<T> &vector) {
  return vector.write_to(builder);
}

}  // namespace db
}  // namespace grnxx

#endif  // GRNXX_DB_VECTOR_HPP
