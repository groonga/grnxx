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
#ifndef GRNXX_DB_ARRAY_HPP
#define GRNXX_DB_ARRAY_HPP

#include "../exception.hpp"
#include "../logger.hpp"
#include "../io/pool.hpp"

namespace grnxx {
namespace db {

const uint64_t ARRAY_HEADER_SIZE = 64;

class ArrayHeader {
 public:
  void initialize(uint64_t value_size, uint64_t array_size);

  uint64_t value_size() const {
    return value_size_;
  }
  uint64_t array_size() const {
    return array_size_;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  uint64_t value_size_;
  uint64_t array_size_;
  uint8_t reserved_[ARRAY_HEADER_SIZE - 16];
};

static_assert(sizeof(ArrayHeader) == ARRAY_HEADER_SIZE,
              "sizeof(ArrayHeader) is wrong");

class ArrayImpl {
 public:
  ArrayImpl();
  ArrayImpl(io::Pool *pool, uint64_t value_size, uint64_t array_size);
  ArrayImpl(io::Pool *pool, uint32_t block_id);
  ~ArrayImpl();

  ArrayImpl(ArrayImpl &&array);
  ArrayImpl &operator=(ArrayImpl &&array);

  void create(io::Pool *pool, uint64_t value_size, uint64_t array_size);
  void open(io::Pool *pool, uint32_t block_id);

  uint32_t block_id() const {
    return block_id_;
  }
  uint64_t value_size() const {
    return header_->value_size();
  }
  uint64_t array_size() const {
    return header_->array_size();
  }
  void *address() const {
    return address_;
  }

  void swap(ArrayImpl &array);

  StringBuilder &write_to(StringBuilder &builder) const;

  static void unlink(io::Pool *pool, uint32_t block_id);

 private:
  io::Pool pool_;
  uint32_t block_id_;
  ArrayHeader *header_;
  void *address_;

  ArrayImpl(const ArrayImpl &);
  ArrayImpl &operator=(const ArrayImpl &);
};

inline void swap(ArrayImpl &lhs, ArrayImpl &rhs) {
  lhs.swap(rhs);
}

template <typename T>
class Array {
 public:
  typedef T Value;

  Array() : impl_() {}
  ~Array() {}

  Array(Array &&array) : impl_(std::move(array.impl_)) {}
  Array &operator=(Array &&array) {
    impl_ = std::move(array.impl_);
    return *this;
  }

  void create(io::Pool *pool, uint64_t size) {
    impl_.create(pool, sizeof(Value), size);
  }
  void open(io::Pool *pool, uint32_t block_id) {
    ArrayImpl new_impl;
    new_impl.open(pool, block_id);
    if (new_impl.value_size() != sizeof(Value)) {
      GRNXX_ERROR() << "invalid value size: expected = " << sizeof(Value)
                    << ", actual = " << new_impl.value_size();
      GRNXX_THROW();
    }
    impl_ = std::move(new_impl);
  }
  void close() {
    ArrayImpl().swap(impl_);
  }

  Value &operator[](uint64_t id) const {
    return address()[id];
  }

  uint32_t block_id() const {
    return impl_.block_id();
  }
  uint64_t size() const {
    return impl_.array_size();
  }
  Value *address() const {
    return static_cast<Value *>(impl_.address());
  }

  void swap(Array &array) {
    impl_.swap(array.impl_);
  }

  StringBuilder &write_to(StringBuilder &builder) const {
    return impl_.write_to(builder);
  }

  static void unlink(io::Pool *pool, uint32_t block_id) {
    Array array;
    array.open(pool, block_id);
    array.close();
    ArrayImpl::unlink(pool, block_id);
  }

 private:
  ArrayImpl impl_;

  Array(const Array &);
  Array &operator=(const Array &);
};

template <typename T>
inline void swap(Array<T> &lhs, Array<T> &rhs) {
  lhs.swap(rhs);
}

inline StringBuilder &operator<<(StringBuilder &builder,
                                 const ArrayHeader &header) {
  return header.write_to(builder);
}
inline StringBuilder &operator<<(StringBuilder &builder,
                                 const ArrayImpl &array) {
  return array.write_to(builder);
}
template <typename T>
inline StringBuilder &operator<<(StringBuilder &builder,
                                 const Array<T> &array) {
  return array.write_to(builder);
}

}  // namespace db
}  // namespace grnxx

#endif  // GRNXX_DB_ARRAY_HPP
