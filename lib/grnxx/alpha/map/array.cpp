/*
  Copyright (C) 2013  Brazil, Inc.

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
#include "array.hpp"

#include <cmath>

namespace grnxx {
namespace alpha {
namespace map {
namespace {

template <typename T>
bool equal_to(T x, T y) {
  return x == y;
}

template <>
bool equal_to(double x, double y) {
  return (std::isnan(x) && std::isnan(y)) || (x == y);
}

template <typename T>
T normalize(T x) {
  return x;
}

template <>
double normalize(double x) {
  return std::isnan(x) ? std::numeric_limits<double>::quiet_NaN() : x;
}

}  // namespace

ArrayHeader::ArrayHeader()
  : map_type(MAP_ARRAY),
    bits_block_id(io::BLOCK_INVALID_ID),
    keys_block_id(io::BLOCK_INVALID_ID),
    max_key_id(-1) {}

template <typename T>
Array<T>::~Array() {}

template <typename T>
Array<T> *Array<T>::create(io::Pool pool, const MapOptions &) {
  std::unique_ptr<Array<T>> array(new (std::nothrow) Array<T>);
  array->pool_ = pool;
  array->block_info_ = pool.create_block(sizeof(ArrayHeader));
  array->header_ = static_cast<ArrayHeader *>(
      pool.get_block_address(*array->block_info_));
  *array->header_ = ArrayHeader();
  array->bits_.create(pool, 0);
  array->header_->bits_block_id = array->bits_.block_id();
  array->keys_.create(pool);
  array->header_->keys_block_id = array->keys_.block_id();
  return array.release();
}

template <typename T>
Array<T> *Array<T>::open(io::Pool pool, uint32_t block_id) {
  std::unique_ptr<Array<T>> array(new (std::nothrow) Array<T>);
  array->pool_ = pool;
  array->block_info_ = pool.get_block_info(block_id);
  array->header_ = static_cast<ArrayHeader *>(
      pool.get_block_address(*array->block_info_));
  array->bits_.open(pool, array->header_->bits_block_id);
  array->keys_.open(pool, array->header_->keys_block_id);
  return array.release();
}

template <typename T>
bool Array<T>::unlink(io::Pool pool, uint32_t block_id) {
  Array<T> *array = open(pool, block_id);
  pool.free_block(array->header_->bits_block_id);
  pool.free_block(array->header_->keys_block_id);
  pool.free_block(array->block_id());
  return false;
}

template <typename T>
uint32_t Array<T>::block_id() const {
  return block_info_->id();
}

template <typename T>
MapType Array<T>::type() const {
  return MAP_ARRAY;
}

template <typename T>
bool Array<T>::get(int64_t key_id, T *key) {
  if ((key_id < 0) || (key_id > header_->max_key_id)) {
    return false;
  }
  if (!get_bit(key_id)) {
    return false;
  }
  if (key) {
    *key = keys_[key_id];
  }
  return true;
}

template <typename T>
bool Array<T>::unset(int64_t key_id) {
  if ((key_id < 0) || (key_id > header_->max_key_id)) {
    return false;
  }
  if (!get_bit(key_id)) {
    return false;
  }
  set_bit(key_id, false);
  return true;
}

template <typename T>
bool Array<T>::reset(int64_t key_id, T dest_key) {
  if ((key_id < 0) || (key_id > header_->max_key_id)) {
    return false;
  }
  if (!get_bit(key_id)) {
    return false;
  }
  if (search(dest_key)) {
    return false;
  }
  keys_[key_id] = normalize(dest_key);
  return true;
}

template <typename T>
bool Array<T>::search(T key, int64_t *key_id) {
  for (int64_t i = 0; i <= header_->max_key_id; ++i) {
    if (get_bit(i)) {
      if (equal_to(key, keys_[i])) {
        if (key_id) {
          *key_id = i;
        }
        return true;
      }
    }
  }
  return false;
}

template <typename T>
bool Array<T>::insert(T key, int64_t *key_id) {
  int64_t key_id_candidate = -1;
  for (int64_t i = 0; i <= header_->max_key_id; ++i) {
    if (get_bit(i)) {
      if (equal_to(key, keys_[i])) {
        if (key_id) {
          *key_id = i;
        }
        return false;
      }
    } else if (key_id_candidate == -1) {
      // Use the youngest ID if there exist IDs associated with removed keys.
      key_id_candidate = i;
    }
  }
  if (key_id_candidate == -1) {
    key_id_candidate = ++header_->max_key_id;
  }
  keys_[key_id_candidate] = normalize(key);
  set_bit(key_id_candidate, true);
  if (key_id) {
    *key_id = key_id_candidate;
  }
  return true;
}

template <typename T>
bool Array<T>::remove(T key) {
  int64_t key_id;
  if (!search(key, &key_id)) {
    return false;
  }
  set_bit(key_id, false);
  return true;
}

template <typename T>
bool Array<T>::update(T src_key, T dest_key, int64_t *key_id) {
  int64_t src_key_id;
  if (!search(src_key, &src_key_id)) {
    return false;
  }
  if (search(dest_key)) {
    return false;
  }
  keys_[src_key_id] = normalize(dest_key);
  if (key_id) {
    *key_id = src_key_id;
  }
  return true;
}

template <typename T>
void Array<T>::truncate() {
  for (int64_t i = 0; i <= header_->max_key_id; ++i) {
    set_bit(i, false);
  }
  header_->max_key_id = -1;
}

template <typename T>
Array<T>::Array()
  : pool_(),
    block_info_(nullptr),
    header_(nullptr),
    bits_(),
    keys_() {}

template class Array<int8_t>;
template class Array<int16_t>;
template class Array<int32_t>;
template class Array<int64_t>;
template class Array<uint8_t>;
template class Array<uint16_t>;
template class Array<uint32_t>;
template class Array<uint64_t>;
template class Array<double>;

}  // namespace map
}  // namespace alpha
}  // namespace grnxx
