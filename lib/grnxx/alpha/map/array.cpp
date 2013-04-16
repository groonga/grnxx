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
#include "grnxx/alpha/map/array.hpp"

#include <cmath>
#include <string>

namespace grnxx {
namespace alpha {
namespace map {
namespace {

template <typename T, bool HAS_NAN = std::numeric_limits<T>::has_quiet_NaN>
struct Helper;

template <typename T>
struct Helper<T, true> {
  static bool equal_to(T x, T y) {
    return (std::isnan(x) && std::isnan(y)) || (x == y);
  }
  static T normalize(T x) {
    return std::isnan(x) ? std::numeric_limits<T>::quiet_NaN() : x;
  }
};

template <typename T>
struct Helper<T, false> {
  static bool equal_to(T x, T y) {
    return x == y;
  }
  static T normalize(T x) {
    return x;
  }
};

db::Blob slice_to_blob(Slice slice, std::string *buf) {
  buf->assign(reinterpret_cast<const char *>(slice.ptr()), slice.size());
  buf->resize(buf->size() + 7, ' ');
  return db::Blob(buf->data(), buf->size());
}

Slice blob_to_slice(const db::Blob &blob) {
  return Slice(blob.address(), blob.length() - 7);
}

}  // namespace

ArrayHeader::ArrayHeader()
  : map_type(MAP_ARRAY),
    bits_block_id(io::BLOCK_INVALID_ID),
    keys_block_id(io::BLOCK_INVALID_ID),
    max_key_id(-1),
    next_key_id(0),
    num_keys(0) {}

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
  std::unique_ptr<Array<T>> array(open(pool, block_id));
  array->bits_.unlink(pool, array->header_->bits_block_id);
  array->keys_.unlink(pool, array->header_->keys_block_id);
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
int64_t Array<T>::max_key_id() const {
  return header_->max_key_id;
}

template <typename T>
int64_t Array<T>::next_key_id() const {
  return header_->next_key_id;
}

template <typename T>
uint64_t Array<T>::num_keys() const {
  return header_->num_keys;
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
bool Array<T>::get_next(int64_t key_id, int64_t *next_key_id, T *next_key) {
  if (key_id >= header_->max_key_id) {
    return false;
  }
  if (key_id < 0) {
    key_id = -1;
  }
  for (++key_id; key_id <= header_->max_key_id; ++key_id) {
    if (get_bit(key_id)) {
      if (next_key_id) {
        *next_key_id = key_id;
      }
      if (next_key) {
        *next_key = keys_[key_id];
      }
      return true;
    }
  }
  return false;
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
  if (key_id < header_->next_key_id) {
    header_->next_key_id = key_id;
  }
  --header_->num_keys;
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
  if (find(dest_key)) {
    return false;
  }
  keys_[key_id] = Helper<T>::normalize(dest_key);
  return true;
}

template <typename T>
bool Array<T>::find(T key, int64_t *key_id) {
  for (int64_t i = 0; i <= header_->max_key_id; ++i) {
    if (get_bit(i)) {
      if (Helper<T>::equal_to(key, keys_[i])) {
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
  int64_t next_key_id_candidate = -1;
  for (int64_t i = 0; i <= header_->max_key_id; ++i) {
    if (get_bit(i)) {
      if (Helper<T>::equal_to(key, keys_[i])) {
        if (key_id) {
          *key_id = i;
        }
        return false;
      }
    } else if (key_id_candidate == -1) {
      // Use the first invalid ID if exists.
      key_id_candidate = i;
    } else if (next_key_id_candidate == -1) {
      // Use the second invalid ID if exists.
      next_key_id_candidate = i;
    }
  }
  if (key_id_candidate == -1) {
    key_id_candidate = ++header_->max_key_id;
  }
  keys_[key_id_candidate] = Helper<T>::normalize(key);
  set_bit(key_id_candidate, true);
  header_->next_key_id = (next_key_id_candidate != -1) ?
      next_key_id_candidate : (header_->max_key_id + 1);
  ++header_->num_keys;
  if (key_id) {
    *key_id = key_id_candidate;
  }
  return true;
}

template <typename T>
bool Array<T>::remove(T key) {
  int64_t key_id;
  if (!find(key, &key_id)) {
    return false;
  }
  set_bit(key_id, false);
  if (key_id < header_->next_key_id) {
    header_->next_key_id = key_id;
  }
  --header_->num_keys;
  return true;
}

template <typename T>
bool Array<T>::update(T src_key, T dest_key, int64_t *key_id) {
  int64_t src_key_id;
  if (!find(src_key, &src_key_id)) {
    return false;
  }
  if (find(dest_key)) {
    return false;
  }
  keys_[src_key_id] = Helper<T>::normalize(dest_key);
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
  header_->next_key_id = 0;
  header_->num_keys = 0;
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
template class Array<GeoPoint>;

Array<Slice>::~Array() {}

Array<Slice> *Array<Slice>::create(io::Pool pool, const MapOptions &) {
  std::unique_ptr<Array<Slice>> array(new (std::nothrow) Array<Slice>);
  array->pool_ = pool;
  array->block_info_ = pool.create_block(sizeof(ArrayHeader));
  array->header_ = static_cast<ArrayHeader *>(
      pool.get_block_address(*array->block_info_));
  *array->header_ = ArrayHeader();
  array->keys_.create(pool);
  array->header_->keys_block_id = array->keys_.block_id();
  return array.release();
}

Array<Slice> *Array<Slice>::open(io::Pool pool, uint32_t block_id) {
  std::unique_ptr<Array<Slice>> array(new (std::nothrow) Array<Slice>);
  array->pool_ = pool;
  array->block_info_ = pool.get_block_info(block_id);
  array->header_ = static_cast<ArrayHeader *>(
      pool.get_block_address(*array->block_info_));
  array->keys_.open(pool, array->header_->keys_block_id);
  return array.release();
}

bool Array<Slice>::unlink(io::Pool pool, uint32_t block_id) {
  std::unique_ptr<Array<Slice>> array(open(pool, block_id));
  array->keys_.unlink(pool, array->header_->keys_block_id);
  pool.free_block(array->block_id());
  return false;
}

uint32_t Array<Slice>::block_id() const {
  return block_info_->id();
}

MapType Array<Slice>::type() const {
  return MAP_ARRAY;
}

int64_t Array<Slice>::max_key_id() const {
  return header_->max_key_id;
}

int64_t Array<Slice>::next_key_id() const {
  return header_->next_key_id;
}

uint64_t Array<Slice>::num_keys() const {
  return header_->num_keys;
}

bool Array<Slice>::get(int64_t key_id, Slice *key) {
  if ((key_id < 0) || (key_id > header_->max_key_id)) {
    return false;
  }
  db::Blob blob = keys_[key_id].get();
  if (!blob) {
    return false;
  }
  if (key) {
    *key = blob_to_slice(blob);
  }
  return true;
}

bool Array<Slice>::get_next(int64_t key_id, int64_t *next_key_id,
                            Slice *next_key) {
  if (key_id >= header_->max_key_id) {
    return false;
  }
  if (key_id < 0) {
    key_id = -1;
  }
  for (++key_id; key_id <= header_->max_key_id; ++key_id) {
    db::Blob blob = keys_[key_id].get();
    if (blob) {
      if (next_key_id) {
        *next_key_id = key_id;
      }
      if (next_key) {
        *next_key = blob_to_slice(blob);
      }
      return true;
    }
  }
  return false;
}

bool Array<Slice>::unset(int64_t key_id) {
  if ((key_id < 0) || (key_id > header_->max_key_id)) {
    return false;
  }
  db::Blob blob = keys_[key_id].get();
  if (!blob) {
    return false;
  }
  keys_[key_id] = nullptr;
  if (key_id < header_->next_key_id) {
    header_->next_key_id = key_id;
  }
  --header_->num_keys;
  return true;
}

bool Array<Slice>::reset(int64_t key_id, Slice dest_key) {
  if ((key_id < 0) || (key_id > header_->max_key_id)) {
    return false;
  }
  db::Blob blob = keys_[key_id].get();
  if (!blob) {
    return false;
  }
  if (!dest_key || find(dest_key)) {
    return false;
  }
  std::string buf;
  keys_[key_id] = slice_to_blob(dest_key, &buf);
  return true;
}

bool Array<Slice>::find(Slice key, int64_t *key_id) {
  for (int64_t i = 0; i <= header_->max_key_id; ++i) {
    db::Blob blob = keys_[i].get();
    if (key == blob_to_slice(blob)) {
      if (key_id) {
        *key_id = i;
      }
      return true;
    }
  }
  return false;
}

bool Array<Slice>::insert(Slice key, int64_t *key_id) {
  if (!key) {
    return false;
  }
  int64_t key_id_candidate = -1;
  int64_t next_key_id_candidate = -1;
  for (int64_t i = 0; i <= header_->max_key_id; ++i) {
    db::Blob blob = keys_[i].get();
    if (key == blob_to_slice(blob)) {
      if (key_id) {
        *key_id = i;
      }
      return false;
    } else if (!blob) {
      if (key_id_candidate == -1) {
        // Use the first invalid ID if exists.
        key_id_candidate = i;
      } else if (next_key_id_candidate == -1) {
        // Use the second invalid ID if exists.
        next_key_id_candidate = i;
      }
    }
  }
  if (key_id_candidate == -1) {
    key_id_candidate = ++header_->max_key_id;
  }
  std::string buf;
  keys_[key_id_candidate] = slice_to_blob(key, &buf);
  header_->next_key_id = (next_key_id_candidate != -1) ?
      next_key_id_candidate : (header_->max_key_id + 1);
  ++header_->num_keys;
  if (key_id) {
    *key_id = key_id_candidate;
  }
  return true;
}

bool Array<Slice>::remove(Slice key) {
  int64_t key_id;
  if (!find(key, &key_id)) {
    return false;
  }
  keys_[key_id] = nullptr;
  if (key_id < header_->next_key_id) {
    header_->next_key_id = key_id;
  }
  --header_->num_keys;
  return true;
}

bool Array<Slice>::update(Slice src_key, Slice dest_key, int64_t *key_id) {
  int64_t src_key_id;
  if (!find(src_key, &src_key_id)) {
    return false;
  }
  if (!dest_key || find(dest_key)) {
    return false;
  }
  std::string buf;
  keys_[src_key_id] = slice_to_blob(dest_key, &buf);
  if (key_id) {
    *key_id = src_key_id;
  }
  return true;
}

void Array<Slice>::truncate() {
  for (int64_t i = 0; i <= header_->max_key_id; ++i) {
    keys_[i] = nullptr;
  }
  header_->max_key_id = -1;
  header_->next_key_id = 0;
  header_->num_keys = 0;
}

Array<Slice>::Array()
  : pool_(),
    block_info_(nullptr),
    header_(nullptr),
    keys_() {}

}  // namespace map
}  // namespace alpha
}  // namespace grnxx
