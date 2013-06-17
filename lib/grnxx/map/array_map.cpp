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
#include "grnxx/map/array_map.hpp"

#include <new>

#include "grnxx/bytes.hpp"
#include "grnxx/geo_point.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/map/array_map/header.hpp"
#include "grnxx/map/helper.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {
namespace map {

template <typename T>
ArrayMap<T>::ArrayMap()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      keys_() {}

template <typename T>
ArrayMap<T>::~ArrayMap() {}

template <typename T>
ArrayMap<T> *ArrayMap<T>::create(Storage *storage, uint32_t storage_node_id,
                                 const MapOptions &options) {
  std::unique_ptr<ArrayMap> map(new (std::nothrow) ArrayMap);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::ArrayMap failed";
    return nullptr;
  }
  if (!map->create_map(storage, storage_node_id, options)) {
    return nullptr;
  }
  return map.release();
}

template <typename T>
ArrayMap<T> *ArrayMap<T>::open(Storage *storage, uint32_t storage_node_id) {
  std::unique_ptr<ArrayMap> map(new (std::nothrow) ArrayMap);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::ArrayMap failed";
    return nullptr;
  }
  if (!map->open_map(storage, storage_node_id)) {
    return nullptr;
  }
  return map.release();
}

template <typename T>
uint32_t ArrayMap<T>::storage_node_id() const {
  return storage_node_id_;
}

template <typename T>
MapType ArrayMap<T>::type() const {
  return MAP_ARRAY;
}

template <typename T>
int64_t ArrayMap<T>::max_key_id() const {
  return keys_->max_key_id();
}

template <typename T>
uint64_t ArrayMap<T>::num_keys() const {
  return keys_->num_keys();
}

template <typename T>
bool ArrayMap<T>::get(int64_t key_id, Key *key) {
  if ((key_id < MAP_MIN_KEY_ID) || (key_id > max_key_id())) {
    // Out of range.
    return false;
  }
  bool bit;
  if (!keys_->get_bit(key_id, &bit)) {
    // Error.
    return false;
  }
  if (!bit) {
    // Not found.
    return false;
  }
  if (!keys_->get_key(key_id, key)) {
    // Error.
    return false;
  }
  return true;
}

template <typename T>
bool ArrayMap<T>::unset(int64_t key_id) {
  if (!keys_->unset(key_id)) {
    // Not found or error.
    return false;
  }
  return true;
}

template <typename T>
bool ArrayMap<T>::reset(int64_t key_id, KeyArg dest_key) {
  if (!get(key_id)) {
    // Not found or error.
    return false;
  }
  if (find(dest_key)) {
    // Found.
    return false;
  }
  if (!keys_->reset(key_id, Helper<T>::normalize(dest_key))) {
    // Error.
    return false;
  }
  return true;
}

template <typename T>
bool ArrayMap<T>::find(KeyArg key, int64_t *key_id) {
  const Key normalized_key = map::Helper<T>::normalize(key);
  for (int64_t i = MAP_MIN_KEY_ID; i <= max_key_id(); ++i) {
    bool bit;
    if (!keys_->get_bit(i, &bit)) {
      // Error.
      return false;
    }
    if (bit) {
      Key stored_key;
      if (!keys_->get_key(i, &stored_key)) {
        // Error.
        return false;
      }
      if (Helper<T>::equal_to(normalized_key, stored_key)) {
        // Found.
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
bool ArrayMap<T>::add(KeyArg key, int64_t *key_id) {
  const Key normalized_key = Helper<T>::normalize(key);
  for (int64_t i = MAP_MIN_KEY_ID; i <= max_key_id(); ++i) {
    bool bit;
    if (!keys_->get_bit(i, &bit)) {
      // Error.
      return false;
    }
    if (bit) {
      Key stored_key;
      if (!keys_->get_key(i, &stored_key)) {
        // Error.
        return false;
      }
      if (Helper<T>::equal_to(normalized_key, stored_key)) {
        // Found.
        if (key_id) {
          *key_id = i;
        }
        return false;
      }
    }
  }
  if (!keys_->add(normalized_key, key_id)) {
    // Error.
    return false;
  }
  return true;
}

template <typename T>
bool ArrayMap<T>::remove(KeyArg key) {
  int64_t key_id;
  if (!find(key, &key_id)) {
    // Not found or error.
    return false;
  }
  if (!keys_->unset(key_id)) {
    // Error.
    return false;
  }
  return true;
}

template <typename T>
bool ArrayMap<T>::replace(KeyArg src_key, KeyArg dest_key, int64_t *key_id) {
  const Key normalized_src_key = Helper<T>::normalize(src_key);
  const Key normalized_dest_key = Helper<T>::normalize(dest_key);
  int64_t src_key_id = MAP_INVALID_KEY_ID;
  for (int64_t i = MAP_MIN_KEY_ID; i <= max_key_id(); ++i) {
    bool bit;
    if (!keys_->get_bit(i, &bit)) {
      // Error.
      return false;
    }
    if (bit) {
      Key stored_key;
      if (!keys_->get_key(i, &stored_key)) {
        // Error.
        return false;
      }
      if (Helper<T>::equal_to(normalized_src_key, stored_key)) {
        // Found.
        src_key_id = i;
      }
      if (Helper<T>::equal_to(normalized_dest_key, stored_key)) {
        // Found.
        return false;
      }
    }
  }
  if (src_key_id == MAP_INVALID_KEY_ID) {
    // Not found.
    return false;
  }
  if (!keys_->reset(src_key_id, normalized_dest_key)) {
    // Error.
    return false;
  }
  if (key_id) {
    *key_id = src_key_id;
  }
  return true;
}

template <typename T>
bool ArrayMap<T>::truncate() {
  return keys_->truncate();
}

template <typename T>
bool ArrayMap<T>::create_map(Storage *storage, uint32_t storage_node_id,
                             const MapOptions &) {
  storage_ = storage;
  StorageNode storage_node =
      storage->create_node(storage_node_id, sizeof(Header));
  if (!storage_node) {
    return false;
  }
  storage_node_id_ = storage_node.id();
  header_ = static_cast<Header *>(storage_node.body());
  *header_ = Header();
  keys_.reset(KeyStore<T>::create(storage, storage_node_id_));
  if (!keys_) {
    storage->unlink_node(storage_node_id_);
    return false;
  }
  header_->keys_storage_node_id = keys_->storage_node_id();
  return true;
}

template <typename T>
bool ArrayMap<T>::open_map(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode storage_node = storage->open_node(storage_node_id);
  if (!storage_node) {
    return false;
  }
  if (storage_node.size() < sizeof(Header)) {
    GRNXX_ERROR() << "invalid format: size = " << storage_node.size()
                  << ", header_size = " << sizeof(Header);
    return false;
  }
  storage_node_id_ = storage_node_id;
  header_ = static_cast<Header *>(storage_node.body());
  keys_.reset(KeyStore<T>::open(storage, header_->keys_storage_node_id));
  if (!keys_) {
    return false;
  }
  return true;
}

template class ArrayMap<int8_t>;
template class ArrayMap<uint8_t>;
template class ArrayMap<int16_t>;
template class ArrayMap<uint16_t>;
template class ArrayMap<int32_t>;
template class ArrayMap<uint32_t>;
template class ArrayMap<int64_t>;
template class ArrayMap<uint64_t>;
template class ArrayMap<double>;
template class ArrayMap<GeoPoint>;
template class ArrayMap<Bytes>;

}  // namespace map
}  // namespace grnxx
