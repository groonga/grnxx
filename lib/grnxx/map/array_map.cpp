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
#include "grnxx/storage.hpp"

namespace grnxx {
namespace map {

struct ArrayMapHeader {
  MapType map_type;
  uint32_t bitmap_storage_node_id;
  uint32_t keys_storage_node_id;
  int64_t max_key_id;
  int64_t next_key_id;
  uint64_t num_keys;

  ArrayMapHeader();
};

ArrayMapHeader::ArrayMapHeader()
    : map_type(MAP_ARRAY),
      bitmap_storage_node_id(STORAGE_INVALID_NODE_ID),
      keys_storage_node_id(STORAGE_INVALID_NODE_ID),
      max_key_id(MAP_MIN_KEY_ID - 1),
      next_key_id(MAP_MIN_KEY_ID),
      num_keys(0) {}

template <typename T>
ArrayMap<T>::ArrayMap()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      bitmap_(),
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
  return header_->max_key_id;
}

template <typename T>
uint64_t ArrayMap<T>::num_keys() const {
  return header_->num_keys;
}

template <typename T>
bool ArrayMap<T>::get(int64_t key_id, Key *key) {
  if ((key_id < MAP_MIN_KEY_ID) || (key_id > header_->max_key_id)) {
    return false;
  }
  bool bit;
  if (!bitmap_->get(key_id, &bit)) {
    return false;
  }
  if (!bit) {
    return false;
  }
  if (!key) {
    return true;
  }
  return keys_->get(key_id, key);
}

template <typename T>
bool ArrayMap<T>::get_next(int64_t key_id, int64_t *next_key_id,
                           Key *next_key) {
  if ((key_id < MAP_MIN_KEY_ID) || (key_id > MAP_MAX_KEY_ID)) {
    key_id = MAP_MIN_KEY_ID - 1;
  }
  for (++key_id; key_id <= max_key_id(); ++key_id) {
    if (get(key_id, next_key)) {
      if (next_key_id) {
        *next_key_id = key_id;
      }
      return true;
    }
  }
  return false;
}

template <typename T>
bool ArrayMap<T>::unset(int64_t key_id) {
  if (!get(key_id)) {
//    GRNXX_WARNING() << "not found: key_id = " << key_id;
    return false;
  }
  if (!bitmap_->set(key_id, false)) {
    return false;
  }
  header_->next_key_id = key_id;
  --header_->num_keys;
  return true;
}

template <typename T>
bool ArrayMap<T>::reset(int64_t key_id, KeyArg dest_key) {
  if (!get(key_id)) {
//    GRNXX_WARNING() << "not found: key_id = " << key_id;
    return false;
  }
  if (find(dest_key)) {
//    GRNXX_WARNING() << "found: dest_key = " << dest_key;
    return false;
  }
  if (!keys_->set(key_id, Helper<T>::normalize(dest_key))) {
    return false;
  }
  return true;
}

template <typename T>
bool ArrayMap<T>::find(KeyArg key, int64_t *key_id) {
  const Key normalized_key = map::Helper<T>::normalize(key);
  for (int64_t i = MAP_MIN_KEY_ID; i <= header_->max_key_id; ++i) {
    bool bit;
    if (!bitmap_->get(i, &bit)) {
      return false;
    }
    if (bit) {
      Key stored_key;
      if (!keys_->get(i, &stored_key)) {
        return false;
      }
      if (Helper<T>::equal_to(normalized_key, stored_key)) {
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
  int64_t next_key_id = header_->next_key_id;
  int64_t next_next_key_id = MAP_INVALID_KEY_ID;
  for (int64_t i = MAP_MIN_KEY_ID; i <= header_->max_key_id; ++i) {
    bool bit;
    if (!bitmap_->get(i, &bit)) {
      return false;
    }
    if (bit) {
      Key stored_key;
      if (!keys_->get(i, &stored_key)) {
        return false;
      }
      if (Helper<T>::equal_to(normalized_key, stored_key)) {
        if (key_id) {
          *key_id = i;
        }
//        GRNXX_WARNING() << "found: key = " << key;
        return false;
      }
    } else if ((i != next_key_id) &&
               (next_next_key_id == MAP_INVALID_KEY_ID)) {
      next_next_key_id = i;
    }
  }
  if (!keys_->set(next_key_id, normalized_key) ||
      !bitmap_->set(next_key_id, true)) {
    return false;
  }
  if (next_key_id == (header_->max_key_id + 1)) {
    ++header_->max_key_id;
  }
  if (next_next_key_id == MAP_INVALID_KEY_ID) {
    header_->next_key_id = header_->max_key_id + 1;
  } else {
    header_->next_key_id = next_next_key_id;
  }
  ++header_->num_keys;
  if (key_id) {
    *key_id = next_key_id;
  }
  return true;
}

template <typename T>
bool ArrayMap<T>::remove(KeyArg key) {
  int64_t key_id;
  if (!find(key, &key_id)) {
//    GRNXX_WARNING() << "not found: key = " << key;
    return false;
  }
  if (!bitmap_->set(key_id, false)) {
    return false;
  }
  header_->next_key_id = key_id;
  --header_->num_keys;
  return true;
}

template <typename T>
bool ArrayMap<T>::replace(KeyArg src_key, KeyArg dest_key, int64_t *key_id) {
  const Key normalized_src_key = Helper<T>::normalize(src_key);
  const Key normalized_dest_key = Helper<T>::normalize(dest_key);
  int64_t src_key_id = MAP_INVALID_KEY_ID;
  for (int64_t i = MAP_MIN_KEY_ID; i <= header_->max_key_id; ++i) {
    bool bit;
    if (!bitmap_->get(i, &bit)) {
      return false;
    }
    if (bit) {
      Key stored_key;
      if (!keys_->get(i, &stored_key)) {
        return false;
      }
      if (Helper<T>::equal_to(normalized_src_key, stored_key)) {
        src_key_id = i;
      }
      if (Helper<T>::equal_to(normalized_dest_key, stored_key)) {
//        GRNXX_WARNING() << "found: dest_key = " << dest_key;
        return false;
      }
    }
  }
  if (src_key_id == MAP_INVALID_KEY_ID) {
//    GRNXX_WARNING() << "not found: src_key = " << src_key;
    return false;
  }
  if (!keys_->set(src_key_id, normalized_dest_key)) {
    return false;
  }
  if (key_id) {
    *key_id = src_key_id;
  }
  return true;
}

template <typename T>
bool ArrayMap<T>::truncate() {
  header_->max_key_id = MAP_MIN_KEY_ID - 1;
  header_->next_key_id = MAP_MIN_KEY_ID;
  header_->num_keys = 0;
  return true;
}

template <typename T>
bool ArrayMap<T>::create_map(Storage *storage, uint32_t storage_node_id,
                             const MapOptions &) {
  storage_ = storage;
  StorageNode storage_node =
      storage->create_node(storage_node_id, sizeof(ArrayMapHeader));
  if (!storage_node) {
    return false;
  }
  storage_node_id_ = storage_node.id();
  header_ = static_cast<ArrayMapHeader *>(storage_node.body());
  *header_ = ArrayMapHeader();
  bitmap_.reset(Bitmap::create(storage, storage_node_id_));
  keys_.reset(KeyArray::create(storage, storage_node_id_));
  if (!bitmap_ || !keys_) {
    storage->unlink_node(storage_node_id_);
    return false;
  }
  header_->bitmap_storage_node_id = bitmap_->storage_node_id();
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
  if (storage_node.size() < sizeof(ArrayMapHeader)) {
    GRNXX_ERROR() << "invalid format: size = " << storage_node.size()
                  << ", header_size = " << sizeof(ArrayMapHeader);
    return false;
  }
  storage_node_id_ = storage_node_id;
  header_ = static_cast<ArrayMapHeader *>(storage_node.body());
  bitmap_.reset(Bitmap::open(storage, header_->bitmap_storage_node_id));
  keys_.reset(KeyArray::open(storage, header_->keys_storage_node_id));
  if (!bitmap_ || !keys_) {
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
