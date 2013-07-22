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
#include "grnxx/map/hash_table.hpp"

#include <new>

#include "grnxx/bytes.hpp"
#include "grnxx/exception.hpp"
#include "grnxx/geo_point.hpp"
#include "grnxx/intrinsic.hpp"
#include "grnxx/lock.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/map/hash_table/hash.hpp"
#include "grnxx/map/hash_table/header.hpp"
#include "grnxx/map/helper.hpp"
#include "grnxx/mutex.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {
namespace map {
namespace {

constexpr int64_t TABLE_ENTRY_UNUSED  = -1;
constexpr int64_t TABLE_ENTRY_REMOVED = -2;

constexpr uint64_t MIN_TABLE_SIZE = 256;

template <typename T>
using Hash = hash_table::Hash<T>;

}  // namespace

template <typename T>
HashTable<T>::HashTable()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      key_ids_(),
      old_key_ids_(),
      keys_() {}

template <typename T>
HashTable<T>::~HashTable() {}

template <typename T>
HashTable<T> *HashTable<T>::create(Storage *storage, uint32_t storage_node_id,
                                   const MapOptions &options) {
  std::unique_ptr<HashTable> map(new (std::nothrow) HashTable);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::HashTable failed";
    throw MemoryError();
  }
  map->create_map(storage, storage_node_id, options);
  return map.release();
}

template <typename T>
HashTable<T> *HashTable<T>::open(Storage *storage, uint32_t storage_node_id) {
  std::unique_ptr<HashTable> map(new (std::nothrow) HashTable);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::HashTable failed";
    throw MemoryError();
  }
  map->open_map(storage, storage_node_id);
  return map.release();
}

template <typename T>
uint32_t HashTable<T>::storage_node_id() const {
  return storage_node_id_;
}

template <typename T>
MapType HashTable<T>::type() const {
  return MAP_HASH_TABLE;
}

template <typename T>
int64_t HashTable<T>::max_key_id() const {
  return keys_->max_key_id();
}

template <typename T>
uint64_t HashTable<T>::num_keys() const {
  return keys_->num_keys();
}

template <typename T>
bool HashTable<T>::get(int64_t key_id, Key *key) {
  if ((key_id < MAP_MIN_KEY_ID) || (key_id > max_key_id())) {
    // Out of range.
    return false;
  }
  if (!keys_->get_bit(key_id)) {
    // Not found.
    return false;
  }
  if (key) {
    *key = keys_->get_key(key_id);
  }
  return true;
}

template <typename T>
bool HashTable<T>::unset(int64_t key_id) {
  refresh_key_ids();
  int64_t *stored_key_id;
  if (!find_key_id(key_id, &stored_key_id)) {
    // Not found.
    return false;
  }
  if (!keys_->unset(key_id)) {
    GRNXX_ERROR() << "failed to unset: key_id = " << key_id;
    throw LogicError();
  }
  *stored_key_id = TABLE_ENTRY_REMOVED;
  return true;
}

template <typename T>
bool HashTable<T>::reset(int64_t key_id, KeyArg dest_key) {
  refresh_key_ids();
  Key src_key;
  if (!get(key_id, &src_key)) {
    // Not found.
    return false;
  }
  int64_t *src_key_id;
  if (!find_key_id(key_id, &src_key_id)) {
    // Not found.
    return false;
  }
  const Key dest_normalized_key = Helper<T>::normalize(dest_key);
  int64_t *dest_key_id;
  if (find_key(dest_normalized_key, &dest_key_id)) {
    // Found.
    return false;
  }
  keys_->reset(key_id, dest_normalized_key);
  if (*dest_key_id == TABLE_ENTRY_UNUSED) {
    ++header_->num_key_ids;
  }
  *dest_key_id = key_id;
  *src_key_id = TABLE_ENTRY_REMOVED;
  return true;
}

template <typename T>
bool HashTable<T>::find(KeyArg key, int64_t *key_id) {
  refresh_key_ids();
  const Key normalized_key = Helper<T>::normalize(key);
  int64_t *stored_key_id;
  if (!find_key(normalized_key, &stored_key_id)) {
    // Not found.
    return false;
  }
  if (key_id) {
    *key_id = *stored_key_id;
  }
  return true;
}

template <typename T>
bool HashTable<T>::add(KeyArg key, int64_t *key_id) {
  refresh_key_ids();
  // Rebuild the hash table if the filling rate is greater than 62.5%.
  const uint64_t key_ids_size = key_ids_->size();
  if (header_->num_key_ids > ((key_ids_size + (key_ids_size / 4)) / 2)) {
    rebuild();
  }
  const Key normalized_key = Helper<T>::normalize(key);
  int64_t *stored_key_id;
  if (find_key(normalized_key, &stored_key_id)) {
    // Found.
    if (key_id) {
      *key_id = *stored_key_id;
    }
    return false;
  } else if (!stored_key_id) {
    // Error.
    return false;
  }
  int64_t next_key_id = keys_->add(normalized_key);
  if (*stored_key_id == TABLE_ENTRY_UNUSED) {
    ++header_->num_key_ids;
  }
  *stored_key_id = next_key_id;
  if (key_id) {
    *key_id = next_key_id;
  }
  return true;
}

template <typename T>
bool HashTable<T>::remove(KeyArg key) {
  refresh_key_ids();
  const Key normalized_key = Helper<T>::normalize(key);
  int64_t *stored_key_id;
  if (!find_key(normalized_key, &stored_key_id)) {
    // Not found.
    return false;
  }
  if (!keys_->unset(*stored_key_id)) {
    GRNXX_ERROR() << "failed to remove: key = " << key
                  << ", key_id = " << *stored_key_id;
    throw LogicError();
  }
  *stored_key_id = TABLE_ENTRY_REMOVED;
  return true;
}

template <typename T>
bool HashTable<T>::replace(KeyArg src_key, KeyArg dest_key, int64_t *key_id) {
  refresh_key_ids();
  const Key src_normalized_key = Helper<T>::normalize(src_key);
  int64_t *src_key_id;
  if (!find_key(src_normalized_key, &src_key_id)) {
    // Not found.
    return false;
  }
  const Key dest_normalized_key = Helper<T>::normalize(dest_key);
  int64_t *dest_key_id;
  if (find_key(dest_normalized_key, &dest_key_id)) {
    // Found.
    return false;
  }
  keys_->reset(*src_key_id, dest_normalized_key);
  if (*dest_key_id == TABLE_ENTRY_UNUSED) {
    ++header_->num_key_ids;
  }
  *dest_key_id = *src_key_id;
  *src_key_id = TABLE_ENTRY_REMOVED;
  if (key_id) {
    *key_id = *dest_key_id;
  }
  return true;
}

template <typename T>
bool HashTable<T>::truncate() {
  refresh_key_ids();
  if (max_key_id() == MAP_MIN_KEY_ID) {
    // Nothing to do.
    return true;
  }
  std::unique_ptr<KeyIDArray> new_key_ids(
      KeyIDArray::create(storage_, storage_node_id_, MIN_TABLE_SIZE,
                         TABLE_ENTRY_UNUSED));
  if (header_->old_key_ids_storage_node_id != STORAGE_INVALID_NODE_ID) try {
    KeyIDArray::unlink(storage_, header_->old_key_ids_storage_node_id);
  } catch (...) {
    KeyIDArray::unlink(storage_, new_key_ids->storage_node_id());
    throw;
  }
  keys_->truncate();
  header_->num_key_ids = 0;
  header_->old_key_ids_storage_node_id = header_->key_ids_storage_node_id;
  header_->key_ids_storage_node_id = new_key_ids->storage_node_id();
  Lock lock(&header_->mutex);
  if (key_ids_->storage_node_id() != header_->key_ids_storage_node_id) {
    old_key_ids_.swap(new_key_ids);
    key_ids_.swap(old_key_ids_);
  }
  return true;
}

template <typename T>
void HashTable<T>::create_map(Storage *storage, uint32_t storage_node_id,
                              const MapOptions &) {
  storage_ = storage;
  StorageNode storage_node =
      storage->create_node(storage_node_id, sizeof(Header));
  storage_node_id_ = storage_node.id();
  try {
    header_ = static_cast<Header *>(storage_node.body());
    *header_ = Header();
    key_ids_.reset(KeyIDArray::create(storage, storage_node_id_,
                                      MIN_TABLE_SIZE, TABLE_ENTRY_UNUSED));
    keys_.reset(KeyStore<T>::create(storage, storage_node_id_));
    header_->key_ids_storage_node_id = key_ids_->storage_node_id();
    header_->keys_storage_node_id = keys_->storage_node_id();
  } catch (...) {
    storage->unlink_node(storage_node_id_);
    throw;
  }
}

template <typename T>
void HashTable<T>::open_map(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode storage_node = storage->open_node(storage_node_id);
  if (storage_node.size() < sizeof(Header)) {
    GRNXX_ERROR() << "invalid format: size = " << storage_node.size()
                  << ", header_size = " << sizeof(Header);
    throw LogicError();
  }
  storage_node_id_ = storage_node_id;
  header_ = static_cast<Header *>(storage_node.body());
  key_ids_.reset(KeyIDArray::open(storage, header_->key_ids_storage_node_id));
  keys_.reset(KeyStore<T>::open(storage, header_->keys_storage_node_id));
}

template <typename T>
bool HashTable<T>::find_key_id(int64_t key_id, int64_t **stored_key_id) {
  KeyIDArray * const key_ids = key_ids_.get();
  Key stored_key;
  if (!get(key_id, &stored_key)) {
    // Not found.
    return false;
  }
  const uint64_t first_hash = Hash<T>()(stored_key);
  for (uint64_t hash = first_hash; ; ) {
    *stored_key_id = &key_ids->get_value(hash & (key_ids->size() - 1));
    if (**stored_key_id == key_id) {
      // Found.
      return true;
    }
    hash = rehash(hash);
    if (hash == first_hash) {
      // Critical error.
      GRNXX_ERROR() << "endless loop";
      throw LogicError();
    }
  }
}

template <typename T>
bool HashTable<T>::find_key(KeyArg key, int64_t **stored_key_id) {
  KeyIDArray * const key_ids = key_ids_.get();
  *stored_key_id = nullptr;
  const uint64_t first_hash = Hash<T>()(key);
  for (uint64_t hash = first_hash; ; ) {
    int64_t * const key_id = &key_ids->get_value(hash & (key_ids->size() - 1));
    if (*key_id == TABLE_ENTRY_UNUSED) {
      // Not found.
      if (!*stored_key_id) {
        *stored_key_id = key_id;
      }
      return false;
    } else if (*key_id == TABLE_ENTRY_REMOVED) {
      // Save the first removed entry.
      if (!*stored_key_id) {
        *stored_key_id = key_id;
      }
    } else {
      Key stored_key = keys_->get_key(*key_id);
      if (Helper<T>::equal_to(stored_key, key)) {
        // Found.
        *stored_key_id = key_id;
        return true;
      }
    }
    hash = rehash(hash);
    if (hash == first_hash) {
      // Critical error.
      GRNXX_ERROR() << "endless loop";
      throw LogicError();
    }
  }
}

template <typename T>
void HashTable<T>::rebuild() {
  uint64_t new_size = num_keys() * 2;
  if (new_size < MIN_TABLE_SIZE) {
    new_size = MIN_TABLE_SIZE;
  }
  new_size = 2ULL << bit_scan_reverse(new_size - 1);
  // TODO: Size upper limit check.
//  if (new_size > key_ids_->size()) {
//    // Critical error.
//    GRNXX_ERROR() << "too large table: size = " << new_size
//                  << ", max_size = " << key_ids_->size();
//    throw LogicError();
//  }
  // Create a new hash table.
  std::unique_ptr<KeyIDArray> new_key_ids(
      KeyIDArray::create(storage_, storage_node_id_, new_size,
                         TABLE_ENTRY_UNUSED));
  try {
    // Copy keys from the current hash table to the new one.
    const uint64_t new_mask = new_size - 1;
    int64_t key_id;
    for (key_id = MAP_MIN_KEY_ID; key_id <= max_key_id(); ++key_id) {
      if (!keys_->get_bit(key_id)) {
        continue;
      }
      Key stored_key = keys_->get_key(key_id);
      const uint64_t first_hash = Hash<T>()(stored_key);
      int64_t *stored_key_id;
      for (uint64_t hash = first_hash; ; hash = rehash(hash)) {
        stored_key_id = &new_key_ids->get_value(hash & new_mask);
        if (*stored_key_id == TABLE_ENTRY_UNUSED) {
          *stored_key_id = key_id;
          break;
        }
      }
    }
  } catch (...) {
    KeyIDArray::unlink(storage_, new_key_ids->storage_node_id());
    throw;
  }
  if (header_->old_key_ids_storage_node_id != STORAGE_INVALID_NODE_ID) {
    KeyIDArray::unlink(storage_, header_->old_key_ids_storage_node_id);
  }
  header_->old_key_ids_storage_node_id = header_->key_ids_storage_node_id;
  header_->key_ids_storage_node_id = new_key_ids->storage_node_id();
  Lock lock(&header_->mutex);
  if (key_ids_->storage_node_id() != header_->key_ids_storage_node_id) {
    old_key_ids_.swap(new_key_ids);
    key_ids_.swap(old_key_ids_);
  }
}

template <typename T>
uint64_t HashTable<T>::rehash(uint64_t hash) const {
  return hash + 1;
}

template <typename T>
void HashTable<T>::refresh_key_ids() {
  if (key_ids_->storage_node_id() != header_->key_ids_storage_node_id) {
    Lock lock(&header_->mutex);
    if (key_ids_->storage_node_id() != header_->key_ids_storage_node_id) {
      std::unique_ptr<KeyIDArray> new_key_ids(
          KeyIDArray::open(storage_, header_->key_ids_storage_node_id));
      old_key_ids_.swap(new_key_ids);
      key_ids_.swap(old_key_ids_);
    }
  }
}

template class HashTable<int8_t>;
template class HashTable<uint8_t>;
template class HashTable<int16_t>;
template class HashTable<uint16_t>;
template class HashTable<int32_t>;
template class HashTable<uint32_t>;
template class HashTable<int64_t>;
template class HashTable<uint64_t>;
template class HashTable<double>;
template class HashTable<GeoPoint>;
template class HashTable<Bytes>;

}  // namespace map
}  // namespace grnxx
