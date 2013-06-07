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

template <typename T>
using Hash = hash_table::Hash<T>;
using hash_table::INVALID_LINK;

template <typename T>
HashTable<T>::HashTable()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      key_ids_(),
      old_key_ids_(),
      keys_(),
      bits_(),
      links_() {}

template <typename T>
HashTable<T>::~HashTable() {}

template <typename T>
HashTable<T> *HashTable<T>::create(Storage *storage, uint32_t storage_node_id,
                                   const MapOptions &options) {
  std::unique_ptr<HashTable> map(new (std::nothrow) HashTable);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::HashTable failed";
    return nullptr;
  }
  if (!map->create_map(storage, storage_node_id, options)) {
    return nullptr;
  }
  return map.release();
}

template <typename T>
HashTable<T> *HashTable<T>::open(Storage *storage, uint32_t storage_node_id) {
  std::unique_ptr<HashTable> map(new (std::nothrow) HashTable);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::HashTable failed";
    return nullptr;
  }
  if (!map->open_map(storage, storage_node_id)) {
    return nullptr;
  }
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
  return header_->max_key_id;
}

template <typename T>
uint64_t HashTable<T>::num_keys() const {
  return header_->num_keys;
}

template <typename T>
bool HashTable<T>::get(int64_t key_id, Key *key) {
  if ((key_id < MAP_MIN_KEY_ID) || (key_id > header_->max_key_id)) {
    // Out of range.
    return false;
  }
  bool bit;
  if (!bits_->get(key_id, &bit)) {
    // Error.
    return false;
  }
  if (!bit) {
    // Not found.
    return false;
  }
  return keys_->get(key_id, key);
}

template <typename T>
bool HashTable<T>::unset(int64_t key_id) {
  if (!refresh_key_ids()) {
    // Error.
    return false;
  }
  int64_t *stored_key_id;
  if (!find_key_id(key_id, &stored_key_id)) {
    // Not found or error.
    return false;
  }
  // Prepare to update "bits_" and "links_".
  const uint64_t unit_id = key_id / bits_->unit_size();
  const uint64_t unit_bit = 1ULL << (key_id % bits_->unit_size());
  BitArrayUnit * const unit = bits_->get_unit(unit_id);
  if (!unit) {
    // Error.
    return false;
  }
  // Set "link" if this operation creates the first 0 bit in the unit.
  uint64_t *link = nullptr;
  if (*unit == ~BitArrayUnit(0)) {
    link = links_->get_pointer(unit_id);
    if (!link) {
      // Error.
      return false;
    }
  }
  *unit &= ~unit_bit;
  *stored_key_id = -1;
  if (link) {
    *link = header_->latest_link;
    header_->latest_link = *link;
  }
  --header_->num_keys;
  return true;
}

template <typename T>
bool HashTable<T>::reset(int64_t key_id, KeyArg dest_key) {
  if (!refresh_key_ids()) {
    // Error.
    return false;
  }
  Key src_key;
  if (!get(key_id, &src_key)) {
    // Not found or error.
    return false;
  }
  int64_t *src_key_id;
  if (!find_key_id(key_id, &src_key_id)) {
    // Not found or error.
    return false;
  }
  const Key dest_normalized_key = Helper<T>::normalize(dest_key);
  int64_t *dest_key_id;
  if (find_key(dest_normalized_key, &dest_key_id)) {
    // Found or error.
    return false;
  }
  if (!keys_->set(key_id, dest_normalized_key)) {
    // Error.
    return false;
  }
  if (*dest_key_id == MAP_INVALID_KEY_ID) {
    ++header_->num_key_ids;
  }
  *dest_key_id = key_id;
  *src_key_id = -1;
  return true;
}

template <typename T>
bool HashTable<T>::find(KeyArg key, int64_t *key_id) {
  if (!refresh_key_ids()) {
    // Error.
    return false;
  }
  const Key normalized_key = Helper<T>::normalize(key);
  int64_t *stored_key_id;
  if (!find_key(normalized_key, &stored_key_id)) {
    // Not found or error.
    return false;
  }
  if (key_id) {
    *key_id = *stored_key_id;
  }
  return true;
}

template <typename T>
bool HashTable<T>::add(KeyArg key, int64_t *key_id) {
  if (!refresh_key_ids()) {
    // Error.
    return false;
  }
  // Rebuild the hash table if the filling rate is greater than 62.5%.
  const uint64_t key_ids_size = key_ids_->mask() + 1;
  if (header_->num_key_ids > ((key_ids_size + (key_ids_size / 4)) / 2)) {
    if (!rebuild()) {
      // Error.
      return false;
    }
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
  // Find an unused key ID.
  const bool is_new_unit = (header_->latest_link == INVALID_LINK);
  uint64_t unit_id;
  if (is_new_unit) {
    unit_id = (header_->max_key_id + 1) / bits_->unit_size();
  } else {
    unit_id = header_->latest_link;
  }
  BitArrayUnit * const unit = bits_->get_unit(unit_id);
  if (!unit) {
    // Error.
    return false;
  }
  const uint8_t unit_bit_id = bit_scan_forward(~*unit);
  const uint64_t unit_bit = 1ULL << unit_bit_id;
  const int64_t next_key_id = (unit_id * bits_->unit_size()) + unit_bit_id;
  uint64_t *link = nullptr;
  if (is_new_unit) {
    if (!links_->set(unit_id, INVALID_LINK)) {
      // Error.
      return false;
    }
    *unit = 0;
    header_->latest_link = (header_->max_key_id + 1) / bits_->unit_size();
  } else if ((*unit | unit_bit) == ~BitArrayUnit(0)) {
    // The unit will be removed from "links_" if it becomes full.
    link = links_->get_pointer(header_->latest_link);
    if (!link) {
      // Error.
      return false;
    }
  }
  if (!keys_->set(next_key_id, normalized_key)) {
    // Error.
    return false;
  }
  if (link) {
    header_->latest_link = *link;
  }
  *unit |= unit_bit;
  if (*stored_key_id == MAP_INVALID_KEY_ID) {
    ++header_->num_key_ids;
  }
  *stored_key_id = next_key_id;
  if (next_key_id > header_->max_key_id) {
    header_->max_key_id = next_key_id;
  }
  ++header_->num_keys;
  if (key_id) {
    *key_id = next_key_id;
  }
  return true;
}

template <typename T>
bool HashTable<T>::remove(KeyArg key) {
  if (!refresh_key_ids()) {
    // Error.
    return false;
  }
  const Key normalized_key = Helper<T>::normalize(key);
  int64_t *stored_key_id;
  if (!find_key(normalized_key, &stored_key_id)) {
    // Not found or error.
    return false;
  }
  // Prepare to update "bits_" and "links_".
  const uint64_t unit_id = *stored_key_id / bits_->unit_size();
  const uint64_t unit_bit = 1ULL << (*stored_key_id % bits_->unit_size());
  BitArrayUnit * const unit = bits_->get_unit(unit_id);
  if (!unit) {
    // Error.
    return false;
  }
  // Set "link" if this operation creates the first 0 bit in the unit.
  uint64_t *link = nullptr;
  if (*unit == ~BitArrayUnit(0)) {
    link = links_->get_pointer(unit_id);
    if (!link) {
      // Error.
      return false;
    }
  }
  *unit &= ~unit_bit;
  *stored_key_id = -1;
  if (link) {
    *link = header_->latest_link;
    header_->latest_link = *link;
  }
  --header_->num_keys;
  return true;
}

template <typename T>
bool HashTable<T>::replace(KeyArg src_key, KeyArg dest_key, int64_t *key_id) {
  if (!refresh_key_ids()) {
    // Error.
    return false;
  }
  const Key src_normalized_key = Helper<T>::normalize(src_key);
  int64_t *src_key_id;
  if (!find_key(src_normalized_key, &src_key_id)) {
    // Not found or error.
    return false;
  }
  const Key dest_normalized_key = Helper<T>::normalize(dest_key);
  int64_t *dest_key_id;
  if (find_key(dest_normalized_key, &dest_key_id)) {
    // Found or error.
    return false;
  }
  if (!keys_->set(*src_key_id, dest_normalized_key)) {
    // Error.
    return false;
  }
  if (*dest_key_id == MAP_INVALID_KEY_ID) {
    ++header_->num_key_ids;
  }
  *dest_key_id = *src_key_id;
  *src_key_id = -1;
  if (key_id) {
    *key_id = *dest_key_id;
  }
  return true;
}

template <typename T>
bool HashTable<T>::truncate() {
  if (!refresh_key_ids()) {
    return false;
  }
  if (header_->max_key_id == MAP_MIN_KEY_ID) {
    // Nothing to do.
    return true;
  }
  std::unique_ptr<KeyIDArray> new_key_ids(
      KeyIDArray::create(storage_, storage_node_id_,
                         KeyIDArray::page_size() - 1));
  if (!new_key_ids) {
    // Error.
    return false;
  }
  if (header_->old_key_ids_storage_node_id != STORAGE_INVALID_NODE_ID) {
    if (!KeyIDArray::unlink(storage_, header_->old_key_ids_storage_node_id)) {
      // Error.
      KeyIDArray::unlink(storage_, new_key_ids->storage_node_id());
      return false;
    }
  }
  header_->max_key_id = MAP_MIN_KEY_ID - 1;
  header_->num_keys = 0;
  header_->num_key_ids = 0;
  header_->latest_link = INVALID_LINK;
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
bool HashTable<T>::create_map(Storage *storage, uint32_t storage_node_id,
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
  key_ids_.reset(KeyIDArray::create(storage, storage_node_id_,
                                    KeyIDArray::page_size() - 1));
  keys_.reset(KeyArray::create(storage, storage_node_id_));
  bits_.reset(BitArray::create(storage, storage_node_id_));
  links_.reset(LinkArray::create(storage, storage_node_id_));
  if (!key_ids_ || !keys_ || !bits_ || !links_) {
    storage->unlink_node(storage_node_id_);
    return false;
  }
  header_->key_ids_storage_node_id = key_ids_->storage_node_id();
  header_->keys_storage_node_id = keys_->storage_node_id();
  header_->bits_storage_node_id = bits_->storage_node_id();
  header_->links_storage_node_id = links_->storage_node_id();
  return true;
}

template <typename T>
bool HashTable<T>::open_map(Storage *storage, uint32_t storage_node_id) {
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
  key_ids_.reset(KeyIDArray::open(storage, header_->key_ids_storage_node_id));
  keys_.reset(KeyArray::open(storage, header_->keys_storage_node_id));
  bits_.reset(BitArray::open(storage, header_->bits_storage_node_id));
  links_.reset(LinkArray::open(storage, header_->links_storage_node_id));
  if (!key_ids_ || !keys_ || !bits_ || !links_) {
    return false;
  }
  return true;
}

template <typename T>
bool HashTable<T>::find_key_id(int64_t key_id, int64_t **stored_key_id) {
  KeyIDArray * const key_ids = key_ids_.get();
  Key stored_key;
  if (!get(key_id, &stored_key)) {
    // Not found or error.
    return false;
  }
  const uint64_t first_hash = Hash<T>()(stored_key);
  for (uint64_t hash = first_hash; ; ) {
    *stored_key_id = key_ids->get_pointer(hash);
    if (!*stored_key_id) {
      // Error.
      return false;
    }
    if (**stored_key_id == key_id) {
      // Found.
      return true;
    }
    hash = rehash(hash);
    if (hash == first_hash) {
      // Critical error.
      GRNXX_ERROR() << "endless loop";
      return false;
    }
  }
}

template <typename T>
bool HashTable<T>::find_key(KeyArg key, int64_t **stored_key_id) {
  KeyIDArray * const key_ids = key_ids_.get();
  *stored_key_id = nullptr;
  const uint64_t first_hash = Hash<T>()(key);
  for (uint64_t hash = first_hash; ; ) {
    int64_t * const key_id = key_ids->get_pointer(hash);
    if (!key_id) {
      // Error.
      return false;
    }
    if (*key_id == MAP_INVALID_KEY_ID) {
      // Not found.
      if (!*stored_key_id) {
        *stored_key_id = key_id;
      }
      return false;
    } else if (*key_id < 0) {
      // Save the first removed entry.
      if (!*stored_key_id) {
        *stored_key_id = key_id;
      }
    } else {
      Key stored_key;
      if (!keys_->get(*key_id, &stored_key)) {
        // Error.
        return false;
      }
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
      return false;
    }
  }
}

template <typename T>
bool HashTable<T>::rebuild() {
  uint64_t new_size = header_->num_keys * 2;
  if (new_size < key_ids_->page_size()) {
    new_size = key_ids_->page_size();
  }
  new_size = 2ULL << bit_scan_reverse(new_size - 1);
  if (new_size > key_ids_->size()) {
    // Critical error.
    GRNXX_ERROR() << "too large table: size = " << new_size
                  << ", max_size = " << key_ids_->size();
    return false;
  }
  const uint64_t new_mask = new_size - 1;
  // Create a new hash table.
  std::unique_ptr<KeyIDArray> new_key_ids(
      KeyIDArray::create(storage_, storage_node_id_, new_mask));
  if (!new_key_ids) {
    // Error.
    return false;
  }
  // Copy keys from the current hash table to the new one.
  int64_t key_id;
  for (key_id = MAP_MIN_KEY_ID; key_id <= max_key_id(); ++key_id) {
    bool bit;
    if (!bits_->get(key_id, &bit)) {
      // Error.
      break;
    }
    if (!bit) {
      continue;
    }
    Key stored_key;
    if (!keys_->get(key_id, &stored_key)) {
      // Error.
      break;
    }
    const uint64_t first_hash = Hash<T>()(stored_key);
    int64_t *stored_key_id;
    for (uint64_t hash = first_hash; ; hash = rehash(hash)) {
      stored_key_id = new_key_ids->get_pointer(hash);
      if (!stored_key_id) {
        // Error.
        break;
      }
      if (*stored_key_id == MAP_INVALID_KEY_ID) {
        *stored_key_id = key_id;
        break;
      }
    }
    if (!stored_key_id) {
      // Error.
      break;
    }
  }
  if (key_id <= max_key_id()) {
    // Error.
    KeyIDArray::unlink(storage_, new_key_ids->storage_node_id());
    return false;
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
  return true;
}

template <typename T>
uint64_t HashTable<T>::rehash(uint64_t hash) const {
  return hash + 1;
}

template <typename T>
bool HashTable<T>::refresh_key_ids() {
  if (key_ids_->storage_node_id() != header_->key_ids_storage_node_id) {
    Lock lock(&header_->mutex);
    if (key_ids_->storage_node_id() != header_->key_ids_storage_node_id) {
      std::unique_ptr<KeyIDArray> new_key_ids(
          KeyIDArray::open(storage_, header_->key_ids_storage_node_id));
      if (!new_key_ids) {
        // Error.
        return false;
      }
      old_key_ids_.swap(new_key_ids);
      key_ids_.swap(old_key_ids_);
    }
  }
  return true;
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
