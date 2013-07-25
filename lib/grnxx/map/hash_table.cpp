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
#include "grnxx/map/common_header.hpp"
#include "grnxx/map/hash.hpp"
#include "grnxx/map/helper.hpp"
#include "grnxx/map/key_pool.hpp"
#include "grnxx/mutex.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {
namespace map {
namespace {

constexpr char FORMAT_STRING[] = "grnxx::map::HashTable";

constexpr int64_t TABLE_ENTRY_UNUSED  = -1;
constexpr int64_t TABLE_ENTRY_REMOVED = -2;

constexpr uint64_t MIN_TABLE_SIZE = 256;

}  // namespace

struct HashTableHeader {
  CommonHeader common_header;
  uint32_t table_storage_node_id;
  uint32_t old_table_storage_node_id;
  uint32_t pool_storage_node_id;
  uint64_t num_entries;
  Mutex mutex;

  // Initialize the member variables.
  HashTableHeader();

  // Return true iff the header seems to be correct.
  explicit operator bool() const;
};

HashTableHeader::HashTableHeader()
    : common_header(FORMAT_STRING, MAP_HASH_TABLE),
      table_storage_node_id(STORAGE_INVALID_NODE_ID),
      old_table_storage_node_id(STORAGE_INVALID_NODE_ID),
      pool_storage_node_id(STORAGE_INVALID_NODE_ID),
      num_entries(0),
      mutex() {}

HashTableHeader::operator bool() const {
  return common_header.format() == FORMAT_STRING;
}

template <typename T>
HashTable<T>::HashTable()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      table_(),
      old_table_(),
      pool_() {}

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
  return pool_->max_key_id();
}

template <typename T>
uint64_t HashTable<T>::num_keys() const {
  return pool_->num_keys();
}

template <typename T>
bool HashTable<T>::get(int64_t key_id, Key *key) {
  if ((key_id < MAP_MIN_KEY_ID) || (key_id > max_key_id())) {
    // Out of range.
    return false;
  }
  return pool_->get(key_id, key);
}

template <typename T>
bool HashTable<T>::unset(int64_t key_id) {
  refresh_table();
  int64_t * const entry = find_key_id(key_id);
  if (!entry) {
    // Not found.
    return false;
  }
  pool_->unset(key_id);
  *entry = TABLE_ENTRY_REMOVED;
  return true;
}

template <typename T>
bool HashTable<T>::reset(int64_t key_id, KeyArg dest_key) {
  refresh_table();
  Key src_key;
  if (!get(key_id, &src_key)) {
    // Not found.
    return false;
  }
  int64_t * const src_entry = find_key_id(key_id);
  if (!src_entry) {
    // Not found.
    return false;
  }
  const Key dest_normalized_key = Helper<T>::normalize(dest_key);
  int64_t *dest_key_id;
  if (find_key(dest_normalized_key, &dest_key_id)) {
    // Found.
    return false;
  }
  pool_->reset(key_id, dest_normalized_key);
  if (*dest_key_id == TABLE_ENTRY_UNUSED) {
    ++header_->num_entries;
  }
  *dest_key_id = key_id;
  *src_entry = TABLE_ENTRY_REMOVED;
  return true;
}

template <typename T>
bool HashTable<T>::find(KeyArg key, int64_t *key_id) {
  refresh_table();
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
  refresh_table();
  // Rebuild the hash table if the filling rate is greater than 62.5%.
  const uint64_t table_size = table_->size();
  if (header_->num_entries > ((table_size + (table_size / 4)) / 2)) {
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
  }
  int64_t next_key_id = pool_->add(normalized_key);
  if (*stored_key_id == TABLE_ENTRY_UNUSED) {
    ++header_->num_entries;
  }
  *stored_key_id = next_key_id;
  if (key_id) {
    *key_id = next_key_id;
  }
  return true;
}

template <typename T>
bool HashTable<T>::remove(KeyArg key) {
  refresh_table();
  const Key normalized_key = Helper<T>::normalize(key);
  int64_t *stored_key_id;
  if (!find_key(normalized_key, &stored_key_id)) {
    // Not found.
    return false;
  }
  pool_->unset(*stored_key_id);
  *stored_key_id = TABLE_ENTRY_REMOVED;
  return true;
}

template <typename T>
bool HashTable<T>::replace(KeyArg src_key, KeyArg dest_key, int64_t *key_id) {
  refresh_table();
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
  pool_->reset(*src_key_id, dest_normalized_key);
  if (*dest_key_id == TABLE_ENTRY_UNUSED) {
    ++header_->num_entries;
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
  refresh_table();
  if (max_key_id() == MAP_MIN_KEY_ID) {
    // Nothing to do.
    return true;
  }
  // Create an empty table.
  std::unique_ptr<Table> new_table(
      Table::create(storage_, storage_node_id_,
                    MIN_TABLE_SIZE, TABLE_ENTRY_UNUSED));
  try {
    if (header_->old_table_storage_node_id != STORAGE_INVALID_NODE_ID) {
      Table::unlink(storage_, header_->old_table_storage_node_id);
      header_->old_table_storage_node_id = STORAGE_INVALID_NODE_ID;
    }
    pool_->truncate();
  } catch (...) {
    Table::unlink(storage_, new_table->storage_node_id());
    throw;
  }
  header_->num_entries = 0;
  header_->old_table_storage_node_id = header_->table_storage_node_id;
  header_->table_storage_node_id = new_table->storage_node_id();
  Lock lock(&header_->mutex);
  if (table_->storage_node_id() != header_->table_storage_node_id) {
    old_table_.swap(new_table);
    table_.swap(old_table_);
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
    table_.reset(Table::create(storage, storage_node_id_,
                               MIN_TABLE_SIZE, TABLE_ENTRY_UNUSED));
    pool_.reset(KeyPool<T>::create(storage, storage_node_id_));
    header_->table_storage_node_id = table_->storage_node_id();
    header_->pool_storage_node_id = pool_->storage_node_id();
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
  if (!*header_) {
    GRNXX_ERROR() << "wrong format: expected = " << FORMAT_STRING
                  << ", actual = " << header_->common_header.format();
    throw LogicError();
  }
  table_.reset(Table::open(storage, header_->table_storage_node_id));
  pool_.reset(KeyPool<T>::open(storage, header_->pool_storage_node_id));
}

template <typename T>
int64_t *HashTable<T>::find_key_id(int64_t key_id) {
  Table * const table = table_.get();
  Key stored_key;
  if (!get(key_id, &stored_key)) {
    // Not found.
    return nullptr;
  }
  const uint64_t hash_mask = table->size() - 1;
  const uint64_t first_hash = Hash<T>()(stored_key);
  for (uint64_t hash = first_hash; ; ) {
    int64_t &entry = table->get_value(hash & hash_mask);
    if (entry == key_id) {
      // Found.
      return &entry;
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
bool HashTable<T>::find_key(KeyArg key, int64_t **entry) {
  Table * const table = table_.get();
  *entry = nullptr;
  const uint64_t hash_mask = table->size() - 1;
  const uint64_t first_hash = Hash<T>()(key);
  for (uint64_t hash = first_hash; ; ) {
    int64_t &this_entry = table->get_value(hash & hash_mask);
    if (this_entry == TABLE_ENTRY_UNUSED) {
      // Not found.
      if (!*entry) {
        *entry = &this_entry;
      }
      return false;
    } else if (this_entry == TABLE_ENTRY_REMOVED) {
      // Save the first removed entry.
      if (!*entry) {
        *entry = &this_entry;
      }
    } else {
      Key stored_key = pool_->get_key(this_entry);
      if (Helper<T>::equal_to(stored_key, key)) {
        // Found.
        *entry = &this_entry;
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
  // Create a new table.
  uint64_t new_size = num_keys() * 2;
  if (new_size < MIN_TABLE_SIZE) {
    new_size = MIN_TABLE_SIZE;
  }
  new_size = 2ULL << bit_scan_reverse(new_size - 1);
  std::unique_ptr<Table> new_table(
      Table::create(storage_, storage_node_id_, new_size, TABLE_ENTRY_UNUSED));
  try {
    // Copy entries from the current table to the new table.
    const uint64_t new_mask = new_size - 1;
    int64_t key_id;
    for (key_id = MAP_MIN_KEY_ID; key_id <= max_key_id(); ++key_id) {
      Key stored_key;
      if (!pool_->get(key_id, &stored_key)) {
        continue;
      }
      const uint64_t first_hash = Hash<T>()(stored_key);
      for (uint64_t hash = first_hash; ; hash = rehash(hash)) {
        int64_t &entry = new_table->get_value(hash & new_mask);
        if (entry == TABLE_ENTRY_UNUSED) {
          entry = key_id;
          break;
        }
      }
    }
  } catch (...) {
    Table::unlink(storage_, new_table->storage_node_id());
    throw;
  }
  if (header_->old_table_storage_node_id != STORAGE_INVALID_NODE_ID) {
    Table::unlink(storage_, header_->old_table_storage_node_id);
  }
  header_->old_table_storage_node_id = header_->table_storage_node_id;
  header_->table_storage_node_id = new_table->storage_node_id();
  Lock lock(&header_->mutex);
  if (table_->storage_node_id() != header_->table_storage_node_id) {
    old_table_.swap(new_table);
    table_.swap(old_table_);
  }
}

template <typename T>
uint64_t HashTable<T>::rehash(uint64_t hash) const {
  return hash + 1;
}

template <typename T>
void HashTable<T>::refresh_table() {
  if (table_->storage_node_id() != header_->table_storage_node_id) {
    Lock lock(&header_->mutex);
    if (table_->storage_node_id() != header_->table_storage_node_id) {
      std::unique_ptr<Table> new_table(
          Table::open(storage_, header_->table_storage_node_id));
      old_table_.swap(new_table);
      table_.swap(old_table_);
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
