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
#include "grnxx/map/pool.hpp"
#include "grnxx/mutex.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {
namespace map {
namespace {

constexpr char FORMAT_STRING[] = "grnxx::map::HashTable";

constexpr uint64_t MIN_TABLE_SIZE = 64;
constexpr uint64_t MAX_TABLE_SIZE = 1ULL << 41;

struct ImplHeader {
  uint64_t num_entries;
  uint64_t table_size;
  uint32_t table_storage_node_id;

  ImplHeader();
};

ImplHeader::ImplHeader()
    : num_entries(0),
      table_size(0),
      table_storage_node_id(STORAGE_INVALID_NODE_ID) {}

class TableEntry {
  static constexpr uint64_t IS_UNUSED_FLAG  = 1ULL << 40;
  static constexpr uint64_t IS_REMOVED_FLAG = 1ULL << 41;
  static constexpr uint8_t  MEMO_SHIFT      = 42;
  static constexpr uint64_t KEY_ID_MASK     = (1ULL << 40) - 1;

 public:
  // Create an unused entry.
  static TableEntry unused_entry() {
    return TableEntry(IS_UNUSED_FLAG);
  }

  // Return true iff this entry is not used.
  bool is_unused() const {
    return value_ & IS_UNUSED_FLAG;
  }
  // Return true iff this entry is removed.
  bool is_removed() const {
    return value_ & IS_REMOVED_FLAG;
  }
  // Return true iff this entry and "hash_value" have the same memo.
  bool test_hash_value(uint64_t hash_value) const {
    return ((value_ ^ hash_value) >> MEMO_SHIFT) == 0;
  }
  // Return a stored key ID.
  int64_t key_id() const {
    return value_ & KEY_ID_MASK;
  }

  // Set a key ID and a memo, which is extracted from "hash_value".
  void set(int64_t key_id, uint64_t hash_value) {
    value_ = key_id | (hash_value & (~0ULL << MEMO_SHIFT));
  }
  // Remove this entry.
  void remove() {
    value_ |= IS_REMOVED_FLAG;
  }

 private:
  uint64_t value_;

  explicit TableEntry(uint64_t value) : value_(value) {}
};

struct TableSizeError {};

}  // namespace

template <typename T>
class HashTableImpl {
  using Header = ImplHeader;
  using Pool   = Pool<T>;

 public:
  using Key    = typename Map<T>::Key;
  using KeyArg = typename Map<T>::KeyArg;
  using Cursor = typename Map<T>::Cursor;

  HashTableImpl();
  ~HashTableImpl();

  static HashTableImpl *create(Storage *storage, uint32_t storage_node_id,
                               Pool *pool);
  static HashTableImpl *open(Storage *storage, uint32_t storage_node_id,
                             Pool *pool);

  static void unlink(Storage *storage, uint32_t storage_node_id) {
    storage->unlink_node(storage_node_id);
  }

  uint32_t storage_node_id() const {
    return storage_node_id_;
  }

  int64_t max_key_id() {
    return pool_->max_key_id();
  }
  uint64_t num_keys() {
    return pool_->num_keys();
  }

  bool get(int64_t key_id, Key *key = nullptr);
  bool unset(int64_t key_id);
  bool reset(int64_t key_id, KeyArg dest_key);

  bool find(KeyArg key, int64_t *key_id = nullptr);
  bool add(KeyArg key, int64_t *key_id = nullptr);
  bool remove(KeyArg key);
  bool replace(KeyArg src_key, KeyArg dest_key, int64_t *key_id = nullptr);

  void defrag();

  void truncate();

 private:
  Storage *storage_;
  uint32_t storage_node_id_;
  Header *header_;
  Pool *pool_;
  TableEntry *table_;
  uint64_t id_mask_;

  void create_impl(Storage *storage, uint32_t storage_node_id, Pool *pool);
  void open_impl(Storage *storage, uint32_t storage_node_id, Pool *pool);

  // Search a hash table for a key ID.
  // On success, return a pointer to a matched entry.
  // On failure, return nullptr.
  TableEntry *find_key_id(int64_t key_id);
  // Search a hash table for a key.
  // On success, assign a pointer to a matched entry to "*entry" and return
  // true.
  // On failure, assign a pointer to the first unused or removed entry to
  // "*entry" and return false.
  bool find_key(KeyArg key, uint64_t hash_value, TableEntry **entry);

  // Build a hash table.
  void build_table();

  // Move to the next entry.
  uint64_t rehash(uint64_t hash) const {
    return hash + 1;
  }

  // Return the table size threshold.
  uint64_t table_size_threshold() const {
    return ((header_->table_size + (header_->table_size / 4)) / 2);
  }
};

template <typename T>
HashTableImpl<T>::HashTableImpl()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      pool_(nullptr),
      table_(nullptr),
      id_mask_(0) {}

template <typename T>
HashTableImpl<T>::~HashTableImpl() {}

template <typename T>
HashTableImpl<T> *HashTableImpl<T>::create(Storage *storage,
                                           uint32_t storage_node_id,
                                           Pool *pool) {
  std::unique_ptr<HashTableImpl> map(new (std::nothrow) HashTableImpl);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::HashTable failed";
    throw MemoryError();
  }
  map->create_impl(storage, storage_node_id, pool);
  return map.release();
}

template <typename T>
HashTableImpl<T> *HashTableImpl<T>::open(Storage *storage,
                                         uint32_t storage_node_id,
                                         Pool *pool) {
  std::unique_ptr<HashTableImpl> map(new (std::nothrow) HashTableImpl);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::HashTable failed";
    throw MemoryError();
  }
  map->open_impl(storage, storage_node_id, pool);
  return map.release();
}

template <typename T>
bool HashTableImpl<T>::get(int64_t key_id, Key *key) {
  if ((key_id < MAP_MIN_KEY_ID) || (key_id > max_key_id())) {
    // Out of range.
    return false;
  }
  if (key) {
    return pool_->get(key_id, key);
  }
  return pool_->get_bit(key_id);
}

template <typename T>
bool HashTableImpl<T>::unset(int64_t key_id) {
  if ((key_id < MAP_MIN_KEY_ID) || (key_id > max_key_id())) {
    // Out of range.
    return false;
  }
  TableEntry * const entry = find_key_id(key_id);
  if (!entry) {
    // Not found.
    return false;
  }
  pool_->unset(key_id);
  entry->remove();
  return true;
}

template <typename T>
bool HashTableImpl<T>::reset(int64_t key_id, KeyArg dest_key) {
  if ((key_id < MAP_MIN_KEY_ID) || (key_id > max_key_id())) {
    // Out of range.
    return false;
  }
  TableEntry * const src_entry = find_key_id(key_id);
  if (!src_entry) {
    // Not found.
    return false;
  }
  const Key dest_normalized_key = Helper<T>::normalize(dest_key);
  const uint64_t dest_hash_value = Hash<T>()(dest_normalized_key);
  TableEntry *dest_entry;
  if (find_key(dest_normalized_key, dest_hash_value, &dest_entry)) {
    // Found.
    return false;
  }
  // Throw an exception if the filling rate of the current table is greater
  // than 62.5%.
  if (header_->num_entries > table_size_threshold()) {
    throw TableSizeError();
  }
  pool_->reset(key_id, dest_normalized_key);
  if (dest_entry->is_unused()) {
    ++header_->num_entries;
  }
  dest_entry->set(key_id, dest_hash_value);
  src_entry->remove();
  return true;
}

template <typename T>
bool HashTableImpl<T>::find(KeyArg key, int64_t *key_id) {
  const Key normalized_key = Helper<T>::normalize(key);
  const uint64_t hash_value = Hash<T>()(normalized_key);
  for (uint64_t id = hash_value; ; ) {
    const TableEntry entry = table_[id & id_mask_];
    if (entry.is_unused()) {
      // Not found.
      return false;
    } else if (!entry.is_removed()) {
      if (entry.test_hash_value(hash_value)) {
        Key stored_key = pool_->get_key(entry.key_id());
        if (Helper<T>::equal_to(stored_key, normalized_key)) {
          // Found.
          if (key_id) {
            *key_id = entry.key_id();
          }
          return true;
        }
      }
    }
    id = rehash(id);
    if (((id ^ hash_value) & id_mask_) == 0) {
      // Critical error.
      GRNXX_ERROR() << "endless loop";
      throw LogicError();
    }
  }
}

template <typename T>
bool HashTableImpl<T>::add(KeyArg key, int64_t *key_id) {
  // Throw an exception if the filling rate of the current table is greater
  // than 62.5%.
  if (header_->num_entries > table_size_threshold()) {
    throw TableSizeError();
  }
  const Key normalized_key = Helper<T>::normalize(key);
  const uint64_t hash_value = Hash<T>()(normalized_key);
  TableEntry *entry;
  if (find_key(normalized_key, hash_value, &entry)) {
    // Found.
    if (key_id) {
      *key_id = entry->key_id();
    }
    return false;
  }
  int64_t next_key_id = pool_->add(normalized_key);
  if (entry->is_unused()) {
    ++header_->num_entries;
  }
  entry->set(next_key_id, hash_value);
  if (key_id) {
    *key_id = next_key_id;
  }
  return true;
}

template <typename T>
bool HashTableImpl<T>::remove(KeyArg key) {
  const Key normalized_key = Helper<T>::normalize(key);
  const uint64_t hash_value = Hash<T>()(normalized_key);
  TableEntry *entry;
  if (!find_key(normalized_key, hash_value, &entry)) {
    // Not found.
    return false;
  }
  pool_->unset(entry->key_id());
  entry->remove();
  return true;
}

template <typename T>
bool HashTableImpl<T>::replace(KeyArg src_key, KeyArg dest_key,
                               int64_t *key_id) {
  const Key src_normalized_key = Helper<T>::normalize(src_key);
  const uint64_t src_hash_value = Hash<T>()(src_normalized_key);
  TableEntry *src_entry;
  if (!find_key(src_normalized_key, src_hash_value, &src_entry)) {
    // Not found.
    return false;
  }
  const Key dest_normalized_key = Helper<T>::normalize(dest_key);
  const uint64_t dest_hash_value = Hash<T>()(dest_normalized_key);
  TableEntry *dest_entry;
  if (find_key(dest_normalized_key, dest_hash_value, &dest_entry)) {
    // Found.
    return false;
  }
  // Throw an exception if the filling rate of the current table is greater
  // than 62.5%.
  if (header_->num_entries > table_size_threshold()) {
    throw TableSizeError();
  }
  pool_->reset(src_entry->key_id(), dest_normalized_key);
  if (dest_entry->is_unused()) {
    ++header_->num_entries;
  }
  dest_entry->set(src_entry->key_id(), dest_hash_value);
  src_entry->remove();
  if (key_id) {
    *key_id = dest_entry->key_id();
  }
  return true;
}

template <typename T>
void HashTableImpl<T>::create_impl(Storage *storage, uint32_t storage_node_id,
                                   Pool *pool) {
  storage_ = storage;
  StorageNode header_node =
      storage->create_node(storage_node_id, sizeof(Header));
  storage_node_id_ = header_node.id();
  try {
    header_ = static_cast<Header *>(header_node.body());
    *header_ = Header();
    pool_ = pool;
    build_table();
  } catch (...) {
    storage->unlink_node(storage_node_id_);
    throw;
  }
}

template <typename T>
void HashTableImpl<T>::open_impl(Storage *storage, uint32_t storage_node_id,
                                 Pool *pool) {
  storage_ = storage;
  StorageNode header_node = storage->open_node(storage_node_id);
  if (header_node.size() < sizeof(Header)) {
    GRNXX_ERROR() << "invalid format: size = " << header_node.size()
                  << ", header_size = " << sizeof(Header);
    throw LogicError();
  }
  storage_node_id_ = storage_node_id;
  header_ = static_cast<Header *>(header_node.body());
  pool_ = pool;
  StorageNode table_node = storage->open_node(header_->table_storage_node_id);
  table_ = static_cast<TableEntry *>(table_node.body());
  id_mask_ = header_->table_size - 1;
}

template <typename T>
TableEntry *HashTableImpl<T>::find_key_id(int64_t key_id) {
  Key stored_key;
  if (!pool_->get(key_id, &stored_key)) {
    // Not found.
    return nullptr;
  }
  const uint64_t hash_value = Hash<T>()(stored_key);
  for (uint64_t id = hash_value; ; ) {
    TableEntry &entry = table_[id & id_mask_];
    if (entry.key_id() == key_id) {
      // Found.
      return &entry;
    }
    id = rehash(id);
    if (((id ^ hash_value) & id_mask_) == 0) {
      // Critical error.
      GRNXX_ERROR() << "endless loop";
      throw LogicError();
    }
  }
}

template <typename T>
bool HashTableImpl<T>::find_key(KeyArg key, uint64_t hash_value,
                                TableEntry **entry) {
  *entry = nullptr;
  for (uint64_t id = hash_value; ; ) {
    TableEntry &this_entry = table_[id & id_mask_];
    if (this_entry.is_unused()) {
      // Not found.
      if (!*entry) {
        *entry = &this_entry;
      }
      return false;
    } else if (this_entry.is_removed()) {
      // Save the first removed entry.
      if (!*entry) {
        *entry = &this_entry;
      }
    } else if (this_entry.test_hash_value(hash_value)) {
      Key stored_key = pool_->get_key(this_entry.key_id());
      if (Helper<T>::equal_to(stored_key, key)) {
        // Found.
        *entry = &this_entry;
        return true;
      }
    }
    id = rehash(id);
    if (((id ^ hash_value) & id_mask_) == 0) {
      // Critical error.
      GRNXX_ERROR() << "endless loop";
      throw LogicError();
    }
  }
}

template <typename T>
void HashTableImpl<T>::build_table() {
  // Create a new table.
  uint64_t new_size = pool_->num_keys() * 2;
  if (new_size < MIN_TABLE_SIZE) {
    new_size = MIN_TABLE_SIZE;
  } else if (new_size > MAX_TABLE_SIZE) {
    new_size = MAX_TABLE_SIZE;
  }
  new_size = 2ULL << bit_scan_reverse(new_size - 1);
  StorageNode table_node =
      storage_->create_node(storage_node_id_, sizeof(TableEntry) * new_size);
  table_ = static_cast<TableEntry *>(table_node.body());
  id_mask_ = new_size - 1;
  for (uint64_t i = 0; i < new_size; ++i) {
    table_[i] = TableEntry::unused_entry();
  }
  // Add entries associated with keys in a pool.
  const uint64_t new_id_mask = new_size - 1;
  const int64_t max_key_id = pool_->max_key_id();
  for (int64_t key_id = MAP_MIN_KEY_ID; key_id <= max_key_id; ++key_id) {
    Key stored_key;
    if (!pool_->get(key_id, &stored_key)) {
      continue;
    }
    const uint64_t hash_value = Hash<T>()(stored_key);
    for (uint64_t id = hash_value; ; id = rehash(id)) {
      TableEntry &entry = table_[id & new_id_mask];
      if (entry.is_unused()) {
        entry.set(key_id, hash_value);
        break;
      }
    }
    ++header_->num_entries;
  }
  header_->table_size = new_size;
  header_->table_storage_node_id = table_node.id();
}

struct HashTableHeader {
  CommonHeader common_header;
  uint64_t pool_id;
  uint64_t impl_id;
  uint32_t pool_storage_node_id;
  uint32_t impl_storage_node_id;
  Mutex mutex;

  // Initialize the member variables.
  HashTableHeader();

  // Return true iff the header seems to be correct.
  explicit operator bool() const;
};

HashTableHeader::HashTableHeader()
    : common_header(FORMAT_STRING, MAP_HASH_TABLE),
      pool_id(0),
      impl_id(0),
      pool_storage_node_id(STORAGE_INVALID_NODE_ID),
      impl_storage_node_id(STORAGE_INVALID_NODE_ID),
      mutex() {}

HashTableHeader::operator bool() const {
  return common_header.format() == FORMAT_STRING;
}

template <typename T>
HashTable<T>::HashTable()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      pool_(),
      impl_(),
      queue_(),
      pool_id_(0),
      impl_id_(0),
      clock_() {}

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
int64_t HashTable<T>::max_key_id() {
  refresh_if_possible();
  return impl_->max_key_id();
}

template <typename T>
uint64_t HashTable<T>::num_keys() {
  refresh_if_possible();
  return impl_->num_keys();
}

template <typename T>
bool HashTable<T>::get(int64_t key_id, Key *key) {
  refresh_if_possible();
  return impl_->get(key_id, key);
}

template <typename T>
bool HashTable<T>::unset(int64_t key_id) {
  refresh_if_possible();
  return impl_->unset(key_id);
}

template <typename T>
bool HashTable<T>::reset(int64_t key_id, KeyArg dest_key) {
  refresh_if_possible();
  try {
    return impl_->reset(key_id, dest_key);
  } catch (TableSizeError) {
    rebuild_table();
    return impl_->reset(key_id, dest_key);
  }
}

template <typename T>
bool HashTable<T>::find(KeyArg key, int64_t *key_id) {
  refresh_if_possible();
  return impl_->find(key, key_id);
}

template <typename T>
bool HashTable<T>::add(KeyArg key, int64_t *key_id) {
  refresh_if_possible();
  try {
    return impl_->add(key, key_id);
  } catch (TableSizeError) {
    rebuild_table();
    return impl_->add(key, key_id);
  }
}

template <typename T>
bool HashTable<T>::remove(KeyArg key) {
  refresh_if_possible();
  return impl_->remove(key);
}

template <typename T>
bool HashTable<T>::replace(KeyArg src_key, KeyArg dest_key, int64_t *key_id) {
  refresh_if_possible();
  try {
    return impl_->replace(src_key, dest_key, key_id);
  } catch (TableSizeError) {
    rebuild_table();
    return impl_->replace(src_key, dest_key, key_id);
  }
}

template <typename T>
void HashTable<T>::rebuild_table() {
  std::unique_ptr<Impl> new_impl(
      Impl::create(storage_, storage_node_id_, pool_.get()));
  {
    // Validate a new impl.
    Lock lock(&header_->mutex);
    header_->impl_storage_node_id = new_impl->storage_node_id();
    ++header_->impl_id;
    impl_.swap(new_impl);
    impl_id_ = header_->impl_id;
  }
  Impl::unlink(storage_, new_impl->storage_node_id());
  try {
    queue_.push(QueueEntry{ nullptr, std::move(new_impl), clock_.now() });
  } catch (const std::exception &exception) {
    GRNXX_ERROR() << "std::queue::push() failed";
    throw StandardError(exception);
  }
}

template <typename T>
void HashTable<T>::defrag() {
  refresh_if_possible();
  pool_->defrag();
  rebuild_table();
}

template <typename T>
void HashTable<T>::truncate() {
  refresh_if_possible();
  std::unique_ptr<Pool> new_pool(Pool::create(storage_, storage_node_id_));
  std::unique_ptr<Impl> new_impl;
  try {
    new_impl.reset(Impl::create(storage_, storage_node_id_, new_pool.get()));
  } catch (...) {
    Pool::unlink(storage_, new_pool->storage_node_id());
    throw;
  }
  {
    // Validate a new impl and a new pool.
    Lock lock(&header_->mutex);
    header_->pool_storage_node_id = new_pool->storage_node_id();
    header_->impl_storage_node_id = new_impl->storage_node_id();
    ++header_->pool_id;
    ++header_->impl_id;
    pool_.swap(new_pool);
    impl_.swap(new_impl);
    pool_id_ = header_->pool_id;
    impl_id_ = header_->impl_id;
  }
  Pool::unlink(storage_, new_pool->storage_node_id());
  Impl::unlink(storage_, new_impl->storage_node_id());
  try {
    queue_.push(QueueEntry{ std::move(new_pool), std::move(new_impl),
                            clock_.now() });
  } catch (const std::exception &exception) {
    GRNXX_ERROR() << "std::queue::push() failed";
    throw StandardError(exception);
  }
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
    pool_.reset(Pool::create(storage, storage_node_id_));
    impl_.reset(Impl::create(storage, storage_node_id_, pool_.get()));
    header_->pool_storage_node_id = pool_->storage_node_id();
    header_->impl_storage_node_id = impl_->storage_node_id();
    pool_id_ = ++header_->pool_id;
    impl_id_ = ++header_->impl_id;
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
}

template <typename T>
void HashTable<T>::refresh_if_possible() {
  if (impl_id_ != header_->impl_id) {
    refresh();
  }
}

template <typename T>
void HashTable<T>::refresh() {
  Lock lock(&header_->mutex);
  if (pool_id_ != header_->pool_id) {
    refresh_pool();
  }
  if (impl_id_ != header_->impl_id) {
    refresh_impl();
  }
}

template <typename T>
void HashTable<T>::refresh_pool() {
  std::unique_ptr<Pool> new_pool(
      Pool::open(storage_, header_->pool_storage_node_id));
  pool_.swap(new_pool);
  pool_id_ = header_->pool_id;
  try {
    queue_.push(QueueEntry{ std::move(new_pool), nullptr, clock_.now() });
  } catch (const std::exception &exception) {
    GRNXX_ERROR() << "std::queue::push() failed";
    throw StandardError(exception);
  }
}

template <typename T>
void HashTable<T>::refresh_impl() {
  std::unique_ptr<Impl> new_impl(
      Impl::open(storage_, header_->impl_storage_node_id, pool_.get()));
  impl_.swap(new_impl);
  impl_id_ = header_->impl_id;
  try {
    queue_.push(QueueEntry{ nullptr, std::move(new_impl), clock_.now() });
  } catch (const std::exception &exception) {
    GRNXX_ERROR() << "std::queue::push() failed";
    throw StandardError(exception);
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
