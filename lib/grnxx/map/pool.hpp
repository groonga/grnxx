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
#ifndef GRNXX_MAP_POOL_HPP
#define GRNXX_MAP_POOL_HPP

#include "grnxx/features.hpp"

#include <memory>
#include <queue>

#include "grnxx/bytes.hpp"
#include "grnxx/duration.hpp"
#include "grnxx/mutex.hpp"
#include "grnxx/traits.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Storage;

namespace map {

// The minimum key ID.
constexpr int64_t POOL_MIN_KEY_ID = 0;
// The maximum key ID.
constexpr int64_t POOL_MAX_KEY_ID = (1LL << 40) - 2;

template <typename T>
struct PoolHeader {
  int64_t max_key_id;
  uint64_t num_keys;
  uint64_t size;
  uint64_t latest_available_unit_id;
  union {
    uint32_t page_storage_node_id;
    uint32_t table_storage_node_id;
  };
  Mutex mutex;

  PoolHeader();
};

struct PoolUnit {
  uint64_t validity_bits;
  uint64_t next_available_unit_id;
};

template <typename T>
class Pool {
  using Header = PoolHeader<T>;
  using Unit   = PoolUnit;

  static constexpr int64_t  MIN_KEY_ID     = POOL_MIN_KEY_ID;
  static constexpr int64_t  MAX_KEY_ID     = POOL_MAX_KEY_ID;

  static constexpr uint64_t UNIT_SIZE      = 64;
  static constexpr uint64_t PAGE_SIZE      = 1ULL << 16;

  static constexpr uint64_t MIN_PAGE_SIZE  = UNIT_SIZE;
  static constexpr uint64_t MIN_TABLE_SIZE = 1ULL << 10;

 public:
  using Key = typename Traits<T>::Type;
  using KeyArg = typename Traits<T>::ArgumentType;

  Pool();
  ~Pool();

  static Pool *create(Storage *storage, uint32_t storage_node_id);
  static Pool *open(Storage *storage, uint32_t storage_node_id);

  static void unlink(Storage *storage, uint32_t storage_node_id);

  uint32_t storage_node_id() const {
    return storage_node_id_;
  }

  static constexpr int64_t min_key_id() {
    return MIN_KEY_ID;
  }
  int64_t max_key_id() const {
    return header_->max_key_id;
  }
  uint64_t num_keys() const {
    return header_->num_keys;
  }

  bool get(int64_t key_id, Key *key) {
    refresh_if_possible();
    const void * const page = get_page(key_id / PAGE_SIZE);
    const uint64_t local_key_id = key_id % PAGE_SIZE;
    const Unit * const unit =
        static_cast<const Unit *>(page) - (local_key_id / UNIT_SIZE) - 1;
    if (~unit->validity_bits & (1ULL << (local_key_id % UNIT_SIZE))) {
      // Not found.
      return false;
    }
    *key = static_cast<const T *>(page)[local_key_id];
    return true;
  }

  Key get_key(int64_t key_id) {
    refresh_if_possible();
    const void * const page = get_page(key_id / PAGE_SIZE);
    return static_cast<const T *>(page)[key_id % PAGE_SIZE];
  }

  bool get_bit(int64_t key_id) {
    refresh_if_possible();
    const void * const page = get_page(key_id / PAGE_SIZE);
    const uint64_t local_key_id = key_id % PAGE_SIZE;
    const Unit * const unit =
        static_cast<const Unit *>(page) - (local_key_id / UNIT_SIZE) - 1;
    return unit->validity_bits & (1ULL << (local_key_id % UNIT_SIZE));
  }

  void unset(int64_t key_id);
  void reset(int64_t key_id, KeyArg dest_key);

  int64_t add(KeyArg key);

  void defrag();
  void sweep(Duration lifetime);

 private:
  Storage *storage_;
  uint32_t storage_node_id_;
  Header *header_;
  std::unique_ptr<void *[]> pages_;
  uint32_t *table_;
  uint64_t size_;
  // TODO: Time must be added.
  std::queue<std::unique_ptr<void *[]>> queue_;

  void create_pool(Storage *storage, uint32_t storage_node_id);
  void open_pool(Storage *storage, uint32_t storage_node_id);

  void *get_page(uint64_t page_id) {
    return pages_[page_id] ? pages_[page_id] : open_page(page_id);
  }
  void *open_page(uint64_t page_id);

  void reserve_key_id(int64_t key_id);

  void expand();
  void expand_page();
  void expand_table();

  void refresh_if_possible() {
    if (size_ != header_->size) {
      refresh();
    }
  }
  void refresh();
  void refresh_page();
  void refresh_table();
};

template <>
struct PoolHeader<Bytes> {
  uint64_t size;
  uint64_t next_offset;
  union {
    uint32_t page_storage_node_id;
    uint32_t table_storage_node_id;
  };
  uint32_t index_pool_storage_node_id;
  Mutex mutex;

  PoolHeader();
};

struct PoolTableEntry {
  uint32_t page_storage_node_id;
  uint32_t size_in_use;

  PoolTableEntry();
};

template <>
class Pool<Bytes> {
  using Header     = PoolHeader<Bytes>;
  using IndexPool  = Pool<uint64_t>;
  using TableEntry = PoolTableEntry;

  static constexpr int64_t  MIN_KEY_ID     = POOL_MIN_KEY_ID;
  static constexpr int64_t  MAX_KEY_ID     = POOL_MAX_KEY_ID;
  static constexpr uint64_t MAX_KEY_SIZE   = 4096;

  static constexpr uint64_t PAGE_SIZE      = 1ULL << 20;

  static constexpr uint64_t MIN_PAGE_SIZE  = 64;
  static constexpr uint64_t MIN_TABLE_SIZE = 1ULL << 10;

  static constexpr uint64_t BYTES_ID_SIZE_MASK = (1ULL << 13) - 1;

  static constexpr uint64_t EMPTY_BYTES_ID = 0;

  static constexpr double   USAGE_RATE_THRESHOLD = 0.5;

 public:
  using Key = typename Traits<Bytes>::Type;
  using KeyArg = typename Traits<Bytes>::ArgumentType;

  Pool();
  ~Pool();

  static Pool *create(Storage *storage, uint32_t storage_node_id);
  static Pool *open(Storage *storage, uint32_t storage_node_id);

  static void unlink(Storage *storage, uint32_t storage_node_id);

  uint32_t storage_node_id() const {
    return storage_node_id_;
  }

  static constexpr int64_t min_key_id() {
    return IndexPool::min_key_id();
  }
  int64_t max_key_id() const {
    return index_pool_->max_key_id();
  }
  uint64_t num_keys() const {
    return index_pool_->num_keys();
  }

  bool get(int64_t key_id, Key *key) {
    uint64_t bytes_id;
    if (!index_pool_->get(key_id, &bytes_id)) {
      // Not found.
      return false;
    }
    *key = get_bytes(bytes_id);
    return true;
  }

  Key get_key(int64_t key_id) {
    return get_bytes(index_pool_->get_key(key_id));
  }

  bool get_bit(int64_t key_id) {
    return index_pool_->get_bit(key_id);
  }

  void unset(int64_t key_id);
  void reset(int64_t key_id, KeyArg dest_key);

  int64_t add(KeyArg key);

  void defrag();
  void sweep(Duration lifetime);

 private:
  Storage *storage_;
  uint32_t storage_node_id_;
  Header *header_;
  std::unique_ptr<IndexPool> index_pool_;
  std::unique_ptr<uint8_t *[]> pages_;
  TableEntry *table_;
  uint64_t size_;
  // TODO: Time must be added.
  std::queue<std::unique_ptr<uint8_t *[]>> queue_;

  void create_pool(Storage *storage, uint32_t storage_node_id);
  void open_pool(Storage *storage, uint32_t storage_node_id);

  Key get_bytes(uint64_t bytes_id) {
    if (bytes_id == EMPTY_BYTES_ID) {
      return Bytes("", 0);
    }
    refresh_if_possible();
    const uint64_t offset = get_offset(bytes_id);
    const uint32_t page_id = static_cast<uint32_t>(offset / PAGE_SIZE);
    return Bytes(get_page(page_id) + (offset % PAGE_SIZE), get_size(bytes_id));
  }
  void unset_bytes(uint64_t bytes_id);
  uint64_t add_bytes(KeyArg bytes);

  uint8_t *get_page(uint32_t page_id) {
    return pages_[page_id] ? pages_[page_id] : open_page(page_id);
  }
  uint8_t *open_page(uint32_t page_id);

  uint64_t reserve_space(uint32_t size);

  static uint64_t get_bytes_id(uint64_t offset, uint32_t size) {
    return (offset * (BYTES_ID_SIZE_MASK + 1)) | size;
  }
  static uint64_t get_offset(uint64_t bytes_id) {
    return bytes_id / (BYTES_ID_SIZE_MASK + 1);
  }
  static uint32_t get_size(uint64_t bytes_id) {
    return static_cast<uint32_t>(bytes_id & BYTES_ID_SIZE_MASK);
  }

  void expand(uint32_t additional_size);
  void expand_page(uint32_t additional_size);
  void expand_table(uint32_t additional_size);

  void refresh_if_possible() {
    if (size_ != header_->size) {
      refresh();
    }
  }
  void refresh();
  void refresh_page();
  void refresh_table();
};

}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_POOL_HPP
