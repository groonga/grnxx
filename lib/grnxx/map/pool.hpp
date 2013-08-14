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
    const void * const page = get_page(key_id);
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
    const void * const page = get_page(key_id);
    return static_cast<const T *>(page)[key_id % PAGE_SIZE];
  }

  bool get_bit(int64_t key_id) {
    const void * const page = get_page(key_id);
    const uint64_t local_key_id = key_id % PAGE_SIZE;
    const Unit * const unit =
        static_cast<const Unit *>(page) - (local_key_id / UNIT_SIZE) - 1;
    return unit->validity_bits & (1ULL << (local_key_id % UNIT_SIZE));
  }

  void unset(int64_t key_id);
  void reset(int64_t key_id, KeyArg dest_key);

  int64_t add(KeyArg key);

  void defrag();

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

  void *get_page(int64_t key_id) {
    if (size_ != header_->size) {
      return open_page(key_id);
    }
    void * const page = pages_[key_id / PAGE_SIZE];
    if (!page) {
      return open_page(key_id);
    }
    return page;
  }
  void *open_page(int64_t key_id);

  void *reserve_page(int64_t key_id);

  void expand_pool();
  void refresh_pool();
};

}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_POOL_HPP
