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
#ifndef GRNXX_MAP_KEY_POOL_HPP
#define GRNXX_MAP_KEY_POOL_HPP

#include "grnxx/features.hpp"

#include <memory>

#include "grnxx/array.hpp"
#include "grnxx/bytes.hpp"
#include "grnxx/map/bytes_pool.hpp"
#include "grnxx/traits.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Storage;

namespace map {

class BytesPool;

struct KeyPoolHeader;

// Change the size of arrays based on "T".
// Note that the size of link array is N/64 where N is the size of KeyArray.
template <typename T, size_t T_SIZE = sizeof(T)>
struct KeyPoolHelper;

// Map<T> has at most 2^40 different keys.
template <typename T, size_t T_SIZE>
struct KeyPoolHelper {
  using KeyArray = Array<T, (1 << 16), (1 << 12)>;
  using BitArray = Array<bool, (1 << 18), (1 << 11)>;
  using Link = uint64_t;
  using LinkArray = Array<Link, (1 << 14), (1 << 10)>;

  static constexpr uint64_t KEY_ARRAY_SIZE  = 1ULL << 40;
  static constexpr uint64_t BIT_ARRAY_SIZE  = 1ULL << 40;
  static constexpr uint64_t LINK_ARRAY_SIZE = 1ULL << 34;
};

// Map<T> has at most 2^8 different keys.
template <typename T>
struct KeyPoolHelper<T, 1> {
  using KeyArray = Array<T>;
  using BitArray = Array<bool>;
  using Link = uint8_t;
  using LinkArray = Array<Link>;

  static constexpr uint64_t KEY_ARRAY_SIZE  = 1ULL << 8;
  static constexpr uint64_t BIT_ARRAY_SIZE  = 1ULL << 8;
  static constexpr uint64_t LINK_ARRAY_SIZE = 1ULL << 2;
};

// Map<T> has at most 2^16 different keys.
template <typename T>
struct KeyPoolHelper<T, 2> {
  using KeyArray = Array<T>;
  using BitArray = Array<bool>;
  using Link = uint16_t;
  using LinkArray = Array<Link>;

  static constexpr uint64_t KEY_ARRAY_SIZE  = 1ULL << 16;
  static constexpr uint64_t BIT_ARRAY_SIZE  = 1ULL << 16;
  static constexpr uint64_t LINK_ARRAY_SIZE = 1ULL << 10;
};

// Map<T> has at most 2^32 different keys.
template <typename T>
struct KeyPoolHelper<T, 4> {
  using KeyArray = Array<T, (1 << 18)>;
  using BitArray = Array<bool, (1 << 20)>;
  using Link = uint32_t;
  using LinkArray = Array<Link, (1 << 15)>;

  static constexpr uint64_t KEY_ARRAY_SIZE  = 1ULL << 32;
  static constexpr uint64_t BIT_ARRAY_SIZE  = 1ULL << 32;
  static constexpr uint64_t LINK_ARRAY_SIZE = 1ULL << 26;
};

template <typename T>
class KeyPool {
  using Header = KeyPoolHeader;
  using KeyArray = typename KeyPoolHelper<T>::KeyArray;
  using BitArray = typename KeyPoolHelper<T>::BitArray;
  using BitArrayUnit = typename BitArray::Unit;
  using Link = typename KeyPoolHelper<T>::Link;
  using LinkArray = typename KeyPoolHelper<T>::LinkArray;

  static constexpr uint64_t UNIT_SIZE = sizeof(BitArrayUnit) * 8;

 public:
  using Key = typename Traits<T>::Type;
  using KeyArg = typename Traits<T>::ArgumentType;

  KeyPool();
  ~KeyPool();

  // Create a pool.
  static KeyPool *create(Storage *storage, uint32_t storage_node_id);
  // Open a pool.
  static KeyPool *open(Storage *storage, uint32_t storage_node_id);

  // Return the storage node ID.
  uint32_t storage_node_id() const {
    return storage_node_id_;
  }

  // Return the maximum key ID ever used.
  // The return value can be a negative value iff the map is empty.
  int64_t max_key_id() const {
    return *max_key_id_;
  }
  // Return the number of keys.
  uint64_t num_keys() const {
    return *num_keys_;
  }

  // Get a key associated with "key_id" and return true iff it exists.
  // Assign the found key to "*key" iff "key" != nullptr.
  bool get(int64_t key_id, Key *key) {
    if (!get_bit(key_id)) {
      return false;
    }
    if (key) {
      *key = get_key(key_id);
    }
    return true;
  }
  // Get a key associated with "key_id" without check.
  Key get_key(int64_t key_id) {
    return keys_->get(key_id);
  }
  // Return true iff "key_id" is valid.
  bool get_bit(int64_t key_id) {
    return bits_->get(key_id);
  }
  // Remove a key associated with "key_id".
  void unset(int64_t key_id);
  // Replace a key associated with "key_id" with "dest_key".
  void reset(int64_t key_id, KeyArg dest_key);

  // Add "key" and return its ID.
  int64_t add(KeyArg key);

  // Remove all the keys.
  void truncate();

 private:
  Storage *storage_;
  uint32_t storage_node_id_;
  Header *header_;
  const int64_t *max_key_id_;
  const uint64_t *num_keys_;
  std::unique_ptr<KeyArray> keys_;
  std::unique_ptr<BitArray> bits_;
  std::unique_ptr<LinkArray> links_;

  // Create a pool.
  void create_pool(Storage *storage, uint32_t storage_node_id);
  // Open a pool.
  void open_pool(Storage *storage, uint32_t storage_node_id);
};

class KeyPoolEntry {
  static constexpr uint64_t IS_VALID_FLAG = 1ULL << 63;

 public:
  KeyPoolEntry() = default;

  // Return true iff the entry is valid.
  explicit operator bool() const {
    return value_ & IS_VALID_FLAG;
  }

  // Return the ID of the associated byte sequence.
  uint64_t bytes_id() const {
    return value_ & ~IS_VALID_FLAG;
  }
  // Return the ID of the next invalid entry.
  uint64_t next_free_entry_id() const {
    return value_;
  }

  // Set the ID of the associated byte sequence.
  void set_bytes_id(uint64_t bytes_id) {
    value_ = IS_VALID_FLAG | bytes_id;
  }
  // Set the ID of the next free entry.
  void set_next_free_entry_id(uint64_t next_free_entry_id) {
    value_ = next_free_entry_id;
  }

 private:
  uint64_t value_;

  explicit KeyPoolEntry(uint64_t value) : value_(value) {}
};

template <>
class KeyPool<Bytes> {
  using Header = KeyPoolHeader;
  using Entry = KeyPoolEntry;
  using EntryArray = Array<Entry, (1 << 16), (1 << 12)>;

  static constexpr uint64_t ENTRY_ARRAY_SIZE = 1ULL << 40;

 public:
  using Key = typename Traits<Bytes>::Type;
  using KeyArg = typename Traits<Bytes>::ArgumentType;

  KeyPool();
  ~KeyPool();

  // Create a pool.
  static KeyPool *create(Storage *storage, uint32_t storage_node_id);
  // Open a pool.
  static KeyPool *open(Storage *storage, uint32_t storage_node_id);

  // Return the storage node ID.
  uint32_t storage_node_id() const {
    return storage_node_id_;
  }

  // Return the maximum key ID ever used.
  // The return value can be a negative value iff the map is empty.
  int64_t max_key_id() const {
    return *max_key_id_;
  }
  // Return the number of keys.
  uint64_t num_keys() const {
    return *num_keys_;
  }

  // Get a key associated with "key_id" and return true iff it exists.
  // Assign the found key to "*key" iff "key" != nullptr.
  bool get(int64_t key_id, Key *key) {
    const Entry entry = entries_->get(key_id);
    if (!entry) {
      return false;
    }
    if (key) {
      *key = pool_->get(entry.bytes_id());
    }
    return true;
  }
  // Get a key associated with "key_id" without check.
  Key get_key(int64_t key_id) {
    return pool_->get(entries_->get(key_id).bytes_id());
  }
  // Return true iff "key_id" is valid.
  bool get_bit(int64_t key_id) {
    return bool(entries_->get(key_id));
  }
  // Remove a key associated with "key_id".
  void unset(int64_t key_id);
  // Replace a key associated with "key_id" with "dest_key".
  void reset(int64_t key_id, KeyArg dest_key);

  // Add "key" and return its ID.
  int64_t add(KeyArg key);

  // Remove all the keys.
  void truncate();

 private:
  Storage *storage_;
  uint32_t storage_node_id_;
  Header *header_;
  const int64_t *max_key_id_;
  const uint64_t *num_keys_;
  std::unique_ptr<BytesPool> pool_;
  std::unique_ptr<EntryArray> entries_;

  // Create a pool.
  void create_pool(Storage *storage, uint32_t storage_node_id);
  // Open a pool.
  void open_pool(Storage *storage, uint32_t storage_node_id);
};

}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_KEY_POOL_HPP
