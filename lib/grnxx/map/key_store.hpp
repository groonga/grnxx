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
#ifndef GRNXX_MAP_KEY_STORE_HPP
#define GRNXX_MAP_KEY_STORE_HPP

#include "grnxx/features.hpp"

#include <memory>

#include "grnxx/array.hpp"
#include "grnxx/bytes.hpp"
#include "grnxx/map/bytes_array.hpp"
#include "grnxx/traits.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Storage;

namespace map {

struct KeyStoreHeader {
  int64_t max_key_id;
  uint64_t num_keys;
  uint64_t latest_link;
  uint32_t keys_storage_node_id;
  uint32_t bits_storage_node_id;
  uint32_t links_storage_node_id;

  KeyStoreHeader();
};

// Change the size of arrays based on "T".
// Note that the size of link array is N/64 where N is the size of BitArray.
template <typename T, size_t T_SIZE = sizeof(T)>
struct KeyStoreHelper;

// Map<T> has at most 2^40 different keys.
template <typename T, size_t T_SIZE>
struct KeyStoreHelper {
  using KeyArray = Array<T, 65536, 4096>;
  using BitArray = Array<bool, 65536, 4096>;
  using LinkArray = Array<uint64_t, 16384, 1024>;

  static constexpr uint64_t KEY_ARRAY_SIZE  = 1ULL << 40;
  static constexpr uint64_t BIT_ARRAY_SIZE  = 1ULL << 40;
  static constexpr uint64_t LINK_ARRAY_SIZE = 1ULL << 34;
};

// Map<T> has at most 2^8 different keys.
template <typename T>
struct KeyStoreHelper<T, 1> {
  using KeyArray = Array<T>;
  using BitArray = Array<bool>;
  using LinkArray = Array<uint64_t>;

  static constexpr uint64_t KEY_ARRAY_SIZE  = 1ULL << 8;
  static constexpr uint64_t BIT_ARRAY_SIZE  = 1ULL << 8;
  static constexpr uint64_t LINK_ARRAY_SIZE = 1ULL << 2;
};

// Map<T> has at most 2^16 different keys.
template <typename T>
struct KeyStoreHelper<T, 2> {
  using KeyArray = Array<T, 256>;
  using BitArray = Array<bool, 256>;
  using LinkArray = Array<uint64_t>;

  static constexpr uint64_t KEY_ARRAY_SIZE  = 1ULL << 16;
  static constexpr uint64_t BIT_ARRAY_SIZE  = 1ULL << 16;
  static constexpr uint64_t LINK_ARRAY_SIZE = 1ULL << 10;
};

// Map<T> has at most 2^32 different keys.
template <typename T>
struct KeyStoreHelper<T, 4> {
  using KeyArray = Array<T, 65536, 256>;
  using BitArray = Array<bool, 16384, 512>;
  using LinkArray = Array<uint64_t, 4096, 128>;

  static constexpr uint64_t KEY_ARRAY_SIZE  = 1ULL << 32;
  static constexpr uint64_t BIT_ARRAY_SIZE  = 1ULL << 32;
  static constexpr uint64_t LINK_ARRAY_SIZE = 1ULL << 26;
};

// Map<T> has at most 2^40 different keys.
template <>
struct KeyStoreHelper<Bytes> {
  using KeyArray = BytesArray;
  using BitArray = Array<bool, 65536, 4096>;
  using LinkArray = Array<uint64_t, 16384, 1024>;

  static constexpr uint64_t KEY_ARRAY_SIZE  = 1ULL << 40;
  static constexpr uint64_t BIT_ARRAY_SIZE  = 1ULL << 40;
  static constexpr uint64_t LINK_ARRAY_SIZE = 1ULL << 34;
};

template <typename T>
class KeyStore {
  using Header = KeyStoreHeader;
  using KeyArray = typename KeyStoreHelper<T>::KeyArray;
  using BitArray = typename KeyStoreHelper<T>::BitArray;
  using BitArrayUnit = typename BitArray::Unit;
  using LinkArray = typename KeyStoreHelper<T>::LinkArray;

 public:
  using Key = typename Traits<T>::Type;
  using KeyArg = typename Traits<T>::ArgumentType;

  KeyStore();
  ~KeyStore();

  static KeyStore *create(Storage *storage, uint32_t storage_node_id);
  static KeyStore *open(Storage *storage, uint32_t storage_node_id);

  uint32_t storage_node_id() const {
    return storage_node_id_;
  }

  int64_t max_key_id() const {
    return header_->max_key_id;
  }
  uint64_t num_keys() const {
    return header_->num_keys;
  }

  Key get_key(int64_t key_id) {
    return keys_->get(key_id);
  }
  bool get_bit(int64_t key_id) {
    return bits_->get(key_id);
  }
  bool unset(int64_t key_id);
  void reset(int64_t key_id, KeyArg dest_key) {
    keys_->set(key_id, dest_key);
  }

  int64_t add(KeyArg key);

  bool truncate();

 private:
  Storage *storage_;
  uint32_t storage_node_id_;
  Header *header_;
  std::unique_ptr<KeyArray> keys_;
  std::unique_ptr<BitArray> bits_;
  std::unique_ptr<LinkArray> links_;

  void create_store(Storage *storage, uint32_t storage_node_id);
  void open_store(Storage *storage, uint32_t storage_node_id);

  static constexpr uint64_t unit_size() {
    return sizeof(typename BitArray::Unit) * 8;
  }
};

}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_KEY_STORE_HPP
