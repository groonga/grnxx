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
#ifndef GRNXX_MAP_ARRAY_MAP_HPP
#define GRNXX_MAP_ARRAY_MAP_HPP

#include "grnxx/map.hpp"
#include "grnxx/array.hpp"

namespace grnxx {
namespace map {

class Storage;

struct ArrayMapHeader {
  MapType map_type;
  uint32_t bits_storage_node_id;
  uint32_t keys_storage_node_id;
  int64_t max_key_id;
  int64_t next_key_id;
  uint64_t num_keys;

  ArrayMapHeader();
};

template <typename T>
class ArrayMap : public Map<T> {
 public:
  using Key = typename Map<T>::Key;
  using KeyArg = typename Map<T>::KeyArg;
  using Cursor = typename Map<T>::Cursor;

  ArrayMap();
  ~ArrayMap();

  static ArrayMap *create(Storage *storage, uint32_t storage_node_id,
                          const MapOptions &options = MapOptions());
  static ArrayMap *open(Storage *storage, uint32_t storage_node_id);

  static bool unlink(Storage *storage, uint32_t storage_node_id);

  uint32_t storage_node_id() const;
  MapType type() const;

  int64_t max_key_id() const;
  int64_t next_key_id() const;
  uint64_t num_keys() const;

  bool get(int64_t key_id, Key *key = nullptr);
//  bool get_next(int64_t key_id, int64_t *next_key_id = nullptr,
//                        Key *next_key = nullptr);
//  bool unset(int64_t key_id);
//  bool reset(int64_t key_id, KeyArg dest_key);

  bool find(KeyArg key, int64_t *key_id = nullptr);
  bool add(KeyArg key, int64_t *key_id = nullptr);
  bool remove(KeyArg key);
  bool replace(KeyArg src_key, KeyArg dest_key,
                      int64_t *key_id = nullptr);

  bool truncate();

 private:
  Storage *storage_;
  uint32_t storage_node_id_;
  ArrayMapHeader *header_;
  Array<uint32_t> bits_;
  Array<T> keys_;

  bool get_bit(int64_t key_id) {
    return bits_[key_id / 32] & (1U << (key_id % 32));
  }
  void set_bit(int64_t key_id, bool bit) {
    if (bit) {
      bits_[key_id / 32] |= 1U << (key_id % 32);
    } else {
      bits_[key_id / 32] &= ~(1U << (key_id % 32));
    }
  }
};

}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_ARRAY_MAP_HPP
