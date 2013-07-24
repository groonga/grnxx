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

#include "grnxx/features.hpp"

#include <memory>

#include "grnxx/map.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Storage;

namespace map {

template <typename T> class KeyPool;

struct ArrayMapHeader;

template <typename T>
class ArrayMap : public Map<T> {
  using Header = ArrayMapHeader;

 public:
  using Key = typename Map<T>::Key;
  using KeyArg = typename Map<T>::KeyArg;
  using Cursor = typename Map<T>::Cursor;

  ArrayMap();
  ~ArrayMap();

  static ArrayMap *create(Storage *storage, uint32_t storage_node_id,
                          const MapOptions &options = MapOptions());
  static ArrayMap *open(Storage *storage, uint32_t storage_node_id);

  uint32_t storage_node_id() const;
  MapType type() const;

  int64_t max_key_id() const;
  uint64_t num_keys() const;

  bool get(int64_t key_id, Key *key = nullptr);
  bool unset(int64_t key_id);
  bool reset(int64_t key_id, KeyArg dest_key);

  bool find(KeyArg key, int64_t *key_id = nullptr);
  bool add(KeyArg key, int64_t *key_id = nullptr);
  bool remove(KeyArg key);
  bool replace(KeyArg src_key, KeyArg dest_key, int64_t *key_id = nullptr);

  bool truncate();

 private:
  Storage *storage_;
  uint32_t storage_node_id_;
  Header *header_;
  std::unique_ptr<KeyPool<T>> pool_;

  void create_map(Storage *storage, uint32_t storage_node_id,
                  const MapOptions &options);
  void open_map(Storage *storage, uint32_t storage_node_id);
};

}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_ARRAY_MAP_HPP
