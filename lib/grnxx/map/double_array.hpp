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
#ifndef GRNXX_MAP_DOUBLE_ARRAY_HPP
#define GRNXX_MAP_DOUBLE_ARRAY_HPP

#include "grnxx/features.hpp"

#include <memory>
#include <queue>

#include "grnxx/bytes.hpp"
#include "grnxx/duration.hpp"
#include "grnxx/map.hpp"
#include "grnxx/map_cursor.hpp"
#include "grnxx/map_cursor_query.hpp"
#include "grnxx/periodic_clock.hpp"
#include "grnxx/time.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Storage;

namespace map {

template <typename T> class Pool;

class DoubleArrayImpl;

struct DoubleArrayHeader;

template <typename T>
class DoubleArray {
 public:
  static Map<T> *create(Storage *storage, uint32_t storage_node_id,
                        const MapOptions &options = MapOptions());
  static Map<T> *open(Storage *storage, uint32_t storage_node_id);
};

template <>
class DoubleArray<Bytes> : public Map<Bytes> {
  using Header = DoubleArrayHeader;
  using Impl   = DoubleArrayImpl;
  using Pool   = map::Pool<Bytes>;

  struct QueueEntry {
    std::unique_ptr<Pool> pool;
    std::unique_ptr<Impl> impl;
    Time time;
  };

 public:
  using Key    = typename Map<Bytes>::Key;
  using KeyArg = typename Map<Bytes>::KeyArg;
  using Cursor = typename Map<Bytes>::Cursor;

  DoubleArray();
  ~DoubleArray();

  static DoubleArray *create(Storage *storage, uint32_t storage_node_id,
                             const MapOptions &options = MapOptions());
  static DoubleArray *open(Storage *storage, uint32_t storage_node_id);

  uint32_t storage_node_id() const;
  MapType type() const;

  int64_t max_key_id();
  uint64_t num_keys();

  bool get(int64_t key_id, Key *key = nullptr);
  bool unset(int64_t key_id);
  bool reset(int64_t key_id, KeyArg dest_key);

  bool find(KeyArg key, int64_t *key_id = nullptr);
  bool add(KeyArg key, int64_t *key_id = nullptr);
  bool remove(KeyArg key);
  bool replace(KeyArg src_key, KeyArg dest_key, int64_t *key_id = nullptr);

  void defrag();
  void sweep(Duration lifetime);

  void truncate();

  bool find_longest_prefix_match(KeyArg query,
                                 int64_t *key_id = nullptr,
                                 Key *key = nullptr);

//  Cursor *create_cursor(
//      MapCursorAllKeys<Bytes> query,
//      const MapCursorOptions &options = MapCursorOptions());
//  Cursor *create_cursor(
//      const MapCursorKeyIDRange<Bytes> &query,
//      const MapCursorOptions &options = MapCursorOptions());
//  Cursor *create_cursor(
//      const MapCursorKeyRange<Bytes> &query,
//      const MapCursorOptions &options = MapCursorOptions());

 private:
  Storage *storage_;
  uint32_t storage_node_id_;
  Header *header_;
  std::unique_ptr<Pool> pool_;
  std::unique_ptr<Impl> impl_;
  std::queue<QueueEntry> queue_;
  uint64_t pool_id_;
  uint64_t impl_id_;
  PeriodicClock clock_;

  void create_map(Storage *storage, uint32_t storage_node_id,
                  const MapOptions &options);
  void open_map(Storage *storage, uint32_t storage_node_id);

  inline void refresh_if_possible();
  void refresh();
  void refresh_pool();
  void refresh_impl();
};

}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_DOUBLE_ARRAY_HPP
