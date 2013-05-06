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
#ifndef GRNXX_ALPHA_MAP_HPP
#define GRNXX_ALPHA_MAP_HPP

#include "grnxx/alpha/map_range.hpp"
#include "grnxx/io/pool.hpp"
#include "grnxx/slice.hpp"

namespace grnxx {

class Charset;

namespace alpha {

template <typename T> class Map;
template <typename T> class MapCursor;
template <typename T> class MapScan;

enum MapType : int32_t {
  MAP_UNKNOWN      = 0,
  MAP_ARRAY        = 1,  // Array-based implementation.
  MAP_DOUBLE_ARRAY = 2,  // DoubleArray-based implementation.
  MAP_PATRICIA     = 3,  // TODO: Patricia-based implementation.
  MAP_HASH_TABLE   = 4   // TODO: HashTable-based implementation.
};

struct MapOptions {
};

struct MapCursorFlagsIdentifier;
using MapCursorFlags = FlagsImpl<MapCursorFlagsIdentifier>;

// Use the default settings.
constexpr MapCursorFlags MAP_CURSOR_DEFAULT           =
    MapCursorFlags::define(0x000);
// Sort keys by ID.
constexpr MapCursorFlags MAP_CURSOR_ORDER_BY_ID       =
    MapCursorFlags::define(0x001);
// Sort keys by key.
constexpr MapCursorFlags MAP_CURSOR_ORDER_BY_KEY      =
    MapCursorFlags::define(0x002);
// TODO: Sort keys by distance.
//constexpr MapCursorFlags MAP_CURSOR_ORDER_BY_DISTANCE =
//    MapCursorFlags::define(0x004);
// Access keys in reverse order.
constexpr MapCursorFlags MAP_CURSOR_REVERSE_ORDER     =
    MapCursorFlags::define(0x010);
// Return keys except min.
constexpr MapCursorFlags MAP_CURSOR_EXCEPT_MIN        =
    MapCursorFlags::define(0x100);
// Return keys except max.
constexpr MapCursorFlags MAP_CURSOR_EXCEPT_MAX        =
    MapCursorFlags::define(0x200);
// Return keys except exact match.
constexpr MapCursorFlags MAP_CURSOR_EXCEPT_QUERY      =
    MapCursorFlags::define(0x400);

struct MapCursorOptions {
  MapCursorFlags flags;
  uint64_t offset;
  uint64_t limit;

  MapCursorOptions() : flags(MAP_CURSOR_DEFAULT), offset(0), limit(-1) {}
};

template <typename T>
class Map {
 public:
  Map();
  virtual ~Map();

  // Create a map on "pool".
  static Map *create(MapType type, io::Pool pool,
                     const MapOptions &options = MapOptions());
  // Open an existing map.
  static Map *open(io::Pool pool, uint32_t block_id);

  // Free blocks allocated to a map.
  static bool unlink(io::Pool pool, uint32_t block_id);

  // Return the header block ID of "*this".
  virtual uint32_t block_id() const;
  // Return the type of "*this".
  virtual MapType type() const;

  // Return the minimum key ID.
  constexpr int64_t min_key_id() {
    return 0;
  }
  // Return the maximum key ID ever used.
  // If the map is empty, the return value can be -1.
  virtual int64_t max_key_id() const;
  // Return the ID of the expected next inserted ID.
  virtual int64_t next_key_id() const;
  // Return the number of keys.
  virtual uint64_t num_keys() const;

  // Get a key associated with "key_id" and return true on success.
  // Assign the found key to "*key" iff "key" != nullptr.
  virtual bool get(int64_t key_id, T *key = nullptr);
  // Find the next key and return true on success. The next key means the key
  // associated with the smallest valid ID that is greater than "key_id".
  // If "key_id" < 0, this finds the first key.
  // Assign the ID to "*next_key_id" iff "next_key_id" != nullptr.
  // Assign the key to "*next_key" iff "next_key" != nullptr.
  virtual bool get_next(int64_t key_id, int64_t *next_key_id = nullptr,
                        T *next_key = nullptr);
  // Remove a key associated with "key_id" and return true on success.
  virtual bool unset(int64_t key_id);
  // Replace a key associated with "key_id" with "dest_key" and return true
  // on success.
  virtual bool reset(int64_t key_id, T dest_key);

  // Find "key" and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  virtual bool find(T key, int64_t *key_id = nullptr);
  // Insert "key" and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  virtual bool insert(T key, int64_t *key_id = nullptr);
  // Remove "key" and return true on success.
  virtual bool remove(T key);
  // Replace "src_key" with "dest_key" and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  virtual bool update(T src_key, T dest_key, int64_t *key_id = nullptr);

  // Perform the longest prefix matching and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  // Assign the key to "*key" iff "key" != nullptr.
  virtual bool find_longest_prefix_match(T query, int64_t *key_id = nullptr,
                                         T *key = nullptr);

  // Remove all the keys in "*this" and return true on success.
  virtual bool truncate();

  // Create a cursor for accessing all the keys.
  virtual MapCursor<T> *open_basic_cursor(
      const MapCursorOptions &options = MapCursorOptions());
  // Create a cursor for accessing keys in range [min, max].
  virtual MapCursor<T> *open_id_cursor(
      int64_t min, int64_t max,
      const MapCursorOptions &options = MapCursorOptions());
  // Create a cursor for accessing keys in range [min, max].
  virtual MapCursor<T> *open_key_cursor(
      T min, T max, const MapCursorOptions &options = MapCursorOptions());

  // Only for GeoPoint.
  // Create a cursor for accessing keys whose most significant "bit_size" bits
  // are same as the MSBs of "query".
  virtual MapCursor<T> *open_bitwise_completion_cursor(
      T query, size_t bit_size,
      const MapCursorOptions &options = MapCursorOptions());

  // Only for Slice.
  // Create a cursor for accessing keys matching a prefix of "query".
  virtual MapCursor<T> *open_prefix_cursor(
      T query, size_t min_size,
      const MapCursorOptions &options = MapCursorOptions());
  // Create a cursor for accessing keys starting with "query".
  virtual MapCursor<T> *open_completion_cursor(
      T query, const MapCursorOptions &options = MapCursorOptions());
  // Create a cursor for accessing keys ending with "query".
  virtual MapCursor<T> *open_reverse_completion_cursor(
      T query, const MapCursorOptions &options = MapCursorOptions());

  MapID id() const {
    return MapID();
  }
  MapKey<T> key() const {
    return MapKey<T>();
  }

  virtual MapCursor<T> *open_cursor(
      const MapIDRange &range,
      const MapCursorOptions &options = MapCursorOptions());
  virtual MapCursor<T> *open_cursor(
      const MapKeyRange<T> &range,
      const MapCursorOptions &options = MapCursorOptions());

  // Only for Slice.
  // Create a MapScan object to find keys in "query".
  virtual MapScan<T> *open_scan(T query, const Charset *charset = nullptr);
};

template <typename T>
class MapCursor {
 public:
  MapCursor();
  virtual ~MapCursor();

  // Move the cursor to the next key and return true on success.
  virtual bool next();
  // Remove the current key and return true on success.
  virtual bool remove();

  // Return the ID of the current key.
  int64_t key_id() const {
    return key_id_;
  }
  // Return a reference to the current key.
  const T &key() const {
    return key_;
  }

 protected:
  int64_t key_id_;
  T key_;
};

template <typename T>
class MapScan {
 public:
  MapScan();
  virtual ~MapScan();

  // Scan the rest of the query and return true iff a key is found (success).
  // On success, the found key is accessible via accessors.
  virtual bool next() = 0;

  // Return the start position of the found key.
  uint64_t offset() const {
    return offset_;
  }
  // Return the size of the found key.
  uint64_t size() const {
    return size_;
  }
  // Return the ID of the found key.
  int64_t key_id() const {
    return key_id_;
  }
  // Return a reference to the found key.
  const T &key() const {
    return key_;
  }

 protected:
  uint64_t offset_;
  uint64_t size_;
  int64_t key_id_;
  T key_;
};

}  // namespace alpha
}  // namespace grnxx

#endif  // GRNXX_ALPHA_MAP_HPP
