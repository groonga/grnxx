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
#ifndef GRNXX_MAP_HPP
#define GRNXX_MAP_HPP

#include "grnxx/features.hpp"

#include "grnxx/map_cursor.hpp"
#include "grnxx/map_cursor_query.hpp"
#include "grnxx/map_scanner.hpp"
#include "grnxx/traits.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Charset;
class Storage;
class StringBuilder;

constexpr int64_t MAP_MIN_KEY_ID     = 0;
constexpr int64_t MAP_MAX_KEY_ID     = (1LL << 40) - 2;
constexpr int64_t MAP_INVALID_KEY_ID = MAP_MAX_KEY_ID + 1;

enum MapType : uint32_t {
  MAP_ARRAY        = 0,  // Array-based implementation.
  MAP_HASH_TABLE   = 1,  // HashTable-based implementation.
  MAP_PATRICIA     = 2,  // Patricia-based implementation.
  MAP_DOUBLE_ARRAY = 3   // DoubleArray-based implementation.
};

StringBuilder &operator<<(StringBuilder &builder, MapType type);

struct MapOptions {
  // Initialize the members.
  MapOptions();
};

template <typename T>
class Map {
 public:
  using Key = typename Traits<T>::Type;
  using KeyArg = typename Traits<T>::ArgumentType;
  using Cursor = MapCursor<T>;
  using Scanner = MapScanner<T>;

  Map();
  virtual ~Map();

  // Create a map.
  static Map *create(Storage *storage, uint32_t storage_node_id,
                     MapType type, const MapOptions &options = MapOptions());
  // Open a map.
  static Map *open(Storage *storage, uint32_t storage_node_id);

  // Unlink a map.
  static void unlink(Storage *storage, uint32_t storage_node_id);

  // Return the storage node ID.
  virtual uint32_t storage_node_id() const = 0;
  // Return the implementation type.
  virtual MapType type() const = 0;

  // Return the minimum key ID.
  constexpr int64_t min_key_id() const {
    return MAP_MIN_KEY_ID;
  }
  // Return the maximum key ID ever used.
  // The return value can be a negative value iff the map is empty.
  virtual int64_t max_key_id() const = 0;
  // Return the number of keys.
  virtual uint64_t num_keys() const = 0;

  // Get a key associated with "key_id" and return true on success.
  // Assign the found key to "*key" iff "key" != nullptr.
  virtual bool get(int64_t key_id, Key *key = nullptr);
  // Find the next key and return true on success. The next key means the key
  // associated with the smallest valid ID that is greater than "key_id".
  // If "key_id" > MAP_MAX_KEY_ID, this finds the first key.
  // Assign the ID to "*next_key_id" iff "next_key_id" != nullptr.
  // Assign the key to "*next_key" iff "next_key" != nullptr.
  virtual bool get_next(int64_t key_id, int64_t *next_key_id = nullptr,
                        Key *next_key = nullptr);
  // Remove a key associated with "key_id" and return true on success.
  virtual bool unset(int64_t key_id);
  // Replace a key associated with "key_id" with "dest_key" and return true
  // on success.
  virtual bool reset(int64_t key_id, KeyArg dest_key);

  // Find "key" and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  virtual bool find(KeyArg key, int64_t *key_id = nullptr);
  // Add "key" and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  virtual bool add(KeyArg key, int64_t *key_id = nullptr);
  // Remove "key" and return true on success.
  virtual bool remove(KeyArg key);
  // Replace "src_key" with "dest_key" and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  virtual bool replace(KeyArg src_key, KeyArg dest_key,
                      int64_t *key_id = nullptr);

  // Perform longest prefix matching and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  // Assign the key to "*key" iff "key" != nullptr.
  virtual bool find_longest_prefix_match(KeyArg query,
                                         int64_t *key_id = nullptr,
                                         Key *key = nullptr);

  // Remove all the keys in "*this" and return true on success.
  virtual void truncate();

  // Return a reference to create a cursor query.
  MapCursorAllKeys<T> all_keys() const {
    return MapCursorAllKeys<T>();
  }
  // Return a reference to create a cursor query.
  MapCursorKeyID<T> key_id() const {
    return MapCursorKeyID<T>();
  }
  // Return a reference to create a cursor query.
  MapCursorKey<T> key() const {
    return MapCursorKey<T>();
  }

  // Create a cursor for accessing all the keys.
  virtual Cursor *create_cursor(
      MapCursorAllKeys<T> query,
      const MapCursorOptions &options = MapCursorOptions());
  // Create a cursor for accessing keys that satisfy "query".
  virtual Cursor *create_cursor(
      const MapCursorKeyIDRange<T> &query,
      const MapCursorOptions &options = MapCursorOptions());
  // Create a cursor for accessing keys that satisfy "query".
  virtual Cursor *create_cursor(
      const MapCursorKeyRange<T> &query,
      const MapCursorOptions &options = MapCursorOptions());

  // Create a MapScanner object to find keys in "query".
  virtual Scanner *create_scanner(KeyArg query,
                                  const Charset *charset = nullptr);
};

}  // namespace grnxx

#endif  // GRNXX_MAP_HPP
