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

#include "grnxx/storage.hpp"
#include "grnxx/traits.hpp"

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
constexpr MapCursorFlags MAP_CURSOR_DEFAULT       =
    MapCursorFlags::define(0x000);
// Sort keys by ID.
constexpr MapCursorFlags MAP_CURSOR_ORDER_BY_ID   =
    MapCursorFlags::define(0x001);
// Sort keys by key.
constexpr MapCursorFlags MAP_CURSOR_ORDER_BY_KEY  =
    MapCursorFlags::define(0x002);
// Access keys in reverse order.
constexpr MapCursorFlags MAP_CURSOR_REVERSE_ORDER =
    MapCursorFlags::define(0x010);

struct MapCursorOptions {
  MapCursorFlags flags;
  uint64_t offset;
  uint64_t limit;

  MapCursorOptions() : flags(MAP_CURSOR_DEFAULT), offset(0), limit(-1) {}
};

template <typename T>
class Map {
 public:
  using Value = typename Traits<T>::Type;
  using ValueArg = typename Traits<T>::ArgumentType;

  Map();
  virtual ~Map();

  // Create a map.
  static Map *create(Storage *storage, uint32_t storage_node_id,
                     MapType type, const MapOptions &options = MapOptions());
  // Open a map.
  static Map *open(Storage *storage, uint32_t storage_node_id);

  // Unlink a map.
  static bool unlink(Storage *storage, uint32_t storage_node_id);

  // Return the storage node ID.
  virtual uint32_t storage_node_id() const;
  // Return the implementation type.
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
  virtual bool get(int64_t key_id, Value *key = nullptr);
  // Find the next key and return true on success. The next key means the key
  // associated with the smallest valid ID that is greater than "key_id".
  // If "key_id" < 0, this finds the first key.
  // Assign the ID to "*next_key_id" iff "next_key_id" != nullptr.
  // Assign the key to "*next_key" iff "next_key" != nullptr.
  virtual bool get_next(int64_t key_id, int64_t *next_key_id = nullptr,
                        Value *next_key = nullptr);
  // Remove a key associated with "key_id" and return true on success.
  virtual bool unset(int64_t key_id);
  // Replace a key associated with "key_id" with "dest_key" and return true
  // on success.
  virtual bool reset(int64_t key_id, ValueArg dest_key);

  // Find "key" and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  virtual bool find(ValueArg key, int64_t *key_id = nullptr);
  // Insert "key" and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  virtual bool insert(ValueArg key, int64_t *key_id = nullptr);
  // Remove "key" and return true on success.
  virtual bool remove(ValueArg key);
  // Replace "src_key" with "dest_key" and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  virtual bool update(ValueArg src_key, ValueArg dest_key,
                      int64_t *key_id = nullptr);

  // Perform the longest prefix matching and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  // Assign the key to "*key" iff "key" != nullptr.
  virtual bool find_longest_prefix_match(ValueArg query,
                                         int64_t *key_id = nullptr,
                                         Value *key = nullptr);

  // Remove all the keys in "*this" and return true on success.
  virtual bool truncate();

  // TODO: Cursors.

  // Only for Slice.
  // Create a MapScan object to find keys in "query".
  virtual MapScan<Value> *open_scan(ValueArg query,
                                    const Charset *charset = nullptr);
};

template <typename T>
class MapCursor {
 public:
  using Value = typename Traits<T>::Type;
  using ValueArg = typename Traits<T>::ArgumentType;

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
  const Value &key() const {
    return key_;
  }

 protected:
  int64_t key_id_;
  Value key_;
};

template <typename T>
class MapScan {
 public:
  using Value = typename Traits<T>::Type;
  using ValueArg = typename Traits<T>::ArgumentType;

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
  const Value &key() const {
    return key_;
  }

 protected:
  uint64_t offset_;
  uint64_t size_;
  int64_t key_id_;
  Value key_;
};

}  // namespace alpha
}  // namespace grnxx

#endif  // GRNXX_MAP_HPP
