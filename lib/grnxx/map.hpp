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

#include "grnxx/flags_impl.hpp"
#include "grnxx/traits.hpp"
#include "grnxx/types.hpp"

namespace grnxx {
namespace map {

template <typename T> class DummyKeyID;
template <typename T> class DummyKey;
template <typename T> class CursorQuery;

}  // namespace map

class Storage;
class Charset;

template <typename T> class Map;

constexpr uint64_t MAP_MIN_KEY_ID     = 0;
constexpr uint64_t MAP_MAX_KEY_ID     = (1ULL << 40) - 1;
constexpr uint64_t MAP_INVALID_KEY_ID = MAP_MAX_KEY_ID + 1;

enum MapType : uint32_t {
  MAP_UNKNOWN      = 0,
  MAP_ARRAY        = 1,  // TODO: Array-based implementation.
  MAP_DOUBLE_ARRAY = 2,  // TODO: DoubleArray-based implementation.
  MAP_PATRICIA     = 3,  // TODO: Patricia-based implementation.
  MAP_HASH_TABLE   = 4   // TODO: HashTable-based implementation.
};

struct MapOptions {
  // Initialize the members.
  MapOptions();
};

// TODO: How to implement NEAR cursor.
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

  // Initialize the members.
  MapCursorOptions();
};

template <typename T>
class MapCursor {
 public:
  using Key = typename Traits<T>::Type;
  using KeyArg = typename Traits<T>::ArgumentType;

  MapCursor();
  virtual ~MapCursor();

  // Move the cursor to the next key and return true on success.
  virtual bool next() = 0;
  // Remove the current key and return true on success.
  virtual bool remove();

  // Return the ID of the current key.
  uint64_t key_id() const {
    return key_id_;
  }
  // Return a reference to the current key.
  const Key &key() const {
    return key_;
  }

 protected:
  uint64_t key_id_;
  Key key_;
};

template <typename T>
class MapScanner {
 public:
  using Key = typename Traits<T>::Type;
  using KeyArg = typename Traits<T>::ArgumentType;

  MapScanner();
  virtual ~MapScanner();

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
  uint64_t key_id() const {
    return key_id_;
  }
  // Return a reference to the found key.
  const Key &key() const {
    return key_;
  }

 protected:
  uint64_t offset_;
  uint64_t size_;
  uint64_t key_id_;
  Key key_;
};

template <typename T>
class Map {
 public:
  using Key = typename Traits<T>::Type;
  using KeyArg = typename Traits<T>::ArgumentType;
  using DummyKeyID = map::DummyKeyID<T>;
  using DummyKey = map::DummyKey<T>;
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
  static bool unlink(Storage *storage, uint32_t storage_node_id);

  // Return the storage node ID.
  virtual uint32_t storage_node_id() const = 0;
  // Return the implementation type.
  virtual MapType type() const = 0;

  // Return the minimum key ID.
  constexpr uint64_t min_key_id() {
    return MAP_MIN_KEY_ID;
  }
  // Return the maximum key ID ever used.
  // If the map is empty, the return value can be MAP_INVALID_KEY_ID.
  virtual uint64_t max_key_id() const = 0;
  // Return the ID of the expected next inserted ID.
  virtual uint64_t next_key_id() const = 0;
  // Return the number of keys.
  virtual uint64_t num_keys() const = 0;

  // Get a key associated with "key_id" and return true on success.
  // Assign the found key to "*key" iff "key" != nullptr.
  virtual bool get(uint64_t key_id, Key *key = nullptr);
  // Find the next key and return true on success. The next key means the key
  // associated with the smallest valid ID that is greater than "key_id".
  // If "key_id" < 0, this finds the first key.
  // Assign the ID to "*next_key_id" iff "next_key_id" != nullptr.
  // Assign the key to "*next_key" iff "next_key" != nullptr.
  virtual bool get_next(uint64_t key_id, uint64_t *next_key_id = nullptr,
                        Key *next_key = nullptr);
  // Remove a key associated with "key_id" and return true on success.
  virtual bool unset(uint64_t key_id);
  // Replace a key associated with "key_id" with "dest_key" and return true
  // on success.
  virtual bool reset(uint64_t key_id, KeyArg dest_key);

  // Find "key" and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  virtual bool find(KeyArg key, uint64_t *key_id = nullptr);
  // Insert "key" and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  virtual bool insert(KeyArg key, uint64_t *key_id = nullptr);
  // Remove "key" and return true on success.
  virtual bool remove(KeyArg key);
  // Replace "src_key" with "dest_key" and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  virtual bool update(KeyArg src_key, KeyArg dest_key,
                      uint64_t *key_id = nullptr);

  // Perform longest prefix matching and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  // Assign the key to "*key" iff "key" != nullptr.
  virtual bool find_longest_prefix_match(KeyArg query,
                                         uint64_t *key_id = nullptr,
                                         Key *key = nullptr);

  // Remove all the keys in "*this" and return true on success.
  virtual bool truncate();

  // TODO: Not yet fixed.
  // Return a reference to create a cursor query.
  const DummyKeyID &key_id() const {
    return *static_cast<const DummyKeyID *>(nullptr);
  }
  // Return a reference to create a cursor query.
  const DummyKey &key() const {
    return *static_cast<const DummyKey *>(nullptr);
  }

  // Create a cursor for accessing all the keys.
  virtual Cursor *create_cursor(
      const MapCursorOptions &options = MapCursorOptions());
  // Create a cursor for accessing keys that satisfy "query".
  virtual Cursor *create_cursor(
      const map::CursorQuery<Key> &query,
      const MapCursorOptions &options = MapCursorOptions());

  // Create a MapScanner object to find keys in "query".
  virtual Scanner *create_scanner(KeyArg query,
                                  const Charset *charset = nullptr);
};

}  // namespace grnxx

#endif  // GRNXX_MAP_HPP
