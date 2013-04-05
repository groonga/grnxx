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

#include "grnxx/io/pool.hpp"
#include "grnxx/slice.hpp"

namespace grnxx {
namespace alpha {

class GeoPoint {
 public:
  GeoPoint() = default;
  GeoPoint(int32_t latitude, int32_t longitude)
    : value_((static_cast<uint64_t>(latitude) << LATITUDE_BITS) |
             (static_cast<uint64_t>(longitude) << LONGITUDE_BITS)) {}

  bool operator==(GeoPoint rhs) const {
    return value_ == rhs.value_;
  }
  bool operator!=(GeoPoint rhs) const {
    return value_ != rhs.value_;
  }

  int32_t latitude() const {
    return static_cast<int32_t>((value_ & LATITUDE_MASK) >> LATITUDE_BITS);
  }
  int32_t longitude() const {
    return static_cast<int32_t>((value_ & LONGITUDE_MASK) >> LONGITUDE_BITS);
  }

  void set_latitude(int32_t latitude) {
    value_ = (value_ & LONGITUDE_MASK) |
             (static_cast<uint64_t>(latitude) << LATITUDE_BITS);
  }
  void set_longitude(int32_t longitude) {
    value_ = (value_ & LATITUDE_MASK) |
             (static_cast<uint64_t>(longitude) << LONGITUDE_BITS);
  }

 private:
  uint64_t value_;

  static constexpr uint64_t LATITUDE_MASK  = 0xFFFFFFFFULL;
  static constexpr uint8_t  LATITUDE_BITS  = 0;
  static constexpr uint64_t LONGITUDE_MASK = 0xFFFFFFFFULL << 32;
  static constexpr uint8_t  LONGITUDE_BITS = 32;
};

enum MapType : int32_t {
  MAP_UNKNOWN      = 0,
  MAP_ARRAY        = 1,  // Test implementation.
  MAP_DOUBLE_ARRAY = 2,  // TODO: Not supported yet.
  MAP_PATRICIA     = 3,  // TODO: Not supported yet.
  MAP_HASH_TABLE   = 4   // TODO: Not supported yet.
};

struct MapOptions {
};

struct MapHeader {
  MapType type;
};

// TODO: Not supported yet.
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
  T key() const {
    return key_;
  }

 private:
  int64_t key_id_;
  T key_;
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

  // Get a key associated with "key_id" and return true on success.
  // Assign the found key to "*key" iff "key" != nullptr.
  virtual bool get(int64_t key_id, T *key = nullptr);
  // Remove a key associated with "key_id" and return true on success.
  virtual bool unset(int64_t key_id);
  // Replace a key associated with "key_id" with "dest_key" and return true
  // on success.
  virtual bool reset(int64_t key_id, T dest_key);

  // Search "key" and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  virtual bool search(T key, int64_t *key_id = nullptr);
  // Insert "key" and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  virtual bool insert(T key, int64_t *key_id = nullptr);
  // Remove "key" and return true on success.
  virtual bool remove(T key);
  // Replace "src_key" with "dest_key" and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  virtual bool update(T src_key, T dest_key, int64_t *key_id = nullptr);

  // Remove all the keys in "*this".
  virtual void truncate();
};

}  // namespace alpha
}  // namespace grnxx

#endif  // GRNXX_ALPHA_MAP_HPP
