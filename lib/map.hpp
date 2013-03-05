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

// Tentative version.

#include "io/pool.hpp"
#include "slice.hpp"

namespace grnxx {

enum MapType : int32_t {
  MAP_UNKNOWN      = 0,
  MAP_DOUBLE_ARRAY = 1   // grnxx::map::DoubleArray.
};

struct MapOptions {
  MapType type;

  MapOptions();
};

struct MapHeader {
  MapType type;

  MapHeader();
};

// TODO: Dynamic memory allocation will be supported.
class MapKey {
 public:
  // Create an empty (zero-size) key.
  MapKey() : slice_() {}
  // Create a key that refers to a zero-terminated string.
  MapKey(const char *cstr) : slice_(cstr) {}
  // Create a key.
  MapKey(const void *ptr, size_t size) : slice_(ptr, size) {}

  // Disable copy.
  MapKey(const MapKey &) = delete;
  MapKey &operator=(const MapKey &) = delete;

  MapKey &operator=(const Slice &s) {
    slice_ = s;
    return *this;
  }

  // Return true iff "*this" is not empty.
  explicit operator bool() const {
    return static_cast<bool>(slice_);
  }

  // Make "*this" empty.
  void clear() {
    slice_.clear();
  }

  // Compare "*this" and "s" and return a negative value if "*this" < "s",
  // zero if "*this" == "s", or a positive value otherwise (if "*this" > "s").
  int compare(const Slice &s) const {
    return slice_.compare(s);
  }

  // Return true iff "s" is a prefix of "*this".
  bool starts_with(const Slice &s) const {
    return slice_.starts_with(s);
  }
  // Return true iff "s" is a suffix of "*this".
  bool ends_with(const Slice &s) const {
    return slice_.ends_with(s);
  }

  // Return the "i"-th byte of "*this".
  uint8_t operator[](size_t i) const {
    return slice_[i];
  }

  // Return the slice of "*this".
  const Slice &slice() const {
    return slice_;
  }
  // Return the starting address of "*this".
  const void *address() const {
    return slice_.address();
  }
  // Return a pointer that refers to the starting address of "*this".
  const uint8_t *ptr() const {
    return slice_.ptr();
  }
  // Return the size of "*this".
  size_t size() const {
    return slice_.size();
  }

 private:
  Slice slice_;
};

inline bool operator==(const MapKey &lhs, const Slice &rhs) {
  return lhs.slice() == rhs;
}

inline bool operator!=(const MapKey &lhs, const Slice &rhs) {
  return lhs.slice() != rhs;
}

inline std::ostream &operator<<(std::ostream &stream, const MapKey &key) {
  return stream << key.slice();
}

class Map {
 public:
  Map();
  virtual ~Map();

  // Create a map on "pool".
  static Map *create(const MapOptions &options, io::Pool pool);
  // Open a map.
  static Map *open(io::Pool pool, uint32_t block_id);

  // Remove blocks allocated to a map.
  static void unlink(io::Pool pool, uint32_t block_id);

  // Return the header block ID of "*this".
  virtual uint32_t block_id() const = 0;

  // Search the key associated with "key_id" and return true on success.
  // Assign the found key to "*key" iff "key" != nullptr.
  virtual bool search(int64_t key_id, MapKey *key = nullptr) = 0;
  // Search "key" and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  virtual bool search(const Slice &key, int64_t *key_id = nullptr) = 0;

  // Search a prefix key of "query" and return true on success.
  // Assign the longest prefix key to "*key" iff "key" != nullptr.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  virtual bool lcp_search(const Slice &query, int64_t *key_id = nullptr,
                          MapKey *key = nullptr) = 0;

  // Insert "key" and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  virtual bool insert(const Slice &key, int64_t *key_id = nullptr) = 0;

  // Remove the key associated with "key_id" and return true on success.
  virtual bool remove(int64_t key_id) = 0;
  // Remove "key" and return true on success.
  virtual bool remove(const Slice &key) = 0;

  // Replace the key associated with "key_id" with "dest_key" and return true
  // on success.
  virtual bool update(int64_t key_id, const Slice &dest_key) = 0;
  // Replace "src_key" with "dest_key" and return true on success.
  // Assign the ID to "*key_id" iff "key_id" != nullptr.
  virtual bool update(const Slice &src_key, const Slice &dest_key,
                      int64_t *key_id = nullptr) = 0;

  // TODO
};

class MapScan {
 public:
  ~MapScan();

  // Create an object to find keys in "query".
  static MapScan *open(Map *map, const Slice &query);

  // Scan the rest of the query and return true iff a key is found (success).
  // On success, the found key is accessible via accessors.
  bool next();

  // Return the query.
  const Slice &query() const {
    return query_;
  }
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
  const MapKey &key() const {
    return key_;
  }

 protected:
  Map *map_;
  Slice query_;
  uint64_t offset_;
  uint64_t size_;
  int64_t key_id_;
  MapKey key_;

  MapScan();
};

}  // namespace grnxx

#endif  // GRNXX_MAP_HPP
