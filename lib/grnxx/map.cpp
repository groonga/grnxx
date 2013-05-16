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
#include "grnxx/map.hpp"

#include <limits>

#include "grnxx/bytes.hpp"
#include "grnxx/geo_point.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/storage.hpp"
#include "grnxx/string_builder.hpp"
#include "grnxx/map/header.hpp"
#include "grnxx/map/helper.hpp"
#include "grnxx/map/scanner.hpp"

namespace grnxx {

StringBuilder &operator<<(StringBuilder &builder, MapType type) {
  switch (type) {
    case MAP_ARRAY: {
      return builder << "MAP_ARRAY";
    }
    case MAP_DOUBLE_ARRAY: {
      return builder << "MAP_DOUBLE_ARRAY";
    }
    case MAP_PATRICIA: {
      return builder << "MAP_PATRICIA";
    }
    case MAP_HASH_TABLE: {
      return builder << "MAP_HASH_TABLE";
    }
    default: {
      return builder << "n/a";
    }
  }
}

MapOptions::MapOptions() {}

MapCursorOptions::MapCursorOptions()
    : flags(MAP_CURSOR_DEFAULT),
      offset(0),
      limit(std::numeric_limits<uint64_t>::max()) {}

template <typename T>
MapCursor<T>::MapCursor() : key_id_(MAP_INVALID_KEY_ID), key_() {}

template <typename T>
MapCursor<T>::~MapCursor() {}

template <typename T>
bool MapCursor<T>::remove() {
  GRNXX_ERROR() << "invalid operation";
  return false;
}

template <typename T>
MapScanner<T>::MapScanner()
    : offset_(0),
      size_(0),
      key_id_(MAP_INVALID_KEY_ID),
      key_() {}

template <typename T>
MapScanner<T>::~MapScanner() {}

template <typename T>
Map<T>::Map() {}

template <typename T>
Map<T>::~Map() {}

template <typename T>
Map<T> *Map<T>::create(Storage *storage, uint32_t storage_node_id,
                       MapType type, const MapOptions &options) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage == nullptr";
    return nullptr;
  }
  switch (type) {
    case MAP_ARRAY: {
      // TODO: Not supported yet.
    }
    case MAP_DOUBLE_ARRAY: {
      // TODO: Not supported yet.
    }
    case MAP_PATRICIA: {
      // TODO: Not supported yet.
    }
    case MAP_HASH_TABLE: {
      // TODO: Not supported yet.
    }
    default: {
      GRNXX_ERROR() << "invalid argument: type = " << type;
      return nullptr;
    }
  }
}

template <typename T>
Map<T> *Map<T>::open(Storage *storage, uint32_t storage_node_id) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage == nullptr";
    return nullptr;
  }
  StorageNode storage_node = storage->open_node(storage_node_id);
  if (!storage_node) {
    return nullptr;
  }
  const map::Header * const header =
      static_cast<const map::Header *>(storage_node.body());
  switch (header->type) {
    case MAP_ARRAY: {
      // TODO: Not supported yet.
    }
    case MAP_DOUBLE_ARRAY: {
      // TODO: Not supported yet.
    }
    case MAP_PATRICIA: {
      // TODO: Not supported yet.
    }
    case MAP_HASH_TABLE: {
      // TODO: Not supported yet.
    }
    default: {
      GRNXX_ERROR() << "invalid format: type = " << header->type;
      return nullptr;
    }
  }
}

template <typename T>
bool Map<T>::unlink(Storage *storage, uint32_t storage_node_id) {
  std::unique_ptr<Map<T>> map(open(storage, storage_node_id));
  if (!map) {
    return false;
  }
  return storage->unlink_node(storage_node_id);
}

template <typename T>
bool Map<T>::get(int64_t, Key *) {
  GRNXX_ERROR() << "invalid operation";
  return false;
}

template <typename T>
bool Map<T>::get_next(int64_t key_id, int64_t *next_key_id, Key *next_key) {
  // Naive implementation.
  if ((key_id < MAP_MIN_KEY_ID) || (key_id > MAP_MAX_KEY_ID)) {
    key_id = MAP_MIN_KEY_ID - 1;
  }
  for (++key_id; key_id <= max_key_id(); ++key_id) {
    if (get(key_id, next_key)) {
      if (next_key_id) {
        *next_key_id = key_id;
      }
      return true;
    }
  }
  return false;
}

template <typename T>
bool Map<T>::unset(int64_t key_id) {
  // Naive implementation.
  Key key;
  if (!get(key_id, &key)) {
    return false;
  }
  return remove(key);
}

template <typename T>
bool Map<T>::reset(int64_t key_id, KeyArg dest_key) {
  // Naive implementation.
  Key src_key;
  if (!get(key_id, &src_key)) {
    return false;
  }
  return replace(src_key, dest_key);
}

template <typename T>
bool Map<T>::find(KeyArg key, int64_t *key_id) {
  // Naive implementation.
  const Key normalized_key = map::Helper<T>::normalize(key);
  int64_t next_key_id = MAP_INVALID_KEY_ID;
  Key next_key;
  while (get_next(next_key_id, &next_key_id, &next_key)) {
    if (map::Helper<T>::equal_to(normalized_key, next_key)) {
      if (key_id) {
        *key_id = next_key_id;
      }
      return true;
    }
  }
  return false;
}

template <typename T>
bool Map<T>::add(KeyArg, int64_t *) {
  GRNXX_ERROR() << "invalid operation";
  return false;
}

template <typename T>
bool Map<T>::remove(KeyArg) {
  GRNXX_ERROR() << "invalid operation";
  return false;
}

template <typename T>
bool Map<T>::replace(KeyArg, KeyArg, int64_t *) {
  GRNXX_ERROR() << "invalid operation";
  return false;
}

template <typename T>
bool Map<T>::find_longest_prefix_match(KeyArg, int64_t *, Key *) {
  GRNXX_ERROR() << "invalid operation";
  return false;
}

template <>
bool Map<Bytes>::find_longest_prefix_match(KeyArg query, int64_t *key_id,
                                           Key *key) {
  // Naive implementation.
  int64_t next_key_id = MAP_INVALID_KEY_ID;
  int64_t longest_prefix_key_id = MAP_INVALID_KEY_ID;
  Key next_key;
  Key longest_prefix_key = nullptr;
  while (get_next(next_key_id, &next_key_id, &next_key)) {
    if (query.starts_with(next_key)) {
      if (next_key.size() >= longest_prefix_key.size()) {
        longest_prefix_key_id = next_key_id;
        longest_prefix_key = next_key;
      }
    }
  }
  if (longest_prefix_key_id != MAP_INVALID_KEY_ID) {
    if (key_id) {
      *key_id = longest_prefix_key_id;
    }
    if (key) {
      *key = longest_prefix_key;
    }
    return true;
  }
  return false;
}

template <typename T>
bool Map<T>::truncate() {
  GRNXX_ERROR() << "invalid operation";
  return false;
}

template <typename T>
MapCursor<T> *Map<T>::create_cursor(const MapCursorOptions &) {
  // TODO: Give a naive implementation.
  GRNXX_ERROR() << "invalid operation";
  return nullptr;
}

template <typename T>
MapCursor<T> *Map<T>::create_cursor(const map::CursorQuery<T> &,
                                    const MapCursorOptions &) {
  // TODO: Give a naive implementation.
  GRNXX_ERROR() << "invalid operation";
  return nullptr;
}

template <typename T>
MapScanner<T> *Map<T>::create_scanner(KeyArg, const Charset *) {
  GRNXX_ERROR() << "invalid operation";
  return nullptr;
}

template <>
MapScanner<Bytes> *Map<Bytes>::create_scanner(KeyArg query,
                                              const Charset *charset) {
  return map::Scanner<Bytes>::create(this, query, charset);
}

template class MapCursor<int8_t>;
template class MapCursor<uint8_t>;
template class MapCursor<int16_t>;
template class MapCursor<uint16_t>;
template class MapCursor<int32_t>;
template class MapCursor<uint32_t>;
template class MapCursor<int64_t>;
template class MapCursor<uint64_t>;
template class MapCursor<double>;
template class MapCursor<GeoPoint>;
template class MapCursor<Bytes>;

template class MapScanner<Bytes>;

template class Map<int8_t>;
template class Map<uint8_t>;
template class Map<int16_t>;
template class Map<uint16_t>;
template class Map<int32_t>;
template class Map<uint32_t>;
template class Map<int64_t>;
template class Map<uint64_t>;
template class Map<double>;
template class Map<GeoPoint>;
template class Map<Bytes>;

}  // namespace grnxx
