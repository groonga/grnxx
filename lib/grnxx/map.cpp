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
#include "grnxx/exception.hpp"
#include "grnxx/geo_point.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/storage.hpp"
#include "grnxx/string_builder.hpp"
#include "grnxx/map/array_map.hpp"
#include "grnxx/map/cursor_impl.hpp"
#include "grnxx/map/double_array.hpp"
#include "grnxx/map/hash_table.hpp"
#include "grnxx/map/header.hpp"
#include "grnxx/map/helper.hpp"
#include "grnxx/map/patricia.hpp"
#include "grnxx/map/scanner_impl.hpp"

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

template <typename T>
Map<T>::Map() {}

template <typename T>
Map<T>::~Map() {}

template <typename T>
Map<T> *Map<T>::create(Storage *storage, uint32_t storage_node_id,
                       MapType type, const MapOptions &options) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage == nullptr";
    throw LogicError();
  }
  switch (type) {
    case MAP_ARRAY: {
      return map::ArrayMap<T>::create(storage, storage_node_id, options);
    }
    case MAP_DOUBLE_ARRAY: {
      return map::DoubleArray<T>::create(storage, storage_node_id, options);
    }
    case MAP_PATRICIA: {
      return map::Patricia<T>::create(storage, storage_node_id, options);
    }
    case MAP_HASH_TABLE: {
      return map::HashTable<T>::create(storage, storage_node_id, options);
    }
    default: {
      GRNXX_ERROR() << "invalid argument: type = " << type;
      throw LogicError();
    }
  }
}

template <typename T>
Map<T> *Map<T>::open(Storage *storage, uint32_t storage_node_id) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage == nullptr";
    throw LogicError();
  }
  StorageNode storage_node = storage->open_node(storage_node_id);
  const map::Header * const header =
      static_cast<const map::Header *>(storage_node.body());
  switch (header->type) {
    case MAP_ARRAY: {
      return map::ArrayMap<T>::open(storage, storage_node_id);
    }
    case MAP_DOUBLE_ARRAY: {
      return map::DoubleArray<T>::open(storage, storage_node_id);
    }
    case MAP_PATRICIA: {
      return map::Patricia<T>::open(storage, storage_node_id);
    }
    case MAP_HASH_TABLE: {
      return map::HashTable<T>::open(storage, storage_node_id);
    }
    default: {
      GRNXX_ERROR() << "invalid format: type = " << header->type;
      throw LogicError();
    }
  }
}

template <typename T>
bool Map<T>::unlink(Storage *storage, uint32_t storage_node_id) {
  std::unique_ptr<Map<T>> map(open(storage, storage_node_id));
  return storage->unlink_node(storage_node_id);
}

template <typename T>
bool Map<T>::get(int64_t, Key *) {
  GRNXX_ERROR() << "invalid operation";
  throw LogicError();
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
  throw LogicError();
}

template <typename T>
bool Map<T>::remove(KeyArg) {
  GRNXX_ERROR() << "invalid operation";
  throw LogicError();
}

template <typename T>
bool Map<T>::replace(KeyArg, KeyArg, int64_t *) {
  GRNXX_ERROR() << "invalid operation";
  throw LogicError();
}

template <typename T>
bool Map<T>::find_longest_prefix_match(KeyArg, int64_t *, Key *) {
  GRNXX_ERROR() << "invalid operation";
  throw LogicError();
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
  throw LogicError();
}

template <typename T>
MapCursor<T> *Map<T>::create_cursor(MapCursorAllKeys<T>,
                                    const MapCursorOptions &options) {
  return map::AllKeysCursor<T>::create(this, options);
}

template <typename T>
MapCursor<T> *Map<T>::create_cursor(const MapCursorKeyIDRange<T> &query,
                                    const MapCursorOptions &options) {
  return map::KeyIDRangeCursor<T>::create(this, query, options);
}

template <typename T>
MapCursor<T> *Map<T>::create_cursor(const MapCursorKeyRange<T> &query,
                                    const MapCursorOptions &options) {
  return map::KeyRangeCursor<T>::create(this, query, options);
}

template <>
MapCursor<GeoPoint> *Map<GeoPoint>::create_cursor(
    const MapCursorKeyRange<GeoPoint> &, const MapCursorOptions &) {
  GRNXX_ERROR() << "invalid operation";
  throw LogicError();
}

template <typename T>
MapScanner<T> *Map<T>::create_scanner(KeyArg, const Charset *) {
  GRNXX_ERROR() << "invalid operation";
  throw LogicError();
}

template <>
MapScanner<Bytes> *Map<Bytes>::create_scanner(KeyArg query,
                                              const Charset *charset) {
  return map::ScannerImpl<Bytes>::create(this, query, charset);
}

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
