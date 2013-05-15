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

#include "grnxx/bytes.hpp"
#include "grnxx/geo_point.hpp"
#include "grnxx/logger.hpp"

namespace grnxx {

MapOptions::MapOptions() {}

MapCursorOptions::MapCursorOptions()
    : flags(MAP_CURSOR_DEFAULT),
      offset(0),
      limit(-1) {}

template <typename T>
MapCursor<T>::MapCursor() : key_id_(-1), key_() {}

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
      key_id_(-1),
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
  // TODO
  return nullptr;
}

template <typename T>
Map<T> *Map<T>::open(Storage *storage, uint32_t storage_node_id) {
  // TODO
  return nullptr;
}

template <typename T>
bool Map<T>::unlink(Storage *storage, uint32_t storage_node_id) {
  // TODO
  return nullptr;
}

template <typename T>
bool Map<T>::get(uint64_t, Key *) {
  GRNXX_ERROR() << "invalid operation";
  return false;
}

template <typename T>
bool Map<T>::get_next(uint64_t, uint64_t *, Key *) {
  // TODO: Give a naive implementation.
  GRNXX_ERROR() << "invalid operation";
  return false;
}

template <typename T>
bool Map<T>::unset(uint64_t key_id) {
  GRNXX_ERROR() << "invalid operation";
  return false;
}

template <typename T>
bool Map<T>::reset(uint64_t key_id, KeyArg dest_key) {
  GRNXX_ERROR() << "invalid operation";
  return false;
}

template <typename T>
bool Map<T>::find(KeyArg, uint64_t *) {
  // TODO: Give a naive implementation.
  GRNXX_ERROR() << "invalid operation";
  return false;
}

template <typename T>
bool Map<T>::insert(KeyArg, uint64_t *) {
  GRNXX_ERROR() << "invalid operation";
  return false;
}

template <typename T>
bool Map<T>::remove(KeyArg) {
  GRNXX_ERROR() << "invalid operation";
  return false;
}

template <typename T>
bool Map<T>::update(KeyArg, KeyArg, uint64_t *) {
  GRNXX_ERROR() << "invalid operation";
  return false;
}

template <typename T>
bool Map<T>::find_longest_prefix_match(KeyArg, uint64_t *, Key *) {
  // TODO: Give a naive implementation.
  GRNXX_ERROR() << "invalid operation";
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
MapCursor<T> *Map<T>::create_cursor(const map::CursorQuery<Key> &,
                                    const MapCursorOptions &) {
  // TODO: Give a naive implementation.
  GRNXX_ERROR() << "invalid operation";
  return nullptr;
}

template <typename T>
MapScanner<T> *Map<T>::create_scanner(KeyArg, const Charset *) {
  // TODO: Give a naive implementation.
  GRNXX_ERROR() << "invalid operation";
  return nullptr;
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

template class MapScanner<int8_t>;
template class MapScanner<uint8_t>;
template class MapScanner<int16_t>;
template class MapScanner<uint16_t>;
template class MapScanner<int32_t>;
template class MapScanner<uint32_t>;
template class MapScanner<int64_t>;
template class MapScanner<uint64_t>;
template class MapScanner<double>;
template class MapScanner<GeoPoint>;
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
