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
#include "grnxx/alpha/map.hpp"

#include "grnxx/alpha/map/array.hpp"
#include "grnxx/slice.hpp"

namespace grnxx {
namespace alpha {

template <typename T>
MapCursor<T>::MapCursor() : key_id_(-1), key_() {}

template <typename T>
MapCursor<T>::~MapCursor() {}

template <typename T>
bool MapCursor<T>::next() {
  // TODO: Not supported!?
  return false;
}

template <typename T>
bool MapCursor<T>::remove() {
  // TODO: Not supported.
  return false;
}

template class MapCursor<int8_t>;
template class MapCursor<int16_t>;
template class MapCursor<int32_t>;
template class MapCursor<int64_t>;
template class MapCursor<uint8_t>;
template class MapCursor<uint16_t>;
template class MapCursor<uint32_t>;
template class MapCursor<uint64_t>;
template class MapCursor<double>;
//template class MapCursor<Slice>;

template <typename T>
Map<T>::Map() {}

template <typename T>
Map<T>::~Map() {}

template <typename T>
Map<T> *Map<T>::create(MapType type, io::Pool pool,
                       const MapOptions &options) {
  switch (type) {
    case MAP_ARRAY: {
      return map::Array<T>::create(pool, options);
    }
    default: {
      // Not supported yet.
      return nullptr;
    }
  }
}

template <typename T>
Map<T> *Map<T>::open(io::Pool pool, uint32_t block_id) {
  const MapHeader *header = static_cast<const MapHeader *>(
      pool.get_block_address(block_id));
  switch (header->type) {
    case MAP_ARRAY: {
      return map::Array<T>::open(pool, block_id);
    }
    default: {
      // Not supported yet.
      return nullptr;
    }
  }
}

template <typename T>
bool Map<T>::unlink(io::Pool pool, uint32_t block_id) {
  const MapHeader *header = static_cast<const MapHeader *>(
      pool.get_block_address(block_id));
  switch (header->type) {
    case MAP_ARRAY: {
      return map::Array<T>::unlink(pool, block_id);
    }
    default: {
      // Not supported yet.
      return false;
    }
  }
}

template <typename T>
uint32_t Map<T>::block_id() const {
  // Not supported.
  return 0;
}

template <typename T>
MapType Map<T>::type() const {
  // Not supported.
  return MAP_UNKNOWN;
}

template <typename T>
bool Map<T>::get(int64_t, T *) {
  // Not supported.
  return false;
}

template <typename T>
bool Map<T>::unset(int64_t) {
  // Not supported.
  return false;
}

template <typename T>
bool Map<T>::reset(int64_t, T) {
  // Not supported.
  return false;
}

template <typename T>
bool Map<T>::search(T, int64_t *) {
  // Not supported.
  return false;
}

template <typename T>
bool Map<T>::insert(T, int64_t *) {
  // Not supported.
  return false;
}

template <typename T>
bool Map<T>::remove(T) {
  // Not supported.
  return false;
}

template <typename T>
bool Map<T>::update(T, T, int64_t *) {
  // Not supported.
  return false;
}

template <typename T>
void Map<T>::truncate() {
  // Not supported.
}

template class Map<int8_t>;
template class Map<int16_t>;
template class Map<int32_t>;
template class Map<int64_t>;
template class Map<uint8_t>;
template class Map<uint16_t>;
template class Map<uint32_t>;
template class Map<uint64_t>;
template class Map<double>;
//template class Map<Slice>;

}  // namespace alpha
}  // namespace grnxx
