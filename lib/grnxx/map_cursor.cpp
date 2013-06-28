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
#include "grnxx/map_cursor.hpp"

#include <limits>

#include "grnxx/bytes.hpp"
#include "grnxx/geo_point.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/map.hpp"
#include "grnxx/string_builder.hpp"

namespace grnxx {

#define GRNXX_FLAGS_WRITE(flag) do { \
  if (flags & flag) { \
    if (!is_first) { \
      builder << " | "; \
    } \
    builder << #flag; \
    is_first = false; \
  } \
} while (false)

StringBuilder &operator<<(StringBuilder &builder, MapCursorFlags flags) {
  bool is_first = true;
  GRNXX_FLAGS_WRITE(MAP_CURSOR_ORDER_BY_ID);
  GRNXX_FLAGS_WRITE(MAP_CURSOR_ORDER_BY_KEY);
  GRNXX_FLAGS_WRITE(MAP_CURSOR_REVERSE_ORDER);
  if (is_first) {
    builder << "MAP_CURSOR_DEFAULT";
  }
  return builder;
}

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

}  // namespace grnxx
