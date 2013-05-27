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
#ifndef GRNXX_MAP_CURSOR_QUERY_HPP
#define GRNXX_MAP_CURSOR_QUERY_HPP

#include "grnxx/features.hpp"

#include "grnxx/flags_impl.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

// MapCursorAllKeys.

template <typename T> struct MapCursorAllKeys {};

// MapCursorKeyID

template <typename T> struct MapCursorKeyID {};

struct MapCursorKeyIDFlagsIdentifier;
using MapCursorKeyIDFlags = FlagsImpl<MapCursorKeyIDFlagsIdentifier>;

constexpr MapCursorKeyIDFlags MAP_CURSOR_KEY_ID_LESS          =
    MapCursorKeyIDFlags::define(0x01);
constexpr MapCursorKeyIDFlags MAP_CURSOR_KEY_ID_LESS_EQUAL    =
    MapCursorKeyIDFlags::define(0x02);
constexpr MapCursorKeyIDFlags MAP_CURSOR_KEY_ID_GREATER       =
    MapCursorKeyIDFlags::define(0x04);
constexpr MapCursorKeyIDFlags MAP_CURSOR_KEY_ID_GREATER_EQUAL =
    MapCursorKeyIDFlags::define(0x08);

template <typename T>
struct MapCursorKeyIDRange {
  MapCursorKeyIDFlags flags;
  int64_t min;
  int64_t max;
};

template <typename T>
struct MapCursorKeyIDLess {
  int64_t max;
  constexpr MapCursorKeyIDFlags flags() {
    return MAP_CURSOR_KEY_ID_LESS;
  }
  operator MapCursorKeyIDRange<T>() const {
    return MapCursorKeyIDRange<T>{ flags(), 0, max };
  }
};

template <typename T>
struct MapCursorKeyIDLessEqual {
  int64_t max;
  constexpr MapCursorKeyIDFlags flags() {
    return MAP_CURSOR_KEY_ID_LESS_EQUAL;
  }
  operator MapCursorKeyIDRange<T>() const {
    return MapCursorKeyIDRange<T>{ flags(), 0, max };
  };
};

template <typename T>
struct MapCursorKeyIDGreater {
  int64_t min;
  constexpr MapCursorKeyIDFlags flags() {
    return MAP_CURSOR_KEY_ID_GREATER;
  }
  operator MapCursorKeyIDRange<T>() const {
    return MapCursorKeyIDRange<T>{ flags(), min, 0 };
  }
};

template <typename T>
struct MapCursorKeyIDGreaterEqual {
  int64_t min;
  constexpr MapCursorKeyIDFlags flags() {
    return MAP_CURSOR_KEY_ID_GREATER_EQUAL;
  }
  operator MapCursorKeyIDRange<T>() const {
    return MapCursorKeyIDRange<T>{ flags(), min };
  }
};

template <typename T>
MapCursorKeyIDLess<T> operator<(MapCursorKeyID<T>, int64_t max) {
  return MapCursorKeyIDLess<T>{ max };
}
template <typename T>
MapCursorKeyIDLessEqual<T> operator<=(MapCursorKeyID<T>, int64_t max) {
  return MapCursorKeyIDLessEqual<T>{ max };
}
template <typename T>
MapCursorKeyIDGreater<T> operator>(MapCursorKeyID<T>, int64_t min) {
  return MapCursorKeyIDGreater<T>{ min };
}
template <typename T>
MapCursorKeyIDGreaterEqual<T> operator>=(MapCursorKeyID<T>, int64_t min) {
  return MapCursorKeyIDGreaterEqual<T>{ min };
}

template <typename T>
MapCursorKeyIDGreater<T> operator<(int64_t min, MapCursorKeyID<T>) {
  return MapCursorKeyIDGreater<T>{ min };
}
template <typename T>
MapCursorKeyIDGreaterEqual<T> operator<=(int64_t min, MapCursorKeyID<T>) {
  return MapCursorKeyIDGreaterEqual<T>{ min };
}
template <typename T>
MapCursorKeyIDLess<T> operator>(int64_t max, MapCursorKeyID<T>) {
  return MapCursorKeyIDLess<T>{ max };
}
template <typename T>
MapCursorKeyIDLessEqual<T> operator>=(int64_t max, MapCursorKeyID<T>) {
  return MapCursorKeyIDLessEqual<T>{ max };
}

template <typename T>
MapCursorKeyIDRange<T> operator&&(MapCursorKeyIDLess<T> less,
                                  MapCursorKeyIDGreater<T> greater) {
  return MapCursorKeyIDRange<T>{ less.flags() | greater.flags(),
                                 greater.min, less.max };
}
template <typename T>
MapCursorKeyIDRange<T> operator&&(MapCursorKeyIDLess<T> less,
                                  MapCursorKeyIDGreaterEqual<T> greater) {
  return MapCursorKeyIDRange<T>{ less.flags() | greater.flags(),
                                 greater.min, less.max };
}
template <typename T>
MapCursorKeyIDRange<T> operator&&(MapCursorKeyIDLessEqual<T> less,
                                  MapCursorKeyIDGreater<T> greater) {
  return MapCursorKeyIDRange<T>{ less.flags() | greater.flags(),
                                 greater.min, less.max };
}
template <typename T>
MapCursorKeyIDRange<T> operator&&(MapCursorKeyIDLessEqual<T> less,
                                  MapCursorKeyIDGreaterEqual<T> greater) {
  return MapCursorKeyIDRange<T>{ less.flags() | greater.flags(),
                                 greater.min, less.max };
}
template <typename T>
MapCursorKeyIDRange<T> operator&&(MapCursorKeyIDGreater<T> greater,
                                  MapCursorKeyIDLess<T> less) {
  return less && greater;
}
template <typename T>
MapCursorKeyIDRange<T> operator&&(MapCursorKeyIDGreater<T> greater,
                                  MapCursorKeyIDLessEqual<T> less) {
  return less && greater;
}
template <typename T>
MapCursorKeyIDRange<T> operator&&(MapCursorKeyIDGreaterEqual<T> greater,
                                  MapCursorKeyIDLess<T> less) {
  return less && greater;
}
template <typename T>
MapCursorKeyIDRange<T> operator&&(MapCursorKeyIDGreaterEqual<T> greater,
                                  MapCursorKeyIDLessEqual<T> less) {
  return less && greater;
}

// MapCursorKey

template <typename T> struct MapCursorKey {};

struct MapCursorKeyFlagsIdentifier;
using MapCursorKeyFlags = FlagsImpl<MapCursorKeyFlagsIdentifier>;

constexpr MapCursorKeyFlags MAP_CURSOR_KEY_LESS          =
    MapCursorKeyFlags::define(0x01);
constexpr MapCursorKeyFlags MAP_CURSOR_KEY_LESS_EQUAL    =
    MapCursorKeyFlags::define(0x02);
constexpr MapCursorKeyFlags MAP_CURSOR_KEY_GREATER       =
    MapCursorKeyFlags::define(0x04);
constexpr MapCursorKeyFlags MAP_CURSOR_KEY_GREATER_EQUAL =
    MapCursorKeyFlags::define(0x08);

template <typename T>
struct MapCursorKeyRange {
  MapCursorKeyFlags flags;
  T min;
  T max;
};

template <typename T>
struct MapCursorKeyLess {
  T max;
  constexpr MapCursorKeyFlags flags() {
    return MAP_CURSOR_KEY_LESS;
  }
  operator MapCursorKeyRange<T>() const {
    return MapCursorKeyRange<T>{ flags(), T(), max };
  }
};

template <typename T>
struct MapCursorKeyLessEqual {
  T max;
  constexpr MapCursorKeyFlags flags() {
    return MAP_CURSOR_KEY_LESS_EQUAL;
  }
  operator MapCursorKeyRange<T>() const {
    return MapCursorKeyRange<T>{ flags(), T(), max };
  }
};

template <typename T>
struct MapCursorKeyGreater {
  T min;
  constexpr MapCursorKeyFlags flags() {
    return MAP_CURSOR_KEY_GREATER;
  }
  operator MapCursorKeyRange<T>() const {
    return MapCursorKeyRange<T>{ flags(), min, T() };
  }
};

template <typename T>
struct MapCursorKeyGreaterEqual {
  T min;
  constexpr MapCursorKeyFlags flags() {
    return MAP_CURSOR_KEY_GREATER_EQUAL;
  }
  operator MapCursorKeyRange<T>() const {
    return MapCursorKeyRange<T>{ flags(), min, T() };
  }
};

template <typename T>
MapCursorKeyLess<T> operator<(MapCursorKey<T>, T max) {
  return MapCursorKeyLess<T>{ max };
}
template <typename T>
MapCursorKeyLessEqual<T> operator<=(MapCursorKey<T>, T max) {
  return MapCursorKeyLessEqual<T>{ max };
}
template <typename T>
MapCursorKeyGreater<T> operator>(MapCursorKey<T>, T min) {
  return MapCursorKeyGreater<T>{ min };
}
template <typename T>
MapCursorKeyGreaterEqual<T> operator>=(MapCursorKey<T>, T min) {
  return MapCursorKeyGreaterEqual<T>{ min };
}

template <typename T>
MapCursorKeyGreater<T> operator<(T min, MapCursorKey<T>) {
  return MapCursorKeyGreater<T>{ min };
}
template <typename T>
MapCursorKeyGreaterEqual<T> operator<=(T min, MapCursorKey<T>) {
  return MapCursorKeyGreaterEqual<T>{ min };
}
template <typename T>
MapCursorKeyLess<T> operator>(T max, MapCursorKey<T>) {
  return MapCursorKeyLess<T>{ max };
}
template <typename T>
MapCursorKeyLessEqual<T> operator>=(T max, MapCursorKey<T>) {
  return MapCursorKeyLessEqual<T>{ max };
}

template <typename T>
MapCursorKeyRange<T> operator&&(MapCursorKeyLess<T> less,
                                MapCursorKeyGreater<T> greater) {
  return MapCursorKeyRange<T>{ less.flags() | greater.flags(),
                               greater.min, less.max };
}
template <typename T>
MapCursorKeyRange<T> operator&&(MapCursorKeyLess<T> less,
                                MapCursorKeyGreaterEqual<T> greater) {
  return MapCursorKeyRange<T>{ less.flags() | greater.flags(),
                               greater.min, less.max };
}
template <typename T>
MapCursorKeyRange<T> operator&&(MapCursorKeyLessEqual<T> less,
                                MapCursorKeyGreater<T> greater) {
  return MapCursorKeyRange<T>{ less.flags() | greater.flags(),
                               greater.min, less.max };
}
template <typename T>
MapCursorKeyRange<T> operator&&(MapCursorKeyLessEqual<T> less,
                                MapCursorKeyGreaterEqual<T> greater) {
  return MapCursorKeyRange<T>{ less.flags() | greater.flags(),
                               greater.min, less.max };
}
template <typename T>
MapCursorKeyRange<T> operator&&(MapCursorKeyGreater<T> greater,
                                MapCursorKeyLess<T> less) {
  return less && greater;
}
template <typename T>
MapCursorKeyRange<T> operator&&(MapCursorKeyGreater<T> greater,
                                MapCursorKeyLessEqual<T> less) {
  return less && greater;
}
template <typename T>
MapCursorKeyRange<T> operator&&(MapCursorKeyGreaterEqual<T> greater,
                                MapCursorKeyLess<T> less) {
  return less && greater;
}
template <typename T>
MapCursorKeyRange<T> operator&&(MapCursorKeyGreaterEqual<T> greater,
                                MapCursorKeyLessEqual<T> less) {
  return less && greater;
}

}  // namespace grnxx

#endif  // GRNXX_MAP_CURSOR_QUERY_HPP
