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
#ifndef GRNXX_ALPHA_MAP_RANGE_HPP
#define GRNXX_ALPHA_MAP_RANGE_HPP

#include "grnxx/basic.hpp"
#include "grnxx/flags_impl.hpp"

namespace grnxx {
namespace alpha {

struct MapRangeFlagsIdentifier;
using MapRangeFlags = FlagsImpl<MapRangeFlagsIdentifier>;

constexpr MapRangeFlags MAP_RANGE_LESS          = MapRangeFlags::define(0x01);
constexpr MapRangeFlags MAP_RANGE_LESS_EQUAL    = MapRangeFlags::define(0x02);
constexpr MapRangeFlags MAP_RANGE_GREATER       = MapRangeFlags::define(0x04);
constexpr MapRangeFlags MAP_RANGE_GREATER_EQUAL = MapRangeFlags::define(0x08);

struct MapID {};

struct MapIDRange {
  int64_t min;
  int64_t max;
  MapRangeFlags flags;
};

struct MapIDLess {
  int64_t max;
  constexpr MapRangeFlags flags() {
    return MAP_RANGE_LESS;
  }
  operator MapIDRange() const {
    return MapIDRange{ int64_t(), max, flags() };
  }
};

struct MapIDLessEqual {
  int64_t max;
  constexpr MapRangeFlags flags() {
    return MAP_RANGE_LESS_EQUAL;
  }
  operator MapIDRange() const {
    return MapIDRange{ int64_t(), max, flags() };
  }
};

struct MapIDGreater {
  int64_t min;
  constexpr MapRangeFlags flags() {
    return MAP_RANGE_GREATER;
  }
  operator MapIDRange() const {
    return MapIDRange{ min, int64_t(), flags() };
  }
};

struct MapIDGreaterEqual {
  int64_t min;
  constexpr MapRangeFlags flags() {
    return MAP_RANGE_GREATER_EQUAL;
  }
  operator MapIDRange() const {
    return MapIDRange{ min, int64_t(), flags() };
  }
};

inline MapIDLess operator<(MapID, int64_t max) {
  return MapIDLess{ max };
}
inline MapIDLessEqual operator<=(MapID, int64_t max) {
  return MapIDLessEqual{ max };
}
inline MapIDGreater operator>(MapID, int64_t min) {
  return MapIDGreater{ min };
}
inline MapIDGreaterEqual operator>=(MapID, int64_t min) {
  return MapIDGreaterEqual{ min };
}

inline MapIDGreater operator<(int64_t min, MapID) {
  return MapIDGreater{ min };
}
inline MapIDGreaterEqual operator<=(int64_t min, MapID) {
  return MapIDGreaterEqual{ min };
}
inline MapIDLess operator>(int64_t max, MapID) {
  return MapIDLess{ max };
}
inline MapIDLessEqual operator>=(int64_t max, MapID) {
  return MapIDLessEqual{ max };
}

inline MapIDRange operator&&(MapIDLess less, MapIDGreater greater) {
  return MapIDRange{ greater.min, less.max, less.flags() | greater.flags() };
}
inline MapIDRange operator&&(MapIDLess less, MapIDGreaterEqual greater) {
  return MapIDRange{ greater.min, less.max, less.flags() | greater.flags() };
}
inline MapIDRange operator&&(MapIDLessEqual less, MapIDGreater greater) {
  return MapIDRange{ greater.min, less.max, less.flags() | greater.flags() };
}
inline MapIDRange operator&&(MapIDLessEqual less, MapIDGreaterEqual greater) {
  return MapIDRange{ greater.min, less.max, less.flags() | greater.flags() };
}
inline MapIDRange operator&&(MapIDGreater greater, MapIDLess less) {
  return less && greater;
}
inline MapIDRange operator&&(MapIDGreater greater, MapIDLessEqual less) {
  return less && greater;
}
inline MapIDRange operator&&(MapIDGreaterEqual greater, MapIDLess less) {
  return less && greater;
}
inline MapIDRange operator&&(MapIDGreaterEqual greater, MapIDLessEqual less) {
  return less && greater;
}

template <typename T> struct MapKey {};

template <typename T>
struct MapKeyRange {
  T min;
  T max;
  MapRangeFlags flags;
};

template <typename T>
struct MapKeyLess {
  T max;
  constexpr MapRangeFlags flags() {
    return MAP_RANGE_LESS;
  }
  operator MapKeyRange<T>() const {
    return MapKeyRange<T>{ int64_t(), max, flags() };
  }
};

template <typename T>
struct MapKeyLessEqual {
  T max;
  constexpr MapRangeFlags flags() {
    return MAP_RANGE_LESS_EQUAL;
  }
  operator MapKeyRange<T>() const {
    return MapKeyRange<T>{ int64_t(), max, flags() };
  }
};

template <typename T>
struct MapKeyGreater {
  T min;
  constexpr MapRangeFlags flags() {
    return MAP_RANGE_GREATER;
  }
  operator MapKeyRange<T>() const {
    return MapKeyRange<T>{ min, int64_t(), flags() };
  }
};

template <typename T>
struct MapKeyGreaterEqual {
  T min;
  constexpr MapRangeFlags flags() {
    return MAP_RANGE_GREATER_EQUAL;
  }
  operator MapKeyRange<T>() const {
    return MapKeyRange<T>{ min, int64_t(), flags() };
  }
};

template <typename T>
MapKeyLess<T> operator<(MapKey<T>, T max) {
  return MapKeyLess<T>{ max };
}
template <typename T>
MapKeyLessEqual<T> operator<=(MapKey<T>, T max) {
  return MapKeyLessEqual<T>{ max };
}
template <typename T>
MapKeyGreater<T> operator>(MapKey<T>, T min) {
  return MapKeyGreater<T>{ min };
}
template <typename T>
MapKeyGreaterEqual<T> operator>=(MapKey<T>, T min) {
  return MapKeyGreaterEqual<T>{ min };
}

template <typename T>
MapKeyGreater<T> operator<(T min, MapKey<T>) {
  return MapKeyGreater<T>{ min };
}
template <typename T>
MapKeyGreaterEqual<T> operator<=(T min, MapKey<T>) {
  return MapKeyGreaterEqual<T>{ min };
}
template <typename T>
MapKeyLess<T> operator>(T max, MapKey<T>) {
  return MapKeyLess<T>{ max };
}
template <typename T>
MapKeyLessEqual<T> operator>=(T max, MapKey<T>) {
  return MapKeyLessEqual<T>{ max };
}

template <typename T>
MapKeyRange<T> operator&&(MapKeyLess<T> less, MapKeyGreater<T> greater) {
  return MapKeyRange<T>{ greater.min, less.max,
                         less.flags() | greater.flags() };
}
template <typename T>
MapKeyRange<T> operator&&(MapKeyLess<T> less, MapKeyGreaterEqual<T> greater) {
  return MapKeyRange<T>{ greater.min, less.max,
                         less.flags() | greater.flags() };
}
template <typename T>
MapKeyRange<T> operator&&(MapKeyLessEqual<T> less, MapKeyGreater<T> greater) {
  return MapKeyRange<T>{ greater.min, less.max,
                         less.flags() | greater.flags() };
}
template <typename T>
MapKeyRange<T> operator&&(MapKeyLessEqual<T> less,
                          MapKeyGreaterEqual<T> greater) {
  return MapKeyRange<T>{ greater.min, less.max,
                         less.flags() | greater.flags() };
}
template <typename T>
MapKeyRange<T> operator&&(MapKeyGreater<T> greater, MapKeyLess<T> less) {
  return less && greater;
}
template <typename T>
MapKeyRange<T> operator&&(MapKeyGreater<T> greater, MapKeyLessEqual<T> less) {
  return less && greater;
}
template <typename T>
MapKeyRange<T> operator&&(MapKeyGreaterEqual<T> greater, MapKeyLess<T> less) {
  return less && greater;
}
template <typename T>
MapKeyRange<T> operator&&(MapKeyGreaterEqual<T> greater,
                          MapKeyLessEqual<T> less) {
  return less && greater;
}

}  // namespace alpha
}  // namespace grnxx

#endif  // GRNXX_ALPHA_MAP_RANGE_HPP
