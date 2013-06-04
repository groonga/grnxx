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
#ifndef GRNXX_MAP_HELPER_HPP
#define GRNXX_MAP_HELPER_HPP

#include "grnxx/features.hpp"

#include <cmath>
#include <cstring>
#include <limits>
#include <type_traits>

#include "grnxx/array.hpp"
#include "grnxx/bytes.hpp"
#include "grnxx/map/bytes_array.hpp"
#include "grnxx/traits.hpp"

namespace grnxx {
namespace map {

// Normalize a key.
template <typename T,
          bool IS_FLOATING_POINT = std::is_floating_point<T>::value>
struct NormalizeHelper;
// Do nothing if "T" is not a floating point type.
template <typename T>
struct NormalizeHelper<T, false> {
  using Key = typename Traits<T>::Type;
  using KeyArg = typename Traits<T>::ArgumentType;

  // Return "key" as is.
  static Key normalize(KeyArg key) {
    return key;
  }
};
// Normalize a floating point number.
template <typename T>
struct NormalizeHelper<T, true> {
  using Key = typename Traits<T>::Type;
  using KeyArg = typename Traits<T>::ArgumentType;

  // Return the normalized NaN iff "key" is a NaN.
  // Return +0.0 iff "key" is +/-0.0.
  // Return "key" as is otherwise.
  static Key normalize(KeyArg key) {
    return std::isnan(key) ? std::numeric_limits<Key>::quiet_NaN() :
        (key != 0.0 ? key : 0.0);
  }
};

// Compare keys.
template <typename T,
          bool IS_FLOATING_POINT = std::is_floating_point<T>::value>
struct EqualToHelper;
// Compare keys with operator==() if "T" is not a floating point type.
template <typename T>
struct EqualToHelper<T, false> {
  using Key = typename Traits<T>::Type;
  using KeyArg = typename Traits<T>::ArgumentType;

  static bool equal_to(KeyArg lhs, KeyArg rhs) {
    return lhs == rhs;
  }
};
// Compare keys as sequences of bytes.
template <typename T>
struct EqualToHelper<T, true> {
  using Key = typename Traits<T>::Type;
  using KeyArg = typename Traits<T>::ArgumentType;

  static bool equal_to(KeyArg lhs, KeyArg rhs) {
    return std::memcmp(&lhs, &rhs, sizeof(Key)) == 0;
  }
};

template <typename T>
struct Helper {
  using Key = typename Traits<T>::Type;
  using KeyArg = typename Traits<T>::ArgumentType;

  static Key normalize(KeyArg key) {
    return NormalizeHelper<T>::normalize(key);
  }
  static bool equal_to(KeyArg lhs, KeyArg rhs) {
    return EqualToHelper<T>::equal_to(lhs, rhs);
  }
};

}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_HELPER_HPP
