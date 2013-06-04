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
#ifndef GRNXX_MAP_ARRAY_MAP_BIT_ARRAY_HPP
#define GRNXX_MAP_ARRAY_MAP_BIT_ARRAY_HPP

#include "grnxx/features.hpp"

#include "grnxx/array.hpp"
#include "grnxx/types.hpp"

namespace grnxx {
namespace map {
namespace array_map {

// Change the array size based on the size of "T".
template <typename T, size_t T_SIZE = sizeof(T)>
struct BitArray;

// Map<T> has at most 2^40 different keys.
template <typename T, size_t T_SIZE>
struct BitArray {
  using Type = Array<bool, 65536, 4096, 4096>;
};

// Map<T> has at most 2^8 different keys.
template <typename T>
struct BitArray<T, 1> {
  using Type = Array<bool, 256, 1, 1>;
};

// Map<T> has at most 2^16 different keys.
template <typename T>
struct BitArray<T, 2> {
  using Type = Array<bool, 256, 256, 1>;
};

// Map<T> has at most 2^32 different keys.
template <typename T>
struct BitArray<T, 4> {
  using Type = Array<bool, 16384, 512, 512>;
};

}  // namespace array_map
}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_ARRAY_MAP_BIT_ARRAY_HPP
