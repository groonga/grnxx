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
#ifndef GRNXX_MAP_HASH_TABLE_KEY_ARRAY_HPP
#define GRNXX_MAP_HASH_TABLE_KEY_ARRAY_HPP

#include "grnxx/features.hpp"

#include "grnxx/array.hpp"
#include "grnxx/bytes.hpp"
#include "grnxx/map/bytes_array.hpp"
#include "grnxx/types.hpp"

namespace grnxx {
namespace map {
namespace hash_table {

// Change the array size based on "T".
template <typename T, size_t T_SIZE = sizeof(T)>
struct KeyArray;

// Map<T> has at most 2^40 different keys.
template <typename T, size_t T_SIZE>
struct KeyArray {
  using Type = Array<T>;
};

// Map<T> has at most 2^8 different keys.
template <typename T>
struct KeyArray<T, 1> {
  using Type = Array<T, 256, 1, 1>;
};

// Map<T> has at most 2^16 different keys.
template <typename T>
struct KeyArray<T, 2> {
  using Type = Array<T, 256, 256, 1>;
};

// Map<T> has at most 2^32 different keys.
template <typename T>
struct KeyArray<T, 4> {
  using Type = Array<T, 65536, 256, 256>;
};

// Map<T> has at most 2^40 different keys.
template <>
struct KeyArray<Bytes> {
  using Type = BytesArray;
};

}  // namespace hash_table
}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_HASH_TABLE_KEY_ARRAY_HPP
