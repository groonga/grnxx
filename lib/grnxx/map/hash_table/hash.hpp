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
#ifndef GRNXX_MAP_HASH_HPP
#define GRNXX_MAP_HASH_HPP

#include "grnxx/features.hpp"

#include "grnxx/bytes.hpp"
#include "grnxx/geo_point.hpp"
#include "grnxx/traits.hpp"
#include "grnxx/types.hpp"

namespace grnxx {
namespace map {
namespace hash_table {

// Calculate a hash value.
template <typename T>
struct Hash;

// Use key as is.
template <>
struct Hash<int8_t> {
  using KeyArg = typename Traits<int8_t>::ArgumentType;
  uint64_t operator()(KeyArg key) const {
    return static_cast<uint8_t>(key);
  }
};

// Use key as is.
template <>
struct Hash<uint8_t> {
  using KeyArg = typename Traits<uint8_t>::ArgumentType;
  uint64_t operator()(KeyArg key) const {
    return key;
  }
};

// Use key as is.
template <>
struct Hash<int16_t> {
  using KeyArg = typename Traits<int16_t>::ArgumentType;
  uint64_t operator()(KeyArg key) const {
    return static_cast<uint16_t>(key);
  }
};

// Use key as is.
template <>
struct Hash<uint16_t> {
  using KeyArg = typename Traits<uint16_t>::ArgumentType;
  uint64_t operator()(KeyArg key) const {
    return key;
  }
};

// Murmur3's 32-bit finalizer.
template <>
struct Hash<int32_t> {
  using KeyArg = typename Traits<int32_t>::ArgumentType;
  uint64_t operator()(KeyArg key) const {
    uint32_t hash = key;
    hash ^= hash >> 16;
    hash *= 0x85ebca6b;
    hash ^= hash >> 13;
    hash *= 0xc2b2ae35;
    hash ^= hash >> 16;
    return hash;
  }
};

// Murmur3's 32-bit finalizer.
template <>
struct Hash<uint32_t> {
  using KeyArg = typename Traits<uint32_t>::ArgumentType;
  uint64_t operator()(KeyArg key) const {
    uint32_t hash = key;
    hash ^= hash >> 16;
    hash *= 0x85ebca6b;
    hash ^= hash >> 13;
    hash *= 0xC2B2AE35;
    hash ^= hash >> 16;
    return hash;
  }
};

// Murmur3's 64-bit finalizer.
template <>
struct Hash<int64_t> {
  using KeyArg = typename Traits<int64_t>::ArgumentType;
  uint64_t operator()(KeyArg key) const {
    uint64_t hash = key;
    hash ^= hash >> 33;
    hash *= 0xFF51AFD7ED558CCD;
    hash ^= hash >> 33;
    hash *= 0xC4CEB9FE1A85EC53;
    hash ^= hash >> 33;
    return hash;
  }
};

// Murmur3's 64-bit finalizer.
template <>
struct Hash<uint64_t> {
  using KeyArg = typename Traits<uint64_t>::ArgumentType;
  uint64_t operator()(KeyArg key) const {
    uint64_t hash = key;
    hash ^= hash >> 33;
    hash *= 0xFF51AFD7ED558CCD;
    hash ^= hash >> 33;
    hash *= 0xC4CEB9FE1A85EC53;
    hash ^= hash >> 33;
    return hash;
  }
};

// Murmur3's 64-bit finalizer.
template <>
struct Hash<double> {
  using KeyArg = typename Traits<double>::ArgumentType;
  uint64_t operator()(KeyArg key) const {
    return Hash<uint64_t>()(reinterpret_cast<const uint64_t &>(key));
  }
};

// Murmur3's 64-bit finalizer.
template <>
struct Hash<GeoPoint> {
  using KeyArg = typename Traits<GeoPoint>::ArgumentType;
  uint64_t operator()(KeyArg key) const {
    return Hash<uint64_t>()(key.value());
  }
};

// 64-bit FNV-1a.
template <>
struct Hash<Bytes> {
  using KeyArg = typename Traits<Bytes>::ArgumentType;
  uint64_t operator()(KeyArg key) const {
    uint64_t hash = 0xCBF29CE484222325ULL;
    for (uint64_t i = 0; i < key.size(); ++i) {
      hash ^= key[i];
      hash *= 0x100000001B3ULL;
    }
    return hash;
  }
};

}  // namespace hash_table
}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_HASH_HPP
