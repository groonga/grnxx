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

// Calculate a hash value.
template <typename T>
struct Hash;

// Use Murmur3's 64-bit finalizer for integers.
template <>
struct Hash<uint64_t> {
  uint64_t operator()(uint64_t key) const {
    uint64_t hash = key;
    hash ^= hash >> 33;
    hash *= 0xFF51AFD7ED558CCDULL;
    hash ^= hash >> 33;
    hash *= 0xC4CEB9FE1A85EC53ULL;
    hash ^= hash >> 33;
    return hash;
  }
};

template <>
struct Hash<int64_t> {
  uint64_t operator()(int64_t key) const {
    return Hash<uint64_t>()(key);
  }
};

template <>
struct Hash<uint32_t> {
  uint64_t operator()(uint32_t key) const {
    return Hash<uint64_t>()(key);
  }
};

template <>
struct Hash<int32_t> {
  uint64_t operator()(int32_t key) const {
    return Hash<uint32_t>()(key);
  }
};

template <>
struct Hash<uint16_t> {
  uint64_t operator()(uint16_t key) const {
    return Hash<uint64_t>()(key);
  }
};

template <>
struct Hash<int16_t> {
  uint64_t operator()(int16_t key) const {
    return Hash<uint16_t>()(key);
  }
};

template <>
struct Hash<uint8_t> {
  uint64_t operator()(uint8_t key) const {
    return Hash<uint64_t>()(key);
  }
};

template <>
struct Hash<int8_t> {
  uint64_t operator()(int8_t key) const {
    return Hash<uint8_t>()(key);
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

}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_HASH_HPP
