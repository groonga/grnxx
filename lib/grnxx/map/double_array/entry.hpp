/*
  Copyright (C) 2012-2013  Brazil, Inc.

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
#ifndef GRNXX_MAP_DOUBLE_ARRAY_ENTRY_HPP
#define GRNXX_MAP_DOUBLE_ARRAY_ENTRY_HPP

#include "grnxx/features.hpp"

#include "grnxx/types.hpp"

namespace grnxx {
namespace map {
namespace double_array {

// The internal structure is as follows:
// - Common
//      63 ( 1): is_valid
// - Valid: is_valid
//    0-62 (63): bytes_id
// - Invalid: !is_valid
//    0-62 (63): next
// where 0 is the LSB and 63 is the MSB.

class Entry {
  static constexpr uint64_t IS_VALID_FLAG = 1ULL << 63;

 public:
  static Entry valid_entry(uint64_t bytes_id) {
    return Entry(IS_VALID_FLAG | bytes_id);
  }
  static Entry invalid_entry(uint64_t next) {
    return Entry(next);
  }

  // Return true iff the entry is associated with a key.
  explicit operator bool() const {
    return value_ & IS_VALID_FLAG;
  }

  // Return the ID of the associated byte sequence.
  uint64_t bytes_id() const {
    return value_ & ~IS_VALID_FLAG;
  }
  // Return the next invalid entry.
  uint64_t next() const {
    return value_;
  }

 private:
  uint64_t value_;

  explicit Entry(uint64_t value) : value_(value) {}
};

}  // namespace double_array
}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_DOUBLE_ARRAY_ENTRY_HPP
