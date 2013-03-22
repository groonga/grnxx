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
#include "grnxx/charset/utf-8.hpp"

#include "grnxx/intrinsic.hpp"

namespace grnxx {
namespace charset {

const Charset *UTF_8::open() {
  static UTF_8 singleton;
  return &singleton;
}

CharsetCode UTF_8::code() const {
  return CHARSET_UTF_8;
}

Slice UTF_8::get_char(const Slice &slice) const {
  if (!slice) {
    return slice;
  }
  if (slice[0] & 0x80) {
    // A multibyte character can be 2, 3, or 4 bytes long. Also, the 2nd,
    // 3rd, and 4th byte must be 10xxxxxx, the most significant 2 bits must
    // be 10.
    const size_t char_size =
        31 - bit_scan_reverse(~(static_cast<uint32_t>(slice[0]) << 24));
    // Return an empty slice if the character is incomplete.
    if (char_size > slice.size()) {
      return slice.prefix(0);
    }
    switch (char_size) {
      case 4: {
        // Return an empty slice if the 4th byte is invalid.
        if ((slice[3] & 0xC0) != 0x80) {
          return slice.prefix(0);
        }
      }
      case 3: {
        // Return an empty slice if the 3rd byte is invalid.
        if ((slice[2] & 0xC0) != 0x80) {
          return slice.prefix(0);
        }
      }
      case 2: {
        // Return an empty slice if the 2nd byte is invalid.
        if ((slice[1] & 0xC0) != 0x80) {
          return slice.prefix(0);
        }
        return slice.prefix(char_size);
      }
      default: {
        return slice.prefix(0);
      }
    }
  }
  return slice.prefix(1);
}

}  // namespace charset
}  // namespace grnxx
