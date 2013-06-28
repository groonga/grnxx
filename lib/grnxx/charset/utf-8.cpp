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

#include "grnxx/bytes.hpp"
#include "grnxx/intrinsic.hpp"

namespace grnxx {
namespace charset {

const Charset *UTF_8::get() {
  static UTF_8 singleton;
  return &singleton;
}

CharsetCode UTF_8::code() const {
  return CHARSET_UTF_8;
}

Bytes UTF_8::get_char(const Bytes &bytes) const {
  return bytes.prefix(get_char_size(bytes));
}

size_t UTF_8::get_char_size(const Bytes &bytes) const {
  if (!bytes) {
    return 0;
  }
  if (bytes[0] & 0x80) {
    // A multibyte character can be 2, 3, or 4 bytes long. Also, the 2nd,
    // 3rd, and 4th byte must be 10xxxxxx, the most significant 2 bits must
    // be 10.
    const size_t char_size =
        31 - bit_scan_reverse(~(static_cast<uint32_t>(bytes[0]) << 24));
    // Return 0 if the character is incomplete.
    if (char_size > bytes.size()) {
      return 0;
    }
    switch (char_size) {
      case 4: {
        // Return 0 if the 4th byte is invalid.
        if ((bytes[3] & 0xC0) != 0x80) {
          return 0;
        }
      }
      case 3: {
        // Return 0 if the 3rd byte is invalid.
        if ((bytes[2] & 0xC0) != 0x80) {
          return 0;
        }
      }
      case 2: {
        // Return 0 if the 2nd byte is invalid.
        if ((bytes[1] & 0xC0) != 0x80) {
          return 0;
        }
        return char_size;
      }
      default: {
        // Return 0 if the character size is invalid.
        return 0;
      }
    }
  }
  // Return 1 for an ASCII character.
  return 1;
}

}  // namespace charset
}  // namespace grnxx
