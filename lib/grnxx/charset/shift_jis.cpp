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
#include "grnxx/charset/shift_jis.hpp"

namespace grnxx {
namespace charset {

const Charset *Shift_JIS::get() {
  static Shift_JIS singleton;
  return &singleton;
}

CharsetCode Shift_JIS::code() const {
  return CHARSET_SHIFT_JIS;
}

Slice Shift_JIS::get_char(const Slice &slice) const {
  return slice.prefix(get_char_size(slice));
}

size_t Shift_JIS::get_char_size(const Slice &slice) const {
  if (!slice) {
    return 0;
  }
  // The 1st byte of a multibyte character is in [81, 9F] or [E0, FC].
  // Reference: http://www.st.rim.or.jp/~phinloda/cqa/cqa15.html#Q4
  if (static_cast<unsigned>((slice[0] ^ 0x20) - 0xA1) < 0x3C) {
    // Return 0 if the character is incomplete.
    if (slice.size() < 2) {
      return 0;
    }
    // Return 0 if the 2nd byte is invalid.
    if (static_cast<unsigned>(slice[1] - 0x40) > (0xFC - 0x40)) {
      return 0;
    }
    return 2;
  }
  // Return 1 for an ASCII character.
  return 1;
}

}  // namespace charset
}  // namespace grnxx
