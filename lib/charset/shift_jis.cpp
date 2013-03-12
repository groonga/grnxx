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
#include "charset/shift_jis.hpp"

namespace grnxx {
namespace charset {

const Charset *Shift_JIS::open() {
  static Shift_JIS singleton;
  return &singleton;
}

CharsetCode Shift_JIS::code() const {
  return CHARSET_SHIFT_JIS;
}

Slice Shift_JIS::get_char(const Slice &slice) const {
  if (!slice) {
    return slice;
  }
  // The 1st byte of a multibyte character is in [81, 9F] or [E0, FC].
  // Reference: http://www.st.rim.or.jp/~phinloda/cqa/cqa15.html#Q4
  if (static_cast<unsigned>((slice[0] ^ 0x20) - 0xA1) < 0x3C) {
    // Return an empty slice if the character is incomplete.
    if (slice.size() < 2) {
      return slice.prefix(0);
    }
    // Return an empty slice if the 2nd byte is invalid.
    if (static_cast<unsigned>(slice[1] - 0x40) > (0xFC - 0x40)) {
      return slice.prefix(0);
    }
    return slice.prefix(2);
  }
  return slice.prefix(1);
}

}  // namespace charset
}  // namespace grnxx
