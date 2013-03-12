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
#include "charset/euc-jp.hpp"

namespace grnxx {
namespace charset {

const Charset *EUC_JP::open() {
  static EUC_JP singleton;
  return &singleton;
}

CharsetCode EUC_JP::code() const {
  return CHARSET_EUC_JP;
}

Slice EUC_JP::get_char(const Slice &slice) const {
  if (!slice) {
    return slice;
  }
  // Reference: http://ja.wikipedia.org/wiki/EUC-JP
  if (slice[0] & 0x80) {
    // 3-byte characters start with 0x8F.
    if (slice[0] == 0x8F) {
      // Return an empty slice if the character is incomplete.
      if (slice.size() < 3) {
        return slice.prefix(0);
      }
      // Return an empty slice if the 2nd byte is invalid.
      // In fact, only bytes in [A1, A8], [B0, ED], and [F3, FE] are valid.
      if (static_cast<unsigned>(slice[1] - 0xA1) > (0xFE - 0xA1)) {
        return slice.prefix(0);
      }
      // Return an empty slice if the 3rd byte is invalid.
      if (static_cast<unsigned>(slice[2] - 0xA1) > (0xFE - 0xA1)) {
        return slice.prefix(0);
      }
      return slice.prefix(3);
    } else {
      // Return an empty slice if the 1st byte is invalid.
      // In fact, only bytes in [A1, A8], [AD, AD], and [B0, FE] are valid.
      if (static_cast<unsigned>(slice[0] - 0xA1) > (0xFE - 0xA1)) {
        return slice.prefix(0);
      }
      // Return an empty slice if the character is incomplete.
      if (slice.size() < 2) {
        return slice.prefix(0);
      }
      // Return an empty slice if the 2nd byte is invalid.
      if (static_cast<unsigned>(slice[1] - 0xA1) > (0xFE - 0xA1)) {
        return slice.prefix(0);
      }
      return slice.prefix(2);
    }
  }
  return slice.prefix(1);
}

}  // namespace charset
}  // namespace grnxx
