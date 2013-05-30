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
#include "grnxx/charset/euc-jp.hpp"

namespace grnxx {
namespace charset {

const Charset *EUC_JP::get() {
  static EUC_JP singleton;
  return &singleton;
}

CharsetCode EUC_JP::code() const {
  return CHARSET_EUC_JP;
}

Bytes EUC_JP::get_char(const Bytes &bytes) const {
  return bytes.prefix(get_char_size(bytes));
}

size_t EUC_JP::get_char_size(const Bytes &bytes) const {
  if (!bytes) {
    return 0;
  }
  // Reference: http://ja.wikipedia.org/wiki/EUC-JP
  if (bytes[0] & 0x80) {
    // A 3-byte character starts with 0x8F.
    if (bytes[0] == 0x8F) {
      // Return 0 if the character is incomplete.
      if (bytes.size() < 3) {
        return 0;
      }
      // Return 0 if the 2nd byte is invalid.
      // In fact, only bytes in [A1, A8], [B0, ED], and [F3, FE] are valid.
      if (static_cast<unsigned>(bytes[1] - 0xA1) > (0xFE - 0xA1)) {
        return 0;
      }
      // Return 0 if the 3rd byte is invalid.
      if (static_cast<unsigned>(bytes[2] - 0xA1) > (0xFE - 0xA1)) {
        return 0;
      }
      return 3;
    } else {
      // Return 0 if the 1st byte is invalid.
      // In fact, only bytes in [A1, A8], [AD, AD], and [B0, FE] are valid.
      if (static_cast<unsigned>(bytes[0] - 0xA1) > (0xFE - 0xA1)) {
        return 0;
      }
      // Return 0 if the character is incomplete.
      if (bytes.size() < 2) {
        return 0;
      }
      // Return 0 if the 2nd byte is invalid.
      if (static_cast<unsigned>(bytes[1] - 0xA1) > (0xFE - 0xA1)) {
        return 0;
      }
      return 2;
    }
  }
  // Return 1 for an ASCII character.
  return 1;
}

Slice EUC_JP::get_char(const Slice &slice) const {
  return slice.prefix(get_char_size(slice));
}

size_t EUC_JP::get_char_size(const Slice &slice) const {
  return get_char_size(Bytes(slice.ptr(), slice.size()));
}

}  // namespace charset
}  // namespace grnxx
