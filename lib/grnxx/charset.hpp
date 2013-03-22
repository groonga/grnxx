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
#ifndef GRNXX_CHARSET_HPP
#define GRNXX_CHARSET_HPP

#include "grnxx/basic.hpp"
#include "grnxx/slice.hpp"
#include "grnxx/string_builder.hpp"

namespace grnxx {

// The values correspond to MIB enum numbers.
// Reference: http://www.iana.org/assignments/character-sets/character-sets.xml
enum CharsetCode : uint16_t {
  CHARSET_SHIFT_JIS = 17,
  CHARSET_EUC_JP    = 18,
  CHARSET_UTF_8     = 106,
  CHARSET_UNKNOWN   = 65535
};

StringBuilder &operator<<(StringBuilder &builder, CharsetCode code);

class Charset {
 public:
  Charset();
  virtual ~Charset();

  // Return a reference to a specific charset.
  static const Charset *open(CharsetCode code);

  // Return the charset code.
  virtual CharsetCode code() const = 0;

  // Return the first character of the string "slice". This function may return
  // an empty slice if "slice" is empty or an invalid sequence.
  virtual Slice get_char(const Slice &slice) const = 0;
};

}  // namespace grnxx

#endif  // GRNXX_CHARSET_HPP
