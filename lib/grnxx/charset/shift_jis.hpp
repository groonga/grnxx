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
#ifndef GRNXX_CHARSET_SHIFT_JIS_HPP
#define GRNXX_CHARSET_SHIFT_JIS_HPP

#include "grnxx/features.hpp"

#include "grnxx/charset.hpp"
#include "grnxx/slice.hpp"

namespace grnxx {
namespace charset {

// Shift_JIS.
class Shift_JIS : public Charset {
 public:
  static const Charset *get();

  CharsetCode code() const;

  Slice get_char(const Slice &slice) const;
  size_t get_char_size(const Slice &slice) const;
};

}  // namespace charset
}  // namespace grnxx

#endif  // GRNXX_CHARSET_SHIFT_JIS_HPP
