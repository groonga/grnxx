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
#ifndef GRNXX_CHARSET_EUC_JP_HPP
#define GRNXX_CHARSET_EUC_JP_HPP

#include "grnxx/features.hpp"

#include "grnxx/bytes.hpp"
#include "grnxx/charset.hpp"

namespace grnxx {
namespace charset {

// EUC-JP: Extended_UNIX_Code_Packed_Format_for_Japanese.
class EUC_JP : public Charset {
 public:
  static const Charset *get();

  CharsetCode code() const;

  Bytes get_char(const Bytes &bytes) const;
  size_t get_char_size(const Bytes &bytes) const;
};

}  // namespace charset
}  // namespace grnxx

#endif  // GRNXX_CHARSET_EUC_JP_HPP
