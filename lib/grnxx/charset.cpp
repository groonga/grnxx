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
#include "grnxx/charset.hpp"

#include "grnxx/charset/euc-jp.hpp"
#include "grnxx/charset/shift_jis.hpp"
#include "grnxx/charset/utf-8.hpp"

namespace grnxx {

StringBuilder &operator<<(StringBuilder &builder, CharsetCode code) {
  switch (code) {
    case CHARSET_SHIFT_JIS: {
      return builder << "Shift_JIS";
    }
    case CHARSET_EUC_JP: {
      return builder << "EUC-JP";
    }
    case CHARSET_UTF_8: {
      return builder << "UTF-8";
    }
    case CHARSET_UNKNOWN: {
      break;
    }
  }
  return builder << "n/a";
}

Charset::Charset() {}
Charset::~Charset() {}

const Charset *Charset::get(CharsetCode code) {
  switch (code) {
    case CHARSET_SHIFT_JIS: {
      return charset::Shift_JIS::get();
    }
    case CHARSET_EUC_JP: {
      return charset::EUC_JP::get();
    }
    case CHARSET_UTF_8: {
      return charset::UTF_8::get();
    }
    case CHARSET_UNKNOWN: {
      break;
    }
  }
  // TODO: Error handling.
  return nullptr;
}

}  // namespace grnxx
