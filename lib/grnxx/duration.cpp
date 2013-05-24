/*
  Copyright (C) 2012-2013  Brazil, Inc.

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
#include "grnxx/duration.hpp"

#include "grnxx/string_builder.hpp"
#include "grnxx/string_format.hpp"

namespace grnxx {

StringBuilder &operator<<(StringBuilder &builder, Duration duration) {
  if (!builder) {
    return builder;
  }
  uint64_t count;
  if (duration.count() >= 0) {
    count = duration.count();
  } else {
    builder << '-';
    count = -duration.count();
  }
  builder << (count / 1000000);
  count %= 1000000;
  if (count != 0) {
    builder << '.' << StringFormat::align_right(count, 6, '0');
  }
  return builder;
}

}  // namespace grnxx
