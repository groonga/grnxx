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
#include "grnxx/time/broken_down_time.hpp"

#include <iostream>

#include "grnxx/string_format.hpp"

namespace grnxx {

StringBuilder &BrokenDownTime::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  builder << (1900 + year) << '-'
          << StringFormat::align_right(mon + 1, 2, '0') << '-'
          << StringFormat::align_right(mday, 2, '0') << ' '
          << StringFormat::align_right(hour, 2, '0') << ':'
          << StringFormat::align_right(min, 2, '0') << ':'
          << StringFormat::align_right(sec, 2, '0') << '.'
          << StringFormat::align_right(usec, 6, '0');
  return builder;
}

std::ostream &operator<<(std::ostream &stream, const BrokenDownTime &time) {
  char buf[32];
  StringBuilder builder(buf);
  builder << time;
  return stream.write(builder.c_str(), builder.length());
}

}  // namespace grnxx