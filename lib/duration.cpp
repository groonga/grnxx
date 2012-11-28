/*
  Copyright (C) 2012  Brazil, Inc.

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
#include "duration.hpp"

#include <ostream>

#include "string_format.hpp"

namespace grnxx {

StringBuilder &Duration::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  uint64_t nanoseconds;
  if (nanoseconds_ >= 0) {
    nanoseconds = nanoseconds_;
  } else {
    builder << '-';
    nanoseconds = -nanoseconds_;
  }
  builder << (nanoseconds / 1000000000);
  nanoseconds %= 1000000000;
  if (nanoseconds != 0) {
    builder << '.' << StringFormat::align_right(nanoseconds, 9, '0');
  }
  return builder;
}

std::ostream &operator<<(std::ostream &stream, Duration duration) {
  char buf[32];
  StringBuilder builder(buf);
  builder << duration;
  return stream.write(builder.c_str(), builder.length());
}

}  // namespace grnxx
