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
#include "grnxx/time/duration.hpp"

#include <ostream>

#include "grnxx/string_format.hpp"

namespace grnxx {

StringBuilder &Duration::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  uint64_t count;
  if (count_ >= 0) {
    count = count_;
  } else {
    builder << '-';
    count = -count_;
  }
  builder << (count / 1000000);
  count %= 1000000;
  if (count != 0) {
    builder << '.' << StringFormat::align_right(count, 6, '0');
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
