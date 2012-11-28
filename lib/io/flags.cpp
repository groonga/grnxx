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
#include "flags.hpp"

#include <ostream>

namespace grnxx {
namespace io {

#define GRNXX_FLAGS_WRITE(flag) do { \
  if (flags & flag) { \
    if (!is_first) { \
      builder << " | "; \
    } \
    builder << #flag; \
    is_first = false; \
  } \
} while (false)

StringBuilder &operator<<(StringBuilder &builder, Flags flags) {
  if (flags) {
    bool is_first = true;
    GRNXX_FLAGS_WRITE(GRNXX_IO_READ_ONLY);
    GRNXX_FLAGS_WRITE(GRNXX_IO_WRITE_ONLY);
    GRNXX_FLAGS_WRITE(GRNXX_IO_ANONYMOUS);
    GRNXX_FLAGS_WRITE(GRNXX_IO_APPEND);
    GRNXX_FLAGS_WRITE(GRNXX_IO_CREATE);
    GRNXX_FLAGS_WRITE(GRNXX_IO_HUGE_TLB);
    GRNXX_FLAGS_WRITE(GRNXX_IO_OPEN);
    GRNXX_FLAGS_WRITE(GRNXX_IO_TEMPORARY);
    GRNXX_FLAGS_WRITE(GRNXX_IO_TRUNCATE);
    GRNXX_FLAGS_WRITE(GRNXX_IO_PRIVATE);
    GRNXX_FLAGS_WRITE(GRNXX_IO_SHARED);
    return builder;
  } else {
    return builder << "0";
  }
}

std::ostream &operator<<(std::ostream &stream, Flags flags) {
  char buf[256];
  StringBuilder builder(buf);
  builder << flags;
  return stream.write(builder.c_str(), builder.length());
}

}  // namespace io
}  // namespace grnxx
