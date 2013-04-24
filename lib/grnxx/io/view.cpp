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
#include "grnxx/io/view.hpp"

#include <ostream>

#include "grnxx/exception.hpp"
#include "grnxx/io/view-posix.hpp"
#include "grnxx/io/view-windows.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/string_builder.hpp"

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

StringBuilder &operator<<(StringBuilder &builder, ViewFlags flags) {
  if (flags) {
    bool is_first = true;
    GRNXX_FLAGS_WRITE(VIEW_READ_ONLY);
    GRNXX_FLAGS_WRITE(VIEW_WRITE_ONLY);
    GRNXX_FLAGS_WRITE(VIEW_ANONYMOUS);
    GRNXX_FLAGS_WRITE(VIEW_HUGE_TLB);
    GRNXX_FLAGS_WRITE(VIEW_PRIVATE);
    GRNXX_FLAGS_WRITE(VIEW_SHARED);
    return builder;
  } else {
    return builder << "0";
  }
}

std::ostream &operator<<(std::ostream &stream, ViewFlags flags) {
  char buf[256];
  StringBuilder builder(buf);
  builder << flags;
  return stream.write(builder.c_str(), builder.length());
}

View::View() {}
View::~View() {}

View *View::open(ViewFlags flags, uint64_t size) {
  return ViewImpl::open(flags, size);
}

View *View::open(ViewFlags flags, File *file) {
  return ViewImpl::open(flags, file);
}

View *View::open(ViewFlags flags, File *file,
                 uint64_t offset, uint64_t size) {
  return ViewImpl::open(flags, file, offset, size);
}

}  // namespace io
}  // namespace grnxx
