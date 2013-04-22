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
#include "grnxx/storage/view.hpp"

#include "grnxx/storage/view-posix.hpp"
#include "grnxx/storage/view-windows.hpp"
#include "grnxx/string_builder.hpp"

namespace grnxx {
namespace storage {

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
    GRNXX_FLAGS_WRITE(VIEW_ANONYMOUS);
    GRNXX_FLAGS_WRITE(VIEW_HUGE_TLB);
    GRNXX_FLAGS_WRITE(VIEW_READ_ONLY);
    return builder;
  } else {
    return builder << "0";
  }
}

View::View() {}
View::~View() {}

View *View::create(File *file, int64_t offset, int64_t size, ViewFlags flags) {
  return ViewImpl::create(file, offset, size, flags);
}

}  // namespace storage
}  // namespace grnxx
