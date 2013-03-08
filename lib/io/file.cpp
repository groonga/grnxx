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
#include "io/file.hpp"

#include <ostream>

#include "exception.hpp"
#include "io/file-posix.hpp"
#include "io/file-windows.hpp"
#include "logger.hpp"

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

StringBuilder &operator<<(StringBuilder &builder, FileFlags flags) {
  if (flags) {
    bool is_first = true;
    GRNXX_FLAGS_WRITE(FILE_READ_ONLY);
    GRNXX_FLAGS_WRITE(FILE_WRITE_ONLY);
    GRNXX_FLAGS_WRITE(FILE_APPEND);
    GRNXX_FLAGS_WRITE(FILE_CREATE);
    GRNXX_FLAGS_WRITE(FILE_OPEN);
    GRNXX_FLAGS_WRITE(FILE_TEMPORARY);
    GRNXX_FLAGS_WRITE(FILE_TRUNCATE);
    return builder;
  } else {
    return builder << "0";
  }
}

std::ostream &operator<<(std::ostream &stream, FileFlags flags) {
  char buf[256];
  StringBuilder builder(buf);
  builder << flags;
  return stream.write(builder.c_str(), builder.length());
}

File::File() {}
File::~File() {}

File *File::open(FileFlags flags, const char *path, int permission) {
  return FileImpl::open(flags, path, permission);
}

bool File::exists(const char *path) {
  return FileImpl::exists(path);
}

void File::unlink(const char *path) {
  FileImpl::unlink(path);
}

bool File::unlink_if_exists(const char *path) {
  return FileImpl::unlink_if_exists(path);
}

}  // namespace io
}  // namespace grnxx
