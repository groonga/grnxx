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
#include "grnxx/storage/file.hpp"

#include "grnxx/storage/file-posix.hpp"
#include "grnxx/storage/file-windows.hpp"
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

StringBuilder &operator<<(StringBuilder &builder, FileFlags flags) {
  if (flags) {
    bool is_first = true;
    GRNXX_FLAGS_WRITE(FILE_READ_ONLY);
    GRNXX_FLAGS_WRITE(FILE_TEMPORARY);
    return builder;
  } else {
    return builder << "FILE_DEFAULT";
  }
}

StringBuilder &operator<<(StringBuilder &builder, FileLockFlags flags) {
  if (flags) {
    bool is_first = true;
    GRNXX_FLAGS_WRITE(FILE_LOCK_SHARED);
    GRNXX_FLAGS_WRITE(FILE_LOCK_EXCLUSIVE);
    GRNXX_FLAGS_WRITE(FILE_LOCK_NONBLOCKING);
    return builder;
  } else {
    return builder << "0";
  }
}

File::File() {}
File::~File() {}

File *File::create(const char *path, FileFlags flags) {
  return FileImpl::create(path, flags);
}

File *File::open(const char *path, FileFlags flags) {
  return FileImpl::open(path, flags);
}

File *File::open_or_create(const char *path, FileFlags flags) {
  return FileImpl::open_or_create(path, flags);
}

bool File::exists(const char *path) {
  return FileImpl::exists(path);
}

bool File::unlink(const char *path) {
  return FileImpl::unlink(path);
}

}  // namespace storage
}  // namespace grnxx
