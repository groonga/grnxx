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
#include "grnxx/storage/path.hpp"

#ifdef GRNXX_WINDOWS
# include <stdlib.h>
#else  // GRNXX_WINDOWS
# include <unistd.h>
#endif  // GRNXX_WINDOWS

#include <cerrno>
#include <cstdlib>
#include <random>

#include "grnxx/error.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/string_builder.hpp"

namespace grnxx {
namespace storage {
namespace {

constexpr size_t MAX_PATH_LENGTH = 1023;

}  // namespace

char *Path::full_path(const char *path) {
  if (!path) {
    path = "";
  }
  char full_path[MAX_PATH_LENGTH + 1];
  size_t full_path_length = 0;
#ifdef GRNXX_WINDOWS
  if (!::_fullpath(full_path, path, sizeof(full_path))) {
    GRNXX_ERROR() << "failed to generate full path: path = " << path
                  << ": '::_fullpath' " << Error(errno);
    return nullptr;
  }
  full_path_length = std::strlen(full_path);
#else  // GRNXX_WINDOWS
  if (path[0] != '/') {
    if (!::getcwd(full_path, sizeof(full_path))) {
      GRNXX_ERROR() << "failed to get current working directory: '::getcwd' "
                    << Error(errno);
      return nullptr;
    }
    full_path_length = std::strlen(full_path);
    full_path[full_path_length++] = '/';
  }

  const size_t path_length = std::strlen(path);
  if ((full_path_length + path_length) >= sizeof(full_path)) {
    GRNXX_ERROR() << "failed to generate full path: path = " << path
                  << ": too long";
    return nullptr;
  }
  std::memcpy(full_path + full_path_length, path, path_length);
  full_path_length += path_length;
  full_path[full_path_length] = '\0';

  size_t src = 0;
  size_t dest = 0;
  while (full_path[src] != '\0') {
    if (full_path[src] == '/') {
      if (full_path[src + 1] == '\0') {
        full_path[dest++] = full_path[src++];
        break;
      } else if (full_path[src + 1] == '/') {
        ++src;
        continue;
      } else if (full_path[src + 1] == '.') {
        if ((full_path[src + 2] == '/') || (full_path[src + 2] == '\0')) {
          src += 2;
          continue;
        } else if (full_path[src + 2] == '.') {
          if ((full_path[src + 3] == '/') || (full_path[src + 3] == '\0')) {
            if (dest > 0) {
              do {
                --dest;
              } while (full_path[dest] != '/');
            }
            src += 3;
            continue;
          }
        }
      }
    }
    full_path[dest++] = full_path[src++];
  }
  if (dest == 0) {
    full_path[0] = '/';
    full_path_length = 1;
  } else {
    full_path_length = dest;
  }
#endif  // GRNXX_WINDOWS
  char * const buf = new (std::nothrow) char[full_path_length + 1];
  std::memcpy(buf, full_path, full_path_length);
  buf[full_path_length] = '\0';
  return buf;
}

char *Path::unique_path(const char *prefix) {
  if (!prefix) {
    prefix = "";
  }
  constexpr size_t SUFFIX_LENGTH = 8;
  const size_t prefix_length = std::strlen(prefix);
  const size_t path_size = prefix_length + SUFFIX_LENGTH + 2;
  char * const path(new (std::nothrow) char[path_size]);
  if (!path) {
    GRNXX_ERROR() << "new char[] failed: path_size = " << path_size;
    return nullptr;
  }
  std::memcpy(path, prefix, prefix_length);
  path[prefix_length] = '_';
  std::random_device random;
  for (size_t i = 0; i < SUFFIX_LENGTH; ++i) {
    const int value = static_cast<int>(random() % 36);
    path[prefix_length + 1 + i] =
        (value < 10) ? ('0' + value) : ('A' + value - 10);
  }
  path[prefix_length + 1 + SUFFIX_LENGTH] = '\0';
  return path;
}

}  // namespace storage
}  // namespace grnxx
