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
#include "grnxx/io/path.hpp"

#include <cerrno>
#include <cstdlib>
#include <random>

#include "grnxx/error.hpp"
#include "grnxx/exception.hpp"
#include "grnxx/logger.hpp"

namespace grnxx {
namespace io {
namespace {

class Freer {
 public:
  void operator()(void *ptr) const {
    std::free(ptr);
  }
};

}  // namespace

String Path::full_path(const char *path) {
  if (!path) {
    path = "";
  }

#ifdef GRNXX_WINDOWS
  std::unique_ptr<char[], Freer> full_path(::_fullpath(nullptr, path, 0));
  if (!full_path) {
    GRNXX_ERROR() << "failed to generate full path: path = " << path
                  << ": '::_fullpath' " << Error(errno);
    GRNXX_THROW();
  }
  return String(full_path.get());
#else  // GRNXX_WINDOWS
  StringBuilder full_path(STRING_BUILDER_AUTO_RESIZE);
  if (path[0] != '/') {
    std::unique_ptr<char[], Freer> working_directory(::getcwd(nullptr, 0));
    if (!working_directory) {
      GRNXX_ERROR() << "failed to get working directory: '::getcwd' "
                    << Error(errno);
      GRNXX_THROW();
    }
    full_path << working_directory.get() << '/';
  }
  full_path << path;
  if (!full_path) {
    GRNXX_ERROR() << "failed to generate full path: path = " << path;
    GRNXX_THROW();
  }

  size_t src = 0, dest = 0;
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
    return "/";
  }
  return String(full_path.c_str(), dest);
#endif  // GRNXX_WINDOWS
}

String Path::unique_path(const char *path) {
  if (!path) {
    path = "";
  }

  const size_t SUFFIX_LENGTH = 8;

  const size_t length = std::strlen(path);
  StringBuilder unique_path(length + SUFFIX_LENGTH + 2);
  unique_path.append(path, length);
  unique_path.append('_');

  std::random_device random;
  for (size_t i = 0; i < SUFFIX_LENGTH; ++i) {
    const int value = static_cast<int>(random() % 36);
    unique_path.append((value < 10) ? ('0' + value) : ('A' + value - 10));
  }

  if (!unique_path) {
    GRNXX_ERROR() << "failed to generate unique path: path = " << path;
    GRNXX_THROW();
  }
  return unique_path.str();
}

}  // namespace io
}  // namespace grnxx
