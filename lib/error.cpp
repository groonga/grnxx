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
#include "error.hpp"

#ifdef GRNXX_WINDOWS
# include <windows.h>
#endif  // GRNXX_WINDOWS

#include <ostream>

namespace grnxx {
namespace {

#ifdef GRNXX_WINDOWS
class Freer {
 public:
  void operator()(void *ptr) const {
    ::LocalFree(ptr);
  }
};
#endif  // GRNXX_WINDOWS

}  // namespace

StringBuilder &Error::write_to(StringBuilder &builder) const {
  switch (type_) {
    case POSIX_ERROR: {
      return builder << '(' << std::strerror(posix_error_) << ')';
    }
#ifdef GRNXX_WINDOWS
    case WINDOWS_ERROR: {
      char *message;
      if (::FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER |
                           FORMAT_MESSAGE_FROM_SYSTEM |
                           FORMAT_MESSAGE_IGNORE_INSERTS,
                           nullptr, windows_error_,
                           MAKELANGID(LANG_ENGLISH, SUBLANG_DEFAULT),
                           reinterpret_cast<LPSTR>(&message),
                           0, nullptr) != 0) {
        std::unique_ptr<char[], Freer> message_freer(message);
        builder << '(' << message << ')';
      } else {
        builder << "(n/a)";
      }
      return builder;
    }
#endif  // GRNXX_WINDOWS
    case GRNXX_ERROR: {
      // TODO
      // grnxx_error_
      return builder << "(undefined)";
    }
    default: {
      return builder << "(undefined)";
    }
  }
}

std::ostream &operator<<(std::ostream &stream, const Error &error) {
  char buf[64];
  StringBuilder builder(buf, STRING_BUILDER_AUTO_RESIZE);
  builder << error;
  return stream.write(builder.c_str(), builder.length());
}

}  // namespace grnxx
