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
#include "grnxx/errno.hpp"

#include <string.h>

#ifdef GRNXX_WINDOWS
# include <windows.h>
#endif  // GRNXX_WINDOWS

#include <memory>

#include "grnxx/string_builder.hpp"

namespace grnxx {
namespace {

constexpr size_t MESSAGE_BUF_SIZE = 256;

#ifdef GRNXX_WINDOWS
class Freer {
 public:
  void operator()(void *ptr) const {
    ::LocalFree(ptr);
  }
};
#endif  // GRNXX_WINDOWS

}  // namespace

StringBuilder &Errno::write_to(StringBuilder &builder) const {
  switch (type_) {
    case STANDARD_ERRNO: {
      // Note that a string returned by ::strerror() may be modified by a
      // subsequent call to ::perror() or ::strerror().
      const char *message = "n/a";
#ifdef GRNXX_MSC
      char message_buf[MESSAGE_BUF_SIZE];
      if (::strerror_s(message_buf, MESSAGE_BUF_SIZE, standard_errno_) == 0) {
        message = message_buf;
      }
#elif defined(GRNXX_HAS_XSI_STRERROR)
      char message_buf[MESSAGE_BUF_SIZE];
      if (::strerror_r(standard_errno_, message_buf, MESSAGE_BUF_SIZE) == 0) {
        message = message_buf;
      }
#elif defined(GRNXX_HAS_GNU_STRERROR)
      // ::strerror_r() may not use "message_buf" when an immutable error
      // message is available.
      char message_buf[MESSAGE_BUF_SIZE];
      message = ::strerror_r(standard_errno_, message_buf, MESSAGE_BUF_SIZE);
#else  // defined(GRNXX_HAS_GNU_STRERROR)
      message = ::strerror(standard_errno_);
#endif  // defined(GRNXX_HAS_GNU_STRERROR)
      return builder << standard_errno_ << " (" << message << ')';
    }
#ifdef GRNXX_WINDOWS
    case WINDOWS_ERRNO: {
      char *message;
      if (::FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER |
                           FORMAT_MESSAGE_FROM_SYSTEM |
                           FORMAT_MESSAGE_IGNORE_INSERTS,
                           nullptr, windows_errno_,
                           MAKELANGID(LANG_ENGLISH, SUBLANG_DEFAULT),
                           reinterpret_cast<LPSTR>(&message),
                           0, nullptr) != 0) {
        std::unique_ptr<char[], Freer> message_freer(message);
        builder << windows_errno_ << " (" << message << ')';
      } else {
        builder << windows_errno << " (n/a)";
      }
      return builder;
    }
#endif  // GRNXX_WINDOWS
    default: {
      return builder << "n/a";
    }
  }
}

}  // namespace grnxx
