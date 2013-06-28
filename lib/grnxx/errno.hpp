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
#ifndef GRNXX_ERRNO_HPP
#define GRNXX_ERRNO_HPP

#include "grnxx/features.hpp"

namespace grnxx {

class StringBuilder;

enum ErrnoType {
  STANDARD_ERRNO,
#ifdef GRNXX_WINDOWS
  WINDOWS_ERRNO
#endif  // GRNXX_WINDOWS
};

class GRNXX_EXPORT Errno {
 public:
  // For errno.
  explicit Errno(int error_code)
      : type_(STANDARD_ERRNO),
        standard_errno_(error_code) {}
#ifdef GRNXX_WINDOWS
  // For DWORD returned by ::GetLastErrno().
  explicit Errno(unsigned long error_code)
      : type_(WINDOWS_ERRNO),
        windows_errno_(error_code) {}
#endif  // GRNXX_WINDOWS

  // Return the errno type.
  ErrnoType type() const {
    return type_;
  }
  // Return the standard errno.
  int standard_errno() const {
    return standard_errno_;
  }
#ifdef GRNXX_WINDOWS
  // Return the windows errno.
  unsigned long windows_errno() const {
    return windows_errno_;
  }
#endif  // GRNXX_WINDOWS

  // Write a human-readable error message to "builder".
  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  ErrnoType type_;
  union {
    int standard_errno_;
#ifdef GRNXX_WINDOWS
    unsigned long windows_errno_;
#endif  // GRNXX_WINDOWS
  };
};

inline StringBuilder &operator<<(StringBuilder &builder, const Errno &error) {
  return error.write_to(builder);
}

}  // namespace grnxx

#endif  // GRNXX_ERRNO_HPP
