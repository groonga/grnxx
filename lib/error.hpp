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
#ifndef GRNXX_ERROR_HPP
#define GRNXX_ERROR_HPP

#include "basic.hpp"
#include "string_builder.hpp"

namespace grnxx {

enum ErrorType {
  POSIX_ERROR,
#ifdef GRNXX_WINDOWS
  WINDOWS_ERROR,
#endif  // GRNXX_WINDOWS
  GRNXX_ERROR
};

enum ErrorCode {
  // TODO
};

class Error {
 public:
  // For errno.
  explicit constexpr Error(int error_code)
    : type_(POSIX_ERROR), posix_error_(error_code) {}
#ifdef GRNXX_WINDOWS
  // For DWORD returned by ::GetLastError().
  explicit constexpr Error(unsigned long error_code)
    : type_(WINDOWS_ERROR), windows_error_(error_code) {}
#endif  // GRNXX_WINDOWS
  explicit constexpr Error(ErrorCode error_code)
    : type_(GRNXX_ERROR), grnxx_error_(error_code) {}

  constexpr ErrorType type() const {
    return type_;
  }
  constexpr int posix_error() const {
    return posix_error_;
  }
#ifdef GRNXX_WINDOWS
  constexpr unsigned long windows_error() const {
    return windows_error_;
  }
#endif  // GRNXX_WINDOWS
  constexpr ErrorCode grnxx_error() const {
    return grnxx_error_;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  ErrorType type_;
  union {
    int posix_error_;
#ifdef GRNXX_WINDOWS
    unsigned long windows_error_;
#endif  // GRNXX_WINDOWS
    ErrorCode grnxx_error_;
  };

  // Copyable.
};

inline StringBuilder &operator<<(StringBuilder &builder, const Error &error) {
  return error.write_to(builder);
}

std::ostream &operator<<(std::ostream &stream, const Error &error);

}  // namespace grnxx

#endif  // GRNXX_ERROR_HPP
