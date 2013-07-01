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
#ifndef GRNXX_EXCEPTION_HPP
#define GRNXX_EXCEPTION_HPP

#include "grnxx/features.hpp"

#include <exception>

#include "grnxx/errno.hpp"

namespace grnxx {

// The base exception class.
class Exception : std::exception {
 public:
  Exception() noexcept {}
  virtual ~Exception() noexcept {}

  virtual const char *what() const noexcept {
    return "grnxx::Exception";
  }
};

// Thrown as an exception when a logical error occurs.
class LogicError : public Exception {
 public:
  LogicError() noexcept {}
  virtual ~LogicError() noexcept {}

  virtual const char *what() const noexcept {
    return "grnxx::LogicError";
  }
};

// Thrown as an exception when memory allocation fails.
class MemoryError : public Exception {
 public:
  MemoryError() noexcept {}
  virtual ~MemoryError() noexcept {}

  virtual const char *what() const noexcept {
    return "grnxx::MemoryError";
  }
};

// Thrown as an exception when a system call fails.
class SystemError : public Exception {
 public:
  SystemError(const Errno &code) noexcept : code_(code) {}
  virtual ~SystemError() noexcept {}

  const Errno &code() const noexcept {
    return code_;
  }
  virtual const char *what() const noexcept {
    return "grnxx::SystemError";
  }

 private:
  Errno code_;
};

}  // namespace grnxx

#endif  // GRNXX_EXCEPTION_HPP
