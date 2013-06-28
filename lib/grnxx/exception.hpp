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

// This exception is thrown when a file operation fails.
class FileError : public Exception {
 public:
  FileError() noexcept {}
  virtual ~FileError() noexcept {}

  virtual const char *what() const noexcept {
    return "grnxx::FileError";
  }
};

// This exception is thrown when memory mapping fails.
class MmapError : public Exception {
 public:
  MmapError() noexcept {}
  virtual ~MmapError() noexcept {}

  virtual const char *what() const noexcept {
    return "grnxx::MmapError";
  }
};

// This exception is thrown when memory allocation fails.
class MemoryError : public Exception {
 public:
  MemoryError() noexcept {}
  virtual ~MemoryError() noexcept {}

  virtual const char *what() const noexcept {
    return "grnxx::MemoryError";
  }
};

}  // namespace grnxx

#endif  // GRNXX_EXCEPTION_HPP
