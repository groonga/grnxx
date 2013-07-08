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
#ifndef GRNXX_COMMON_HEADER_HPP
#define GRNXX_COMMON_HEADER_HPP

#include "grnxx/features.hpp"

#include "grnxx/bytes.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class CommonHeader {
 public:
  // Buffer size for "format_" and "version_".
  static constexpr size_t FORMAT_SIZE  = 64;
  static constexpr size_t VERSION_SIZE = 32;

  // Trivial default constructor.
  CommonHeader() = default;
  // Create a common header with "format" and the current version.
  explicit CommonHeader(const char *format);

  // Return the format string.
  Bytes format() const;
  // Return the version string.
  Bytes version() const;

 private:
  char format_[FORMAT_SIZE];
  char version_[VERSION_SIZE];
};

}  // namespace grnxx

#endif  // GRNXX_COMMON_HEADER_HPP
