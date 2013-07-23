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
#ifndef GRNXX_MAP_HEADER_HPP
#define GRNXX_MAP_HEADER_HPP

#include "grnxx/bytes.hpp"
#include "grnxx/common_header.hpp"
#include "grnxx/map.hpp"

namespace grnxx {
namespace map {

class CommonHeader {
 public:
  // Create a common header with "format", the current version, and "type".
  CommonHeader(const char *format, MapType type);

  // Return true iff the header seems to be correct.
  explicit operator bool() const;

  // Return the format string.
  Bytes format() const {
    return common_header_.format();
  }
  // Return the version string.
  Bytes version() const {
    return common_header_.version();
  }
  // Return the implementation type.
  MapType type() const {
    return type_;
  }

 private:
  grnxx::CommonHeader common_header_;
  MapType type_;
};

}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_HEADER_HPP
