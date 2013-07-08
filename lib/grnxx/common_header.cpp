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
#include "grnxx/common_header.hpp"

#include <cstring>

#include "grnxx/exception.hpp"
#include "grnxx/grnxx.hpp"
#include "grnxx/logger.hpp"

namespace grnxx {

CommonHeader::CommonHeader(const char *format) : format_{}, version_{} {
  // Copy the format string.
  if (!format) {
    GRNXX_ERROR() << "invalid format: format = nullptr";
    throw LogicError();
  }
  const std::size_t format_length = std::strlen(format);
  if (format_length >= FORMAT_SIZE) {
    GRNXX_ERROR() << "too long format: format = " << format;
    throw LogicError();
  }
  std::memcpy(format_, format, format_length);

  // Copy the current version string.
  const char * const current_version = Grnxx::version();
  if (!current_version) {
    GRNXX_ERROR() << "invalid version: current_version = nullptr";
    throw LogicError();
  }
  const std::size_t current_version_length = std::strlen(current_version);
  if (current_version_length >= VERSION_SIZE) {
    GRNXX_ERROR() << "too long version: current_version = " << current_version;
    throw LogicError();
  }
  std::memcpy(version_, current_version, current_version_length);
}

Bytes CommonHeader::format() const {
  if (format_[FORMAT_SIZE - 1] == '\0') {
    return Bytes(format_);
  }
  return Bytes(format_, FORMAT_SIZE);
}

Bytes CommonHeader::version() const {
  if (version_[VERSION_SIZE - 1] == '\0') {
    return Bytes(version_);
  }
  return Bytes(version_, VERSION_SIZE);
}

}  // namespace grnxx
