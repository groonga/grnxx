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
#ifndef GRNXX_STORAGE_PATH_HPP
#define GRNXX_STORAGE_PATH_HPP

#include "grnxx/features.hpp"

namespace grnxx {
namespace storage {

class Path {
 public:
  // Generate a full path from "path" and return an allocated buffer which
  // must be freed with delete[].
  static char *full_path(const char *path);

  // Generate an almost unique path by adding a random suffix and return an
  // allocated buffer which must be freed with delete[].
  // For example, when "prefix" == "temp", the result is "temp_XXXXXXXX",
  // where X indicates a random character ('0'-'9' or 'A'-'Z').
  static char *unique_path(const char *prefix);

  // Create a clone of "path" and return an allocated buffer which must be
  // freed with delete[].
  static char *clone_path(const char *path);
};

}  // namespace storage
}  // namespace grnxx

#endif  // GRNXX_STORAGE_PATH_HPP
