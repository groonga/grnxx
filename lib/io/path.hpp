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
#ifndef GRNXX_IO_PATH_HPP
#define GRNXX_IO_PATH_HPP

#include "string.hpp"

namespace grnxx {
namespace io {

class Path {
 public:
  // Generate a full path from a path.
  // Return a copy of the given path if it is a full path.
  static String full_path(const char *path);

  // Generate an almost unique path by adding a random suffix.
  // If the given path is "temp", this function generates "temp_XXXXXXXX",
  // where X indicates a random character ('0'-'9' or 'A'-'Z').
  static String unique_path(const char *path);

 private:
  Path();
  ~Path();

  Path(const Path &);
  Path &operator=(const Path &);
};

}  // namespace io
}  // namespace grnxx

#endif  // GRNXX_IO_PATH_HPP
