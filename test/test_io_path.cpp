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
#include <cassert>
#include <sstream>

#include "io/path.hpp"
#include "logger.hpp"

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  grnxx::String full_path = grnxx::io::Path::full_path(nullptr);
  GRNXX_NOTICE() << "full_path = " << full_path;

  full_path = grnxx::io::Path::full_path("temp.grn");
  GRNXX_NOTICE() << "full_path = " << full_path;

  assert(grnxx::io::Path::full_path("/") == "/");
  assert(grnxx::io::Path::full_path("/.") == "/");
  assert(grnxx::io::Path::full_path("/..") == "/");

  assert(grnxx::io::Path::full_path("/usr/local/lib") == "/usr/local/lib");
  assert(grnxx::io::Path::full_path("/usr/local/lib/") == "/usr/local/lib/");
  assert(grnxx::io::Path::full_path("/usr/local/lib/.") == "/usr/local/lib");
  assert(grnxx::io::Path::full_path("/usr/local/lib/./") == "/usr/local/lib/");

  assert(grnxx::io::Path::full_path("/usr/local/lib/..") == "/usr/local");
  assert(grnxx::io::Path::full_path("/usr/local/lib/../") == "/usr/local/");

  grnxx::String unique_path = grnxx::io::Path::unique_path("temp.grn");
  GRNXX_NOTICE() << "unique_path = " << unique_path;

  unique_path = grnxx::io::Path::unique_path(nullptr);
  GRNXX_NOTICE() << "unique_path = " << unique_path;

  return 0;
}
