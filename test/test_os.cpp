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

#include "os.hpp"
#include "logger.hpp"

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  GRNXX_NOTICE() << "page_size = " << grnxx::OS::get_page_size();

  GRNXX_NOTICE() << "getenv(PATH) = "
                 << grnxx::OS::get_environment_variable("PATH");
  GRNXX_NOTICE() << "getenv(NO_SUCH_NAME) = "
                 << grnxx::OS::get_environment_variable("NO_SUCH_NAME");

  return 0;
}
