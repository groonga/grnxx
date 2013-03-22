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

#include "grnxx/backtrace.hpp"
#include "grnxx/logger.hpp"

void function_3rd() {
  GRNXX_NOTICE() << __FUNCTION__ << "()";

  std::vector<std::string> backtrace;
  grnxx::Backtrace::pretty_backtrace(0, &backtrace);
  for (std::size_t i = 0; i < backtrace.size(); ++i) {
    GRNXX_NOTICE() << backtrace[i].c_str();
  }
}

void function_2nd() {
  GRNXX_NOTICE() << __FUNCTION__ << "()";

  function_3rd();
}

void function_1st() {
  GRNXX_NOTICE() << __FUNCTION__ << "()";

  function_2nd();
}

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  function_1st();

  return 0;
}
