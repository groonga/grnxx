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
#include <cerrno>
#include <cstdio>

#include "error.hpp"
#include "logger.hpp"

//void f3() {
//  std::cerr << "I'm f3()" << std::endl;
//  GRNXX_LOG_BACKTRACE(5);
//}

//void f2() {
//  std::cerr << "I'm f2()" << std::endl;
//  f3();
//  GRNXX_LOG_WITH_BACKTRACE(3, "This is a %s.", "pen");
//}

//void f1() {
//  std::cerr << "I'm f1()" << std::endl;
//  f2();
//}

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  assert(grnxx::Logger::open("temp.log"));

  GRNXX_LOGGER(grnxx::ERROR_LOGGER) << "ERROR_LOGGER";
  GRNXX_LOGGER(grnxx::WARNING_LOGGER) << "WARNING_LOGGER";
  GRNXX_LOGGER(grnxx::NOTICE_LOGGER) << "NOTICE_LOGGER";

  GRNXX_ERROR() << "GRNXX_ERROR";
  GRNXX_WARNING() << "GRNXX_WARNING";
  GRNXX_NOTICE() << "GRNXX_NOTICE";

  const char * const path = "no_such_directory/no_such_file";
  assert(std::fopen(path, "rb") == NULL);

  GRNXX_ERROR() << "failed to open file: <" << path << ">: 'fopen' "
                << grnxx::Error(errno);

  GRNXX_LOGGER(0) << "Level: 0";
  GRNXX_LOGGER(1) << "Level: 1";
  GRNXX_LOGGER(2) << "Level: 2";
  GRNXX_LOGGER(3) << "Level: 3";
  GRNXX_LOGGER(4) << "Level: 4";

  GRNXX_NOTICE() << "This" << grnxx::Logger::new_line()
                 << "is" << grnxx::Logger::new_line()
                 << "a multi-line log.";

  GRNXX_NOTICE() << "backtrace: " << grnxx::Logger::backtrace();

  return 0;
}
