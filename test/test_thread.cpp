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

#include "logger.hpp"
#include "thread.hpp"
#include "time.hpp"

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  enum { LOOP_COUNT = 1000 };

  grnxx::Time start = grnxx::Time::now();
  for (int i = 0; i < LOOP_COUNT; ++i) {
    grnxx::Thread::switch_to_others();
  }
  grnxx::Time end = grnxx::Time::now();

  GRNXX_NOTICE() << "switch_to_others(): elapsed [ns]: "
                 << ((end - start).nanoseconds() / LOOP_COUNT);

  start = grnxx::Time::now();
  for (int i = 0; i < LOOP_COUNT; ++i) {
    grnxx::Thread::sleep(grnxx::Duration(0));
  }
  end = grnxx::Time::now();

  GRNXX_NOTICE() << "sleep(0): elapsed [ns]: "
                 << ((end - start).nanoseconds() / LOOP_COUNT);

  start = grnxx::Time::now();
  grnxx::Thread::sleep(grnxx::Duration::milliseconds(10));
  end = grnxx::Time::now();

  GRNXX_NOTICE() << "sleep(10ms): elapsed [ns] = "
                 << (end - start).nanoseconds();

  return 0;
}
