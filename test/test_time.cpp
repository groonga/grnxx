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
#include "time.hpp"

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  assert(grnxx::Time::max().nanoseconds() ==
         std::numeric_limits<std::int64_t>::max());
  assert(grnxx::Time::min().nanoseconds() ==
         std::numeric_limits<std::int64_t>::min());

  grnxx::Time time = grnxx::Time::now();
  GRNXX_NOTICE() << "grnxx::Time::now: " << time;

  time = grnxx::Time::now_in_seconds();
  GRNXX_NOTICE() << "grnxx::Time::now_in_seconds: " << time;

  enum { LOOP_COUNT = 1 << 16 };

  grnxx::Time start = grnxx::Time::now();
  for (int i = 0; i < LOOP_COUNT; ++i) {
    grnxx::Time::now();
  }
  grnxx::Time end = grnxx::Time::now();

  grnxx::Duration elapsed = end - start;
  GRNXX_NOTICE() << "grnxx::Time::now: average elapsed [ns] = "
                 << (elapsed.nanoseconds() / LOOP_COUNT);

  start = grnxx::Time::now();
  for (int i = 0; i < LOOP_COUNT; ++i) {
    grnxx::Time::now_in_seconds();
  }
  end = grnxx::Time::now();

  elapsed = end - start;
  GRNXX_NOTICE() << "grnxx::Time::now_in_seconds"
                 << ": average elapsed [ns] = "
                 << (elapsed.nanoseconds() / LOOP_COUNT);

  return 0;
}
