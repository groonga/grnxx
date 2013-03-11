/*
  Copyright (C) 2013  Brazil, Inc.

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
#include "time/stopwatch.hpp"
#include "time/system_clock.hpp"

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  GRNXX_NOTICE() << "grnxx::SystemClock::now().universal_time(): "
                 << grnxx::SystemClock::now().universal_time();
  GRNXX_NOTICE() << "grnxx::SystemClock::now().local_time(): "
                 << grnxx::SystemClock::now().local_time();

  enum { LOOP_COUNT = 1 << 16 };

  grnxx::Time now = grnxx::SystemClock::now();

  grnxx::Stopwatch stopwatch(true);
  for (int i = 0; i < LOOP_COUNT; ++i) {
    now.universal_time();
  }
  grnxx::Duration elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "grnxx::Time::universal_time(): average elapsed [ns] = "
                 << (1000.0 * elapsed.count() / LOOP_COUNT);

  stopwatch.reset();
  for (int i = 0; i < LOOP_COUNT; ++i) {
    now.local_time();
  }
  elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "grnxx::Time::local_time(): average elapsed [ns] = "
                 << (1000.0 * elapsed.count() / LOOP_COUNT);

  return 0;
}
