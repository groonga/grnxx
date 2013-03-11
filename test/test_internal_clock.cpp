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
#include <unistd.h>

#include <cassert>

#include "logger.hpp"
#include "thread.hpp"
#include "time/internal_clock.hpp"
#include "time/stopwatch.hpp"

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  grnxx::Time time = grnxx::InternalClock::now();
  GRNXX_NOTICE() << "grnxx::InternalClock::now(): " << time;
  GRNXX_NOTICE() << "grnxx::InternalClock::now().local_time(): "
                 << time.local_time();

  time = grnxx::InternalClock::now();
  GRNXX_NOTICE() << "grnxx::InternalClock::now(): " << time;
  GRNXX_NOTICE() << "grnxx::InternalClock::now().local_time(): "
                 << time.local_time();

  grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(500));

  time = grnxx::InternalClock::now();
  GRNXX_NOTICE() << "grnxx::InternalClock::now(): " << time;
  GRNXX_NOTICE() << "grnxx::InternalClock::now().local_time(): "
                 << time.local_time();

  grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(500));

  time = grnxx::InternalClock::now();
  GRNXX_NOTICE() << "grnxx::InternalClock::now(): " << time;
  GRNXX_NOTICE() << "grnxx::InternalClock::now().local_time(): "
                 << time.local_time();

  enum { LOOP_COUNT = 1 << 20 };

  grnxx::Stopwatch stopwatch(true);
  for (int i = 0; i < LOOP_COUNT; ++i) {
    grnxx::InternalClock::now();
  }
  grnxx::Duration elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "grnxx::InternalClock::now: average elapsed [ns] = "
                 << (1000.0 * elapsed.count() / LOOP_COUNT);

  return 0;
}
