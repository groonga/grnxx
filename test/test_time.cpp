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
#include <cassert>

#include "grnxx/logger.hpp"
#include "grnxx/thread.hpp"
#include "grnxx/time/broken_down_time.hpp"
#include "grnxx/time/internal_clock.hpp"
#include "grnxx/time/periodic_clock.hpp"
#include "grnxx/time/stopwatch.hpp"
#include "grnxx/time/system_clock.hpp"
#include "grnxx/time/time.hpp"

void test_time() {
  assert(grnxx::Time::max().count() ==
         std::numeric_limits<std::int64_t>::max());
  assert(grnxx::Time::min().count() ==
         std::numeric_limits<std::int64_t>::min());
}

void test_broken_down_time() {
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
}

void test_system_clock() {
  grnxx::Time time = grnxx::SystemClock::now();
  GRNXX_NOTICE() << "grnxx::SystemClock::now(): " << time;
  GRNXX_NOTICE() << "grnxx::SystemClock::now().local_time(): "
                 << time.local_time();

  enum { LOOP_COUNT = 1 << 16 };

  grnxx::Stopwatch stopwatch(true);
  for (int i = 0; i < LOOP_COUNT; ++i) {
    grnxx::SystemClock::now();
  }
  grnxx::Duration elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "grnxx::SystemClock::now: average elapsed [ns] = "
                 << (1000.0 * elapsed.count() / LOOP_COUNT);
}

void test_internal_clock() {
  grnxx::Time time = grnxx::InternalClock::now();
  GRNXX_NOTICE() << "grnxx::InternalClock::now(): " << time;
  GRNXX_NOTICE() << "grnxx::InternalClock::now().local_time(): "
                 << time.local_time();

  time = grnxx::InternalClock::now();
  GRNXX_NOTICE() << "grnxx::InternalClock::now(): " << time;
  GRNXX_NOTICE() << "grnxx::InternalClock::now().local_time(): "
                 << time.local_time();

  grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(310));

  time = grnxx::InternalClock::now();
  GRNXX_NOTICE() << "grnxx::InternalClock::now(): " << time;
  GRNXX_NOTICE() << "grnxx::InternalClock::now().local_time(): "
                 << time.local_time();

  grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(310));

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
}

void test_periodic_clock() {
  grnxx::PeriodicClock clock;

  grnxx::Time time = grnxx::PeriodicClock::now();
  GRNXX_NOTICE() << "grnxx::PeriodicClock::now(): " << time;
  GRNXX_NOTICE() << "grnxx::PeriodicClock::now().local_time(): "
                 << time.local_time();

  time = grnxx::PeriodicClock::now();
  GRNXX_NOTICE() << "grnxx::PeriodicClock::now(): " << time;
  GRNXX_NOTICE() << "grnxx::PeriodicClock::now().local_time(): "
                 << time.local_time();

  grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(310));

  time = grnxx::PeriodicClock::now();
  GRNXX_NOTICE() << "grnxx::PeriodicClock::now(): " << time;
  GRNXX_NOTICE() << "grnxx::PeriodicClock::now().local_time(): "
                 << time.local_time();

  grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(310));

  time = grnxx::PeriodicClock::now();
  GRNXX_NOTICE() << "grnxx::PeriodicClock::now(): " << time;
  GRNXX_NOTICE() << "grnxx::PeriodicClock::now().local_time(): "
                 << time.local_time();

  enum { LOOP_COUNT = 1 << 20 };

  grnxx::Stopwatch stopwatch(true);
  for (int i = 0; i < LOOP_COUNT; ++i) {
    grnxx::PeriodicClock::now();
  }
  grnxx::Duration elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "grnxx::PeriodicClock::now: average elapsed [ns] = "
                 << (1000.0 * elapsed.count() / LOOP_COUNT);
}

void test_stopwatch() {
  grnxx::Stopwatch stopwatch(false);
  assert(stopwatch.elapsed() == grnxx::Duration(0));

  stopwatch.start();
  grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(1));
  grnxx::Duration elapsed = stopwatch.elapsed();
  assert(elapsed > grnxx::Duration(0));

  grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(1));
  assert(stopwatch.elapsed() > elapsed);

  stopwatch.stop();
  elapsed = stopwatch.elapsed();

  grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(1));
  assert(stopwatch.elapsed() == elapsed);

  stopwatch.start();
  grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(1));
  assert(stopwatch.elapsed() > elapsed);

  GRNXX_NOTICE() << "stopwatch.elapsed() = " << stopwatch.elapsed();

  elapsed = stopwatch.elapsed();
  stopwatch.reset();
  grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(1));
  assert(stopwatch.elapsed() < elapsed);
  assert(stopwatch.elapsed() > grnxx::Duration(0));

  stopwatch.stop();
  stopwatch.reset();
  assert(stopwatch.elapsed() == grnxx::Duration(0));

  grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(1));
  assert(stopwatch.elapsed() == grnxx::Duration(0));
}

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_time();
  test_broken_down_time();
  test_system_clock();
  test_internal_clock();
  test_periodic_clock();
  test_stopwatch();

  return 0;
}
