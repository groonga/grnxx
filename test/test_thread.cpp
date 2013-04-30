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

#include "grnxx/lock.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/mutex.hpp"
#include "grnxx/thread.hpp"
#include "grnxx/time/stopwatch.hpp"
#include "grnxx/time/system_clock.hpp"

namespace {

grnxx::Mutex mutex(grnxx::MUTEX_UNLOCKED);

void thread_routine() {
  grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(10));
}

}  // namespace

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  enum { LOOP_COUNT = 1000 };

  grnxx::Stopwatch stopwatch(true);
  for (int i = 0; i < LOOP_COUNT; ++i) {
    grnxx::Thread::yield();
  }
  grnxx::Duration elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "yield(): elapsed [ns]: "
                 << (1000.0 * elapsed.count() / LOOP_COUNT);

  stopwatch.reset();
  for (int i = 0; i < LOOP_COUNT; ++i) {
    grnxx::Thread::sleep_for(grnxx::Duration(0));
  }
  elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "sleep_for(0): elapsed [ns]: "
                 << (1000.0 * elapsed.count() / LOOP_COUNT);

  stopwatch.reset();
  grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(10));
  elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "sleep_for(10ms): elapsed [ns] = "
                 << (1000.0 * elapsed.count());

  stopwatch.reset();
  grnxx::Thread::sleep_until(grnxx::SystemClock::now() +
                             grnxx::Duration::milliseconds(10));
  elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "sleep_until(now + 10ms): elapsed [ns] = "
                 << (1000.0 * elapsed.count());

  stopwatch.reset();
  std::unique_ptr<grnxx::Thread> thread(grnxx::Thread::create(thread_routine));
  assert(thread);
  assert(thread->join());
  elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "thread + join: elapsed [ns] = "
                 << (1000.0 * elapsed.count());

  stopwatch.reset();
  thread.reset(grnxx::Thread::create([]() {
    grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(10));
  }));
  assert(thread);
  assert(thread->join());
  elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "thread + join: elapsed [ns] = "
                 << (1000.0 * elapsed.count());

  stopwatch.reset();
  thread.reset(grnxx::Thread::create([]() {
    grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(10));
  }));
  assert(thread);
  assert(thread->detach());
  elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "thread + detach: elapsed [ns] = "
                 << (1000.0 * elapsed.count());

  return 0;
}
