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
#include "grnxx/stopwatch.hpp"

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);


  assert(!grnxx::Mutex(grnxx::MUTEX_UNLOCKED).locked());
  assert(grnxx::Mutex(grnxx::MUTEX_LOCKED).locked());

  grnxx::Mutex mutex(grnxx::MUTEX_UNLOCKED);

  GRNXX_NOTICE() << "mutex = " << mutex;

  assert(mutex.try_lock());
  assert(mutex.locked());

  GRNXX_NOTICE() << "mutex = " << mutex;

  assert(!mutex.try_lock());
  assert(mutex.locked());

  assert(mutex.unlock());
  assert(!mutex.locked());

  mutex.lock();
  assert(mutex.locked());

  assert(mutex.unlock());
  assert(!mutex.locked());

  assert(mutex.lock(grnxx::Duration(0)));
  assert(mutex.locked());

  assert(!mutex.lock(grnxx::Duration(0)));
  assert(mutex.locked());

  assert(mutex.unlock());
  assert(!mutex.locked());


  enum { LOOP_COUNT = 1 << 20 };

  grnxx::Stopwatch stopwatch(true);
  for (int i = 0; i < LOOP_COUNT; ++i) {
    grnxx::Lock lock(&mutex);
    assert(lock);
  }
  const grnxx::Duration elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "grnxx::Lock: elapsed [ns] = "
                 << (1000.0 * elapsed.count() / LOOP_COUNT);

  return 0;
}
