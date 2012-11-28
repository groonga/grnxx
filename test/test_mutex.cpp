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

#include "lock.hpp"
#include "logger.hpp"
#include "mutex.hpp"
#include "time.hpp"

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);


  grnxx::Mutex mutex;
  assert(mutex.value() == grnxx::Mutex::UNLOCKED);

  GRNXX_NOTICE() << "mutex = " << mutex;

  assert(mutex.try_lock());
  assert(mutex.value() == grnxx::Mutex::LOCKED);

  GRNXX_NOTICE() << "mutex = " << mutex;

  assert(!mutex.try_lock());
  assert(mutex.value() == grnxx::Mutex::LOCKED);

  assert(mutex.unlock());
  assert(mutex.value() == grnxx::Mutex::UNLOCKED);

  assert(mutex.lock());
  assert(mutex.value() == grnxx::Mutex::LOCKED);

  mutex.clear();
  assert(mutex.value() == grnxx::Mutex::UNLOCKED);


  grnxx::Mutex::Object mutex_object = grnxx::Mutex::UNLOCKED;

  assert(grnxx::Mutex::try_lock(&mutex_object));
  assert(mutex_object == grnxx::Mutex::LOCKED);

  assert(!grnxx::Mutex::try_lock(&mutex_object));
  assert(mutex_object == grnxx::Mutex::LOCKED);

  assert(grnxx::Mutex::unlock(&mutex_object));
  assert(mutex_object == grnxx::Mutex::UNLOCKED);

  assert(grnxx::Mutex::lock(&mutex_object));
  assert(mutex_object == grnxx::Mutex::LOCKED);


  enum { LOOP_COUNT = 1 << 20 };

  const grnxx::Time start = grnxx::Time::now();
  for (int i = 0; i < LOOP_COUNT; ++i) {
    grnxx::Lock lock(&mutex);
    assert(lock);
  }
  const grnxx::Time end = grnxx::Time::now();

  GRNXX_NOTICE() << "grnxx::Lock: elapsed [ns] = "
                 << ((end - start).nanoseconds() / LOOP_COUNT);

  return 0;
}
