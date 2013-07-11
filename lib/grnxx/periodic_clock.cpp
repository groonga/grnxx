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
#include "grnxx/periodic_clock.hpp"

#include <memory>

#include "grnxx/intrinsic.hpp"
#include "grnxx/lock.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/mutex.hpp"
#include "grnxx/thread.hpp"

namespace grnxx {
namespace {

// Accuracy of the periodic clock. Note that a short sleep may lead to a
// busy-wait loop, which exhausts CPU resources.
constexpr Duration UPDATE_INTERVAL = Duration::milliseconds(100);

// The number of PeriodicClock objects.
volatile uint32_t ref_count = 0;
// The current thread ID.
volatile uint32_t thread_id = 0;
Mutex mutex;

}  // namespace

Time PeriodicClock::now_ = Time::min();

PeriodicClock::PeriodicClock() {
  Lock lock(&mutex);
  if (++ref_count == 1) try {
    // Start an internal thread that updates "now_" periodically.
    std::unique_ptr<grnxx::Thread> thread(grnxx::Thread::create(routine));
    thread->detach();
    // Immediately update "now_".
    now_ = SystemClock::now();
  } catch (...) {
    GRNXX_WARNING() << "failed to create thread for PeriodicClock";
  }
}

PeriodicClock::~PeriodicClock() {
  Lock lock(&mutex);
  if (--ref_count == 0) {
    now_ = Time::min();
    // Increment "thread_id" so that an internal thread will stop.
    atomic_fetch_and_add(1, &thread_id);
  }
}

void PeriodicClock::routine() {
  // Increment "thread_id" to generate the ID of this thread.
  const uint64_t this_thread_id = atomic_fetch_and_add(1, &thread_id) + 1;
  // This thread terminates if there are no PeriodicClock objects or another
  // thread is running.
  while ((ref_count != 0) && (this_thread_id == thread_id)) {
    Thread::sleep_for(UPDATE_INTERVAL);
    Lock lock(&mutex);
    if ((ref_count != 0) && (this_thread_id == thread_id)) {
      now_ = SystemClock::now();
    }
  }
}

}  // namespace grnxx
