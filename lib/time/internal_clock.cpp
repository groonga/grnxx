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
#include "time/internal_clock.hpp"

#include "../config.h"

#ifdef HAVE_PTHREAD_ATFORK
# include <pthread.h>
#endif  // HAVE_PTHREAD_ATFORK

#include <thread>

#include "error.hpp"
#include "lock.hpp"
#include "logger.hpp"
#include "thread.hpp"
#include "time/system_clock.hpp"

namespace grnxx {
namespace {

// Accuracy of the internal clock. Note that a short sleep may lead to a
// busy-wait loop, which exhausts CPU resources.
constexpr Duration UPDATE_INTERVAL = Duration::milliseconds(100);

Time *internal_time = nullptr;

// Update the internal clock periodically.
void internal_clock_routine() {
  // TODO: Fix this endless loop.
  while (true) {
    Thread::sleep_for(UPDATE_INTERVAL);
    *internal_time = SystemClock::now();
  }
}

// Start a thread to update the internal clock.
void start_internal_clock() {
  try {
    std::thread thread(internal_clock_routine);
    thread.detach();
  } catch (...) {
    // Failed to start thread.
    *internal_time = Time::min();
    return;
  }

#ifdef HAVE_PTHREAD_ATFORK
  // Start a new thread, if fork() is invoked, on the child process.
  int error = ::pthread_atfork(nullptr, nullptr, start_internal_clock);
  if (error != 0) {
    // The current process works well even if this failed.
    GRNXX_WARNING() << "failed to set a fork handler: '::pthread_atfork' "
                    << Error(error);
  }
#endif  // HAVE_PTHREAD_ATFORK

  *internal_time = SystemClock::now();
}

}  // namespace

Time InternalClock::now_ = Time::min();

Time InternalClock::start() {
  if (!internal_time) {
    static Mutex mutex(MUTEX_UNLOCKED);
    Lock lock(&mutex);
    if (!internal_time) {
      internal_time = &now_;
      start_internal_clock();
      if (now_ != Time::min()) {
        return now_;
      }
    }
  }
  // Use the system clock if the internal clock is not available.
  return SystemClock::now();
}

}  // namespace grnxx
