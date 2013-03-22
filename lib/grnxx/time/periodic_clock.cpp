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
#include "grnxx/time/periodic_clock.hpp"

#include <thread>

#include "grnxx/intrinsic.hpp"
#include "grnxx/thread.hpp"

namespace grnxx {
namespace {

// Accuracy of the periodic clock. Note that a short sleep may lead to a
// busy-wait loop, which exhausts CPU resources.
constexpr Duration UPDATE_INTERVAL = Duration::milliseconds(100);

volatile uint32_t ref_count = 0;
std::thread thread;

}  // namespace

Time PeriodicClock::now_ = Time::min();

PeriodicClock::PeriodicClock() {
  for ( ; ; ) {
    const uint32_t count = ref_count;
    if (atomic_compare_and_swap(count, count + 1, &ref_count)) {
      if (count == 0) {
        // Start the internal thread.
        try {
          thread = std::thread(routine);
          now_ = SystemClock::now();
        } catch (...) {
          // Do nothing on failure.
        }
      }
      break;
    }
  }
}

PeriodicClock::~PeriodicClock() {
  for ( ; ; ) {
    const uint32_t count = ref_count;
    if (atomic_compare_and_swap(count, count - 1, &ref_count)) {
      if (count == 1) {
        // Stop the internal thread.
        thread.join();
        now_ = Time::min();
      }
      break;
    }
  }
}

// Periodically update the internal time variable.
void PeriodicClock::routine() {
  while (ref_count != 0) {
    Thread::sleep_for(UPDATE_INTERVAL);
    now_ = SystemClock::now();
  }
}

}  // namespace grnxx
