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
#include "recycler.hpp"

#include "system_clock.hpp"

namespace grnxx {

void Recycler::update() {
  // First, zero-clear the internal counter so that other threads and processes
  // will not come into this function. However, this is not a perfect barrier.
  count_ = 0;

  // SystemClock::now() takes around 1 microsecond on Core2 Duo 1.6GHz. So, if
  // RECYCLER_STAMP_COUNT_PER_UPDATE == 500, stamp() spends 2ns/call for
  // SystemClock::now() on average.
  const Time now = SystemClock::now();

  StampPair current_stamp_pair = stamp_pair_;

  // Update stamps iff enough time has passed after the latest update.
  const uint16_t current_time_id =
      current_stamp_pair.current & RECYCLER_STAMP_MASK;
  const Duration step_duration =
      frozen_duration_ / (RECYCLER_STAMP_BUF_SIZE / 2);
  if (now > (times_[current_time_id] + step_duration)) {
    // Use a compare-and-swap (CAS) to avoid a collision.
    StampPair next_stamp_pair;
    next_stamp_pair.current = current_stamp_pair.current + 1;
    next_stamp_pair.threshold = current_stamp_pair.threshold;
    if (!atomic_compare_and_swap(current_stamp_pair, next_stamp_pair,
                                 &stamp_pair_)) {
      return;
    }

    // There exists a moment when stamps_ is updated but times_ is not updated
    // yet. So, times_ must be initialized with a future time.
    times_[next_stamp_pair.current & RECYCLER_STAMP_MASK] = now;

    // Update stamp_pair_.threshold for check().
    const Time threshold_time = now - frozen_duration_;
    for (current_stamp_pair = next_stamp_pair;
         current_stamp_pair.threshold < current_stamp_pair.current;
         current_stamp_pair = next_stamp_pair) {

      // Use the time associated with the next stamp.
      const uint16_t threshold_time_id =
          ++next_stamp_pair.threshold & RECYCLER_STAMP_MASK;
      if (threshold_time < times_[threshold_time_id]) {
        break;
      }

      // Use a compare-and-swap (CAS) to avoid a collision.
      if (!atomic_compare_and_swap(current_stamp_pair, next_stamp_pair,
                                   &stamp_pair_)) {
        break;
      }

      // Initialize times_ with a future time.
      times_[current_stamp_pair.threshold & RECYCLER_STAMP_MASK] =
          RECYCLER_FUTURE_TIME;
    }
  }
}

StringBuilder &Recycler::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  const StampPair stamp_pair = stamp_pair_;

  builder << "{ count = " << count_
          << ", current = " << stamp_pair.current
          << ", threshold = " << stamp_pair.threshold
          << ", frozen_duration = " << frozen_duration_;

  builder << ", times = { ";
  for (uint16_t stamp = stamp_pair.threshold;
       stamp <= stamp_pair.current; ++stamp) {
    const uint16_t time_id = stamp & RECYCLER_STAMP_MASK;
    if (stamp != stamp_pair.threshold) {
      builder << ", ";
    }
    builder << '[' << stamp << "] = " << times_[time_id];
  }
  builder << " }";

  return builder << " }";
}

}  // namespace grnxx
