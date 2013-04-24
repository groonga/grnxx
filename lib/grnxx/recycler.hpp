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
#ifndef GRNXX_RECYCLER_HPP
#define GRNXX_RECYCLER_HPP

#include "grnxx/time/time.hpp"

namespace grnxx {

class StringBuilder;

constexpr uint8_t  RECYCLER_STAMP_BUF_SIZE_BITS = 6;
constexpr uint16_t RECYCLER_STAMP_BUF_SIZE      =
    uint16_t(1 << RECYCLER_STAMP_BUF_SIZE_BITS);
constexpr uint16_t RECYCLER_STAMP_MASK          = RECYCLER_STAMP_BUF_SIZE - 1;

constexpr uint32_t RECYCLER_STAMP_COUNT_PER_UPDATE = 512;

constexpr Time RECYCLER_FUTURE_TIME = Time::max();

class Recycler {
 public:
  Recycler() : count_(), stamp_pair_(), frozen_duration_(), times_() {}
  explicit Recycler(Duration frozen_duration)
    : count_(0), stamp_pair_{ 0, 0 },
      frozen_duration_(frozen_duration), times_() {
    times_[0] = Time(0);
    for (uint16_t i = 1; i < RECYCLER_STAMP_BUF_SIZE; ++i) {
      times_[i] = Time(RECYCLER_FUTURE_TIME);
    }
  }

  std::uint16_t stamp() {
    // Update stamp_pair_ and times_, once per RECYCLER_STAMP_COUNT_PER_UPDATE.
    // Note that count_ is zero-cleared in update(), but in a multi-threaded
    // case, the zero-clear might be ignored by ++count_.
    //  temp = count_ + 1;
    //  count_ = 0;  // Zero-cleared in update().
    //  count_ = temp;
    if (++count_ >= RECYCLER_STAMP_COUNT_PER_UPDATE) {
      update();
    }
    return stamp_pair_.current;
  }

  bool check(uint16_t stamp) {
    // In a multi-threaded case, stamp_pair_ might be updated during check().
    // When a stamp causes an over-flow, the update may result in a critical
    // problem. So, this function uses a copy of stamp_pair_.
    StampPair stamp_pair = stamp_pair_;
    stamp_pair.current = this->stamp();
    stamp_pair.threshold = stamp_pair_.threshold;
    if (stamp_pair.current < stamp_pair.threshold) {
      return (stamp > stamp_pair.current) && (stamp < stamp_pair.threshold);
    } else {
      return (stamp > stamp_pair.current) || (stamp < stamp_pair.threshold);
    }
  }

  Duration frozen_duration() const {
    return frozen_duration_;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  uint32_t count_;
  struct StampPair {
    uint16_t current;
    uint16_t threshold;
  } stamp_pair_;
  Duration frozen_duration_;
  Time times_[RECYCLER_STAMP_BUF_SIZE];

  void update();
};

inline StringBuilder &operator<<(StringBuilder &builder,
                                 const Recycler &recycler) {
  return recycler.write_to(builder);
}

}  // namespace grnxx

#endif  // GRNXX_RECYCLER_HPP
