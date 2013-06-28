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
#ifndef GRNXX_TIME_HPP
#define GRNXX_TIME_HPP

#include "grnxx/features.hpp"

#include "grnxx/broken_down_time.hpp"
#include "grnxx/duration.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class StringBuilder;

// Time in microseconds since the Unix epoch (1970-01-01 00:00:00 UTC).
// 64-bit tick count (usec) is used.
class Time {
 public:
  // Trivial default constructor.
  Time() = default;
  // Construct a time object whose tick count is "count".
  explicit constexpr Time(int64_t count) : count_(count) {}

  // Return the minimum tick count.
  static constexpr Time min() {
    return Time(std::numeric_limits<int64_t>::min());
  }
  // Return the maximum tick count.
  static constexpr Time max() {
    return Time(std::numeric_limits<int64_t>::max());
  }

  // Transform tick count to broken-down time (UTC).
  BrokenDownTime universal_time() const;
  // Transform tick count to broken-down time (local).
  BrokenDownTime local_time() const;

  // Return the tick count.
  constexpr int64_t count() const {
    return count_;
  }
  // Set the tick count.
  void set_count(int64_t count) {
    count_ = count;
  }

 private:
  int64_t count_;

  // Copyable.
};

inline Time &operator+=(Time &lhs, Duration rhs) {
  lhs.set_count(lhs.count() + rhs.count());
  return lhs;
}
inline Time &operator-=(Time &lhs, Duration rhs) {
  lhs.set_count(lhs.count() - rhs.count());
  return lhs;
}

inline constexpr Time operator+(Time lhs, Duration rhs) {
  return Time(lhs.count() + rhs.count());
}
inline constexpr Time operator+(Duration lhs, Time rhs) {
  return Time(lhs.count() + rhs.count());
}
inline constexpr Time operator-(Time lhs, Duration rhs) {
  return Time(lhs.count() - rhs.count());
}
inline constexpr Duration operator-(Time lhs, Time rhs) {
  return Duration(lhs.count() - rhs.count());
}

inline constexpr bool operator==(Time lhs, Time rhs) {
  return lhs.count() == rhs.count();
}
inline constexpr bool operator!=(Time lhs, Time rhs) {
  return lhs.count() != rhs.count();
}
inline constexpr bool operator<(Time lhs, Time rhs) {
  return lhs.count() < rhs.count();
}
inline constexpr bool operator<=(Time lhs, Time rhs) {
  return lhs.count() <= rhs.count();
}
inline constexpr bool operator>(Time lhs, Time rhs) {
  return lhs.count() > rhs.count();
}
inline constexpr bool operator>=(Time lhs, Time rhs) {
  return lhs.count() >= rhs.count();
}

StringBuilder &operator<<(StringBuilder &builder, Time time);

}  // namespace grnxx

#endif  // GRNXX_TIME_HPP
