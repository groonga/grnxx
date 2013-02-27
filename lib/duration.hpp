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
#ifndef GRNXX_DURATION_HPP
#define GRNXX_DURATION_HPP

#include "basic.hpp"
#include "string_builder.hpp"

namespace grnxx {

// Time difference in microseconds.
// 64-bit tick count (usec) is used.
class Duration {
 public:
  // Trivial default constructor.
  Duration() = default;
  // Construct a duration object whose tick count is "count".
  explicit constexpr Duration(int64_t count) : count_(count) {}

  // Return the minimum tick count.
  static constexpr Duration min() {
    return Duration(std::numeric_limits<int64_t>::min());
  }
  // Return the maximum tick count.
  static constexpr Duration max() {
    return Duration(std::numeric_limits<int64_t>::max());
  }

  // Return a duration of "count" microseconds.
  static constexpr Duration microseconds(int64_t count) {
    return Duration(count);
  }
  // Return a duration of "count" milliseconds.
  static constexpr Duration milliseconds(int64_t count) {
    return Duration(count * 1000);
  }
  // Return a duration of "count" seconds.
  static constexpr Duration seconds(int64_t count) {
    return Duration(count * 1000000);
  }
  // Return a duration of "count" minutes.
  static constexpr Duration minutes(int64_t count) {
    return Duration(count * 1000000 * 60);
  }
  // Return a duration of "count" hours.
  static constexpr Duration hours(int64_t count) {
    return Duration(count * 1000000 * 60 * 60);
  }
  // Return a duration of "count" days.
  static constexpr Duration days(int64_t count) {
    return Duration(count * 1000000 * 60 * 60 * 24);
  }
  // Return a duration of "count" weeks.
  static constexpr Duration weeks(int64_t count) {
    return Duration(count * 1000000 * 60 * 60 * 24 * 7);
  }

  // Return the tick count.
  constexpr int64_t count() {
    return count_;
  }
  // Set the tick count.
  void set_count(int64_t count) {
    count_ = count;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  int64_t count_;

  // Copyable.
};

GRNXX_ASSERT_POD(Duration);

inline constexpr Duration operator+(Duration duration) {
  return duration;
}
inline constexpr Duration operator-(Duration duration) {
  return Duration(-duration.count());
}

inline Duration &operator+=(Duration &lhs, Duration rhs) {
  lhs.set_count(lhs.count() + rhs.count());
  return lhs;
}
inline Duration &operator-=(Duration &lhs, Duration rhs) {
  lhs.set_count(lhs.count() - rhs.count());
  return lhs;
}
inline Duration &operator*=(Duration &lhs, int64_t rhs) {
  lhs.set_count(lhs.count() * rhs);
  return lhs;
}
inline Duration &operator/=(Duration &lhs, int64_t rhs) {
  if (rhs == 0) {
    lhs.set_count(0);
  } else {
    lhs.set_count(lhs.count() / rhs);
  }
  return lhs;
}
inline Duration &operator%=(Duration &lhs, Duration rhs) {
  if (rhs.count() == 0) {
    lhs.set_count(0);
  } else {
    lhs.set_count(lhs.count() % rhs.count());
  }
  return lhs;
}

inline constexpr Duration operator+(Duration lhs, Duration rhs) {
  return Duration(lhs.count() + rhs.count());
}
inline constexpr Duration operator-(Duration lhs, Duration rhs) {
  return Duration(lhs.count() - rhs.count());
}
inline constexpr Duration operator*(Duration lhs, int64_t rhs) {
  return Duration(lhs.count() * rhs);
}
inline constexpr Duration operator*(int64_t lhs, Duration rhs) {
  return Duration(lhs * rhs.count());
}
inline constexpr Duration operator/(Duration lhs, int64_t rhs) {
  return (rhs != 0) ? Duration(lhs.count() / rhs) : Duration(0);
}
inline constexpr Duration operator%(Duration lhs, Duration rhs) {
  return (rhs.count() != 0) ?
      Duration(lhs.count() % rhs.count()) : Duration(0);
}

inline constexpr bool operator==(Duration lhs, Duration rhs) {
  return lhs.count() == rhs.count();
}
inline constexpr bool operator!=(Duration lhs, Duration rhs) {
  return lhs.count() != rhs.count();
}
inline constexpr bool operator<(Duration lhs, Duration rhs) {
  return lhs.count() < rhs.count();
}
inline constexpr bool operator<=(Duration lhs, Duration rhs) {
  return lhs.count() <= rhs.count();
}
inline constexpr bool operator>(Duration lhs, Duration rhs) {
  return lhs.count() > rhs.count();
}
inline constexpr bool operator>=(Duration lhs, Duration rhs) {
  return lhs.count() >= rhs.count();
}

inline StringBuilder &operator<<(StringBuilder &builder, Duration duration) {
  return duration.write_to(builder);
}

std::ostream &operator<<(std::ostream &stream, Duration duration);

}  // namespace grnxx

#endif  // GRNXX_DURATION_HPP
