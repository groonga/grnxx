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
#ifndef GRNXX_DURATION_HPP
#define GRNXX_DURATION_HPP

#include "basic.hpp"
#include "string_builder.hpp"

namespace grnxx {

class Duration {
 public:
  Duration() = default;
  explicit constexpr Duration(int64_t nanoseconds)
    : nanoseconds_(nanoseconds) {}

  static constexpr Duration nanoseconds(int64_t nanoseconds) {
    return Duration(nanoseconds);
  }
  static constexpr Duration microseconds(int64_t microseconds) {
    return Duration(microseconds * 1000);
  }
  static constexpr Duration milliseconds(int64_t milliseconds) {
    return Duration(milliseconds * 1000000);
  }
  static constexpr Duration seconds(int64_t seconds) {
    return Duration(seconds * 1000000000);
  }
  static constexpr Duration minutes(int64_t minutes) {
    return Duration(minutes * 1000000000 * 60);
  }
  static constexpr Duration hours(int64_t hours) {
    return Duration(hours * 1000000000 * 60 * 60);
  }
  static constexpr Duration days(int64_t days) {
    return Duration(days * 1000000000 * 60 * 60 * 24);
  }
  static constexpr Duration weeks(int64_t weeks) {
    return Duration(weeks * 1000000000 * 60 * 60 * 24 * 7);
  }

  constexpr int64_t nanoseconds() const {
    return nanoseconds_;
  }
  void set_nanoseconds(int64_t nanoseconds) {
    nanoseconds_ = nanoseconds;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  int64_t nanoseconds_;

  // Copyable.
};

inline constexpr Duration operator+(Duration duration) {
  return duration;
}
inline constexpr Duration operator-(Duration duration) {
  return Duration(-duration.nanoseconds());
}

inline Duration &operator+=(Duration &lhs, Duration rhs) {
  lhs.set_nanoseconds(lhs.nanoseconds() + rhs.nanoseconds());
  return lhs;
}
inline Duration &operator-=(Duration &lhs, Duration rhs) {
  lhs.set_nanoseconds(lhs.nanoseconds() - rhs.nanoseconds());
  return lhs;
}
inline Duration &operator*=(Duration &lhs, int64_t rhs) {
  lhs.set_nanoseconds(lhs.nanoseconds() * rhs);
  return lhs;
}
inline Duration &operator/=(Duration &lhs, int64_t rhs) {
  if (rhs == 0) {
    lhs.set_nanoseconds(0);
  } else {
    lhs.set_nanoseconds(lhs.nanoseconds() / rhs);
  }
  return lhs;
}
inline Duration &operator%=(Duration &lhs, Duration rhs) {
  if (rhs.nanoseconds() == 0) {
    lhs.set_nanoseconds(0);
  } else {
    lhs.set_nanoseconds(lhs.nanoseconds() % rhs.nanoseconds());
  }
  return lhs;
}

inline constexpr Duration operator+(Duration lhs, Duration rhs) {
  return Duration(lhs.nanoseconds() + rhs.nanoseconds());
}
inline constexpr Duration operator-(Duration lhs, Duration rhs) {
  return Duration(lhs.nanoseconds() - rhs.nanoseconds());
}
inline constexpr Duration operator*(Duration lhs, int64_t rhs) {
  return Duration(lhs.nanoseconds() * rhs);
}
inline constexpr Duration operator*(int64_t lhs, Duration rhs) {
  return Duration(lhs * rhs.nanoseconds());
}
inline constexpr Duration operator/(Duration lhs, int64_t rhs) {
  return (rhs != 0) ? Duration(lhs.nanoseconds() / rhs) : Duration(0);
}
inline constexpr Duration operator%(Duration lhs, Duration rhs) {
  return (rhs.nanoseconds() != 0) ?
      Duration(lhs.nanoseconds() % rhs.nanoseconds()) : Duration(0);
}

inline constexpr bool operator==(Duration lhs, Duration rhs) {
  return lhs.nanoseconds() == rhs.nanoseconds();
}
inline constexpr bool operator!=(Duration lhs, Duration rhs) {
  return lhs.nanoseconds() != rhs.nanoseconds();
}
inline constexpr bool operator<(Duration lhs, Duration rhs) {
  return lhs.nanoseconds() < rhs.nanoseconds();
}
inline constexpr bool operator<=(Duration lhs, Duration rhs) {
  return lhs.nanoseconds() <= rhs.nanoseconds();
}
inline constexpr bool operator>(Duration lhs, Duration rhs) {
  return lhs.nanoseconds() > rhs.nanoseconds();
}
inline constexpr bool operator>=(Duration lhs, Duration rhs) {
  return lhs.nanoseconds() >= rhs.nanoseconds();
}

inline StringBuilder &operator<<(StringBuilder &builder, Duration duration) {
  return duration.write_to(builder);
}

std::ostream &operator<<(std::ostream &stream, Duration duration);

}  // namespace grnxx

#endif  // GRNXX_TIME_HPP
