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
#ifndef GRNXX_TIME_HPP
#define GRNXX_TIME_HPP

#include "basic.hpp"
#include "duration.hpp"

namespace grnxx {

class Time {
 public:
  Time() : nanoseconds_(INVALID_NANOSECONDS) {}
  explicit Time(int64_t nanoseconds) : nanoseconds_(nanoseconds) {}

  static Time now();
  static Time now_in_seconds();

  static Time invalid_time() {
    return Time();
  }

  GRNXX_EXPLICIT_CONVERSION operator bool() const {
    return nanoseconds_ != INVALID_NANOSECONDS;
  }

  int64_t nanoseconds() const {
    return nanoseconds_;
  }
  void set_nanoseconds(int64_t nanoseconds) {
    nanoseconds_ = nanoseconds;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  int64_t nanoseconds_;

  static const int64_t INVALID_NANOSECONDS =
      std::numeric_limits<int64_t>::min();

  // Copyable.
};

inline Time &operator+=(Time &lhs, Duration rhs) {
  lhs.set_nanoseconds(lhs.nanoseconds() + rhs.nanoseconds());
  return lhs;
}
inline Time &operator-=(Time &lhs, Duration rhs) {
  lhs.set_nanoseconds(lhs.nanoseconds() - rhs.nanoseconds());
  return lhs;
}

inline Time operator+(Time lhs, Duration rhs) {
  return Time(lhs.nanoseconds() + rhs.nanoseconds());
}
inline Time operator+(Duration lhs, Time rhs) {
  return Time(lhs.nanoseconds() + rhs.nanoseconds());
}
inline Time operator-(Time lhs, Duration rhs) {
  return Time(lhs.nanoseconds() - rhs.nanoseconds());
}
inline Duration operator-(Time lhs, Time rhs) {
  return Duration(lhs.nanoseconds() - rhs.nanoseconds());
}

inline bool operator==(Time lhs, Time rhs) {
  return lhs.nanoseconds() == rhs.nanoseconds();
}
inline bool operator!=(Time lhs, Time rhs) {
  return lhs.nanoseconds() != rhs.nanoseconds();
}
inline bool operator<(Time lhs, Time rhs) {
  return lhs.nanoseconds() < rhs.nanoseconds();
}
inline bool operator<=(Time lhs, Time rhs) {
  return lhs.nanoseconds() <= rhs.nanoseconds();
}
inline bool operator>(Time lhs, Time rhs) {
  return lhs.nanoseconds() > rhs.nanoseconds();
}
inline bool operator>=(Time lhs, Time rhs) {
  return lhs.nanoseconds() >= rhs.nanoseconds();
}

inline StringBuilder &operator<<(StringBuilder &builder, Time time) {
  return time.write_to(builder);
}

std::ostream &operator<<(std::ostream &stream, Time time);

}  // namespace grnxx

#endif  // GRNXX_TIME_HPP
