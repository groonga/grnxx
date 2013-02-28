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
#ifndef GRNXX_BROKEN_DOWN_TIME_HPP
#define GRNXX_BROKEN_DOWN_TIME_HPP

#include "basic.hpp"
#include "string_builder.hpp"

namespace grnxx {

struct BrokenDownTime {
  int usec;   // Microseconds.
  int sec;    // Seconds.
  int min;    // Minutes.
  int hour;   // Hours.
  int mday;   // Day of the month.
  int mon;    // Month.
  int year;   // Year.
  int wday;   // Day of the week.
  int yday;   // Day in the year.
  int isdst;  // Daylight saving time.

  static constexpr BrokenDownTime invalid_value() {
    return BrokenDownTime{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
  }

  StringBuilder &write_to(StringBuilder &builder) const;
};

inline StringBuilder &operator<<(StringBuilder &builder,
                                 const BrokenDownTime &time) {
  return time.write_to(builder);
}

std::ostream &operator<<(std::ostream &stream, const BrokenDownTime &time);

}  // namespace grnxx

#endif  // GRNXX_BROKEN_DOWN_TIME_HPP
