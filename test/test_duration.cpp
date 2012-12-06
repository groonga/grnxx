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
#include <cassert>
#include <sstream>

#include "duration.hpp"
#include "logger.hpp"

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  assert(grnxx::Duration(123).nanoseconds() == 123);

  assert(grnxx::Duration::nanoseconds(123).nanoseconds() == 123);
  assert(grnxx::Duration::seconds(1).nanoseconds() == 1000000000);
  assert(grnxx::Duration::minutes(1).nanoseconds() == 1000000000LL * 60);
  assert(grnxx::Duration::hours(1).nanoseconds() == 1000000000LL * 60 * 60);
  assert(grnxx::Duration::days(1).nanoseconds() ==
         1000000000LL * 60 * 60 * 24);
  assert(grnxx::Duration::weeks(1).nanoseconds() ==
         1000000000LL * 60 * 60 * 24 * 7);

  GRNXX_NOTICE() << "nanosecond = " << grnxx::Duration::nanoseconds(1);
  GRNXX_NOTICE() << "second = " << grnxx::Duration::seconds(1);
  GRNXX_NOTICE() << "minute = " << grnxx::Duration::minutes(1);
  GRNXX_NOTICE() << "hour = " << grnxx::Duration::hours(1);
  GRNXX_NOTICE() << "day = " << grnxx::Duration::days(1);
  GRNXX_NOTICE() << "week = " << grnxx::Duration::weeks(1);

  const grnxx::Duration hour = grnxx::Duration::hours(1);

  assert((hour + hour) == grnxx::Duration::hours(2));
  assert((hour - hour) == grnxx::Duration::hours(0));
  assert((hour * 3) == grnxx::Duration::hours(3));
  assert((hour / 2) == grnxx::Duration::minutes(30));
  assert((hour % grnxx::Duration::minutes(50)) ==
         grnxx::Duration::minutes(10));

  grnxx::Duration duration = grnxx::Duration::weeks(1);

  assert((duration += grnxx::Duration::days(1)) ==
         grnxx::Duration::days(8));
  assert(duration == grnxx::Duration::days(8));

  assert((duration -= grnxx::Duration::weeks(1)) ==
         grnxx::Duration::days(1));
  assert(duration == grnxx::Duration::days(1));

  assert((duration *= 3) == grnxx::Duration::days(3));
  assert(duration == grnxx::Duration::days(3));

  assert((duration /= 24) == grnxx::Duration::hours(3));
  assert(duration == grnxx::Duration::hours(3));

  assert((duration %= grnxx::Duration::hours(3)) ==
         grnxx::Duration::hours(0));
  assert(duration == grnxx::Duration::hours(0));

  assert(grnxx::Duration(123) == grnxx::Duration(123));
  assert(grnxx::Duration(123) != grnxx::Duration(456));

  assert(grnxx::Duration(123) < grnxx::Duration(456));
  assert(grnxx::Duration(456) > grnxx::Duration(123));

  assert(grnxx::Duration(123) <= grnxx::Duration(123));
  assert(grnxx::Duration(123) <= grnxx::Duration(456));

  assert(grnxx::Duration(456) >= grnxx::Duration(456));
  assert(grnxx::Duration(456) >= grnxx::Duration(123));

  std::stringstream stream;
  stream << grnxx::Duration(123456789);
  assert(stream.str() == "0.123456789");

  stream.str("");
  stream << grnxx::Duration::seconds(123);
  assert(stream.str() == "123");

  stream.str("");
  stream << (grnxx::Duration::seconds(456) + grnxx::Duration(789));
  assert(stream.str() == "456.000000789");

  stream.str("");
  stream << grnxx::Duration(-123456789);
  assert(stream.str() == "-0.123456789");

  stream.str("");
  stream << grnxx::Duration::seconds(-123);
  assert(stream.str() == "-123");

  stream.str("");
  stream << -(grnxx::Duration::seconds(456) + grnxx::Duration(789));
  assert(stream.str() == "-456.000000789");

  return 0;
}
