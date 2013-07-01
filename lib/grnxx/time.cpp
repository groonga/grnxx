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
#include "grnxx/time.hpp"

#include <ctime>

#include "grnxx/lock.hpp"
#include "grnxx/mutex.hpp"
#include "grnxx/string_builder.hpp"
#include "grnxx/string_format.hpp"

namespace grnxx {
namespace {

// Note: std::tm does not support usec (microseconds).
BrokenDownTime create_broken_down_time(const std::tm &tm, int64_t count) {
  BrokenDownTime time;
  time.usec = static_cast<int>(count % 1000000);
  time.sec = tm.tm_sec;
  time.min = tm.tm_min;
  time.hour = tm.tm_hour;
  time.mday = tm.tm_mday;
  time.mon = tm.tm_mon;
  time.year = tm.tm_year;
  time.wday = tm.tm_wday;
  time.yday = tm.tm_yday;
  time.isdst = tm.tm_isdst;
  return time;
}

}  // namespace

BrokenDownTime Time::universal_time() const {
  const std::time_t posix_time = static_cast<std::time_t>(count_ / 1000000);
  std::tm tm;
#ifdef GRNXX_MSC
  if (::gmtime_s(&posix_time, &tm) != 0) {
    return BrokenDownTime::invalid_value();
  }
#elif defined(GRNXX_HAS_GMTIME_R)
  if (::gmtime_r(&posix_time, &tm) == nullptr) {
    return BrokenDownTime::invalid_value();
  }
#else  // defined(GRNXX_HAS_GMTIME_R)
  // Lock is used for exclusive access, but it is still not thread-safe
  // because other threads may call std::gmtime().
  static Mutex mutex;
  Lock lock(&mutex);
  std::tm * const shared_tm = std::gmtime(&posix_time);
  if (!shared_tm) {
    return BrokenDownTime::invalid_value();
  }
  tm = *shared_tm;
#endif  // defined(GRNXX_HAS_GMTIME_R)
  return create_broken_down_time(tm, count_);
}

BrokenDownTime Time::local_time() const {
  const std::time_t posix_time = static_cast<std::time_t>(count_ / 1000000);
  std::tm tm;
#ifdef GRNXX_MSC
  if (::localtime_s(&posix_time, &tm) != 0) {
    return BrokenDownTime::invalid_value();
  }
#elif defined(GRNXX_HAS_LOCALTIME_R)
  if (::localtime_r(&posix_time, &tm) == nullptr) {
    return BrokenDownTime::invalid_value();
  }
#else  // defined(GRNXX_HAS_LOCALTIME_R)
  // Lock is used for exclusive access, but it is still not thread-safe
  // because other threads may call std::localtime().
  static Mutex mutex;
  Lock lock(&mutex);
  std::tm * const shared_tm = std::localtime(&posix_time);
  if (!shared_tm) {
    return BrokenDownTime::invalid_value();
  }
  tm = *shared_tm;
#endif  // defined(GRNXX_HAS_LOCALTIME_R)
  return create_broken_down_time(tm, count_);
}

StringBuilder &operator<<(StringBuilder &builder, Time time) {
  if (!builder) {
    return builder;
  }
  uint64_t count;
  if (time.count() >= 0) {
    count = time.count();
  } else {
    builder << '-';
    count = -time.count();
  }
  builder << (count / 1000000);
  count %= 1000000;
  if (count != 0) {
    builder << '.' << StringFormat::align_right(count, 6, '0');
  }
  return builder;
}

}  // namespace grnxx
