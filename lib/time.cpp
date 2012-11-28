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
#include "time.hpp"

#ifdef GRNXX_WINDOWS
# include <sys/types.h>
# include <sys/timeb.h>
#endif  // GRNXX_WINDOWS

#ifndef GRNXX_HAS_CLOCK_GETTIME
# include <sys/time.h>
#endif  // GRNXX_HAS_CLOCK_GETTIME

#include <cerrno>
#include <ctime>
#include <ostream>

#include "error.hpp"
#include "intrinsic.hpp"
#include "logger.hpp"
#include "string_format.hpp"

namespace grnxx {

Time Time::now() {
#ifdef GRNXX_WINDOWS
  struct _timeb current_time;
# ifdef GRNXX_MSC
  const errno_t error_code = ::_ftime_s(&current_time);
  if (error_code != 0) {
    // In practice, ::_ftime_s() must not fail.
    return now_in_seconds();
  }
# else  // GRNXX_MSC
  ::_ftime(&current_time);
# endif  // GRNXX_MSC
  return Time((current_time.time * int64_t(1000000000)) +
              (current_time.millitm * 1000000));
#else  // GRNXX_WINDOWS
# if defined(GRNXX_HAS_CLOCK_GETTIME)
  struct timespec current_time;
  if (::clock_gettime(CLOCK_REALTIME, &current_time) != 0) {
    // In practice, ::clock_gettime() must not fail.
    return now_in_seconds();
  }
  return Time((current_time.tv_sec * int64_t(1000000000)) +
              current_time.tv_nsec);
# else  // defined(GRNXX_HAS_CLOCK_GETTIME)
  // Note: POSIX.1-2008 marks gettimeofday() as obsolete.
  struct timeval current_time;
  if (::gettimeofday(&current_time, nullptr) != 0) {
    // In practice, ::gettimeofday() must not fail.
    return now_in_seconds();
  }
  return Time((current_time.tv_sec * int64_t(1000000000)) +
              (current_time.tv_usec * 1000));
# endif  // defined(GRNXX_HAS_CLOCK_GETTIME)
#endif  // GRNXX_WINDOWS
}

Time Time::now_in_seconds() {
  return Time(static_cast<int64_t>(std::time(nullptr)) * 1000000000);
}

StringBuilder &Time::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  const std::time_t posix_time =
      static_cast<std::time_t>(nanoseconds_ / 1000000000);
  struct tm broken_down_time;
#ifdef GRNXX_MSC
  if (::localtime_s(&posix_time, &broken_down_time) != 0) {
    return builder << "0000-00-00 00:00:00.000000";
  }
#elif defined(GRNXX_HAS_LOCALTIME_R)
  if (::localtime_r(&posix_time, &broken_down_time) == nullptr) {
    return builder << "0000-00-00 00:00:00.000000";
  }
#else  // defined(GRNXX_HAS_LOCALTIME_R)
  // TODO: should be thread-safe.
  struct tm * const global_broken_down_time = ::localtime(&posix_time);
  if (global_broken_down_time == nullptr) {
    return builder << "0000-00-00 00:00:00.000000";
  }
  broken_down_time = *global_broken_down_time;
#endif  // defined(GRNXX_HAS_LOCALTIME_R)

  builder << (1900 + broken_down_time.tm_year) << '-'
          << StringFormat::align_right(broken_down_time.tm_mon + 1, 2, '0') << '-'
          << StringFormat::align_right(broken_down_time.tm_mday, 2, '0') << ' '
          << StringFormat::align_right(broken_down_time.tm_hour, 2, '0') << ':'
          << StringFormat::align_right(broken_down_time.tm_min, 2, '0') << ':'
          << StringFormat::align_right(broken_down_time.tm_sec, 2, '0') << '.'
          << StringFormat::align_right(nanoseconds_ / 1000 % 1000000, 6, '0');
  return builder;
}

std::ostream &operator<<(std::ostream &stream, Time time) {
  char buf[32];
  StringBuilder builder(buf);
  builder << time;
  return stream.write(builder.c_str(), builder.length());
}

}  // namespace grnxx
