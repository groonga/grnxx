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
#include "thread.hpp"

#ifdef GRNXX_WINDOWS
# ifndef NOMINMAX
#  define NOMINMAX
# endif  // NOMINMAX
# include <windows.h>
#endif  // GRNXX_WINDOWS

#ifdef GRNXX_HAS_NANOSLEEP
# include <time.h>
#endif  // GRNXX_HAS_NANOSLEEP

#include <thread>

namespace grnxx {

void Thread::yield() {
  std::this_thread::yield();
}

void Thread::sleep(Duration duration) {
#ifdef GRNXX_WINDOWS
  if (duration.count() < 0) {
    ::Sleep(0);
  } else {
    const int64_t milliseconds = duration.count() / 1000;
    if (milliseconds <
        static_cast<int64_t>(std::numeric_limits<DWORD>::max())) {
      ::Sleep(static_cast<DWORD>(milliseconds));
    } else {
      ::Sleep(std::numeric_limits<DWORD>::max());
    }
  }
#elif defined(GRNXX_HAS_NANOSLEEP)
  struct timespec request;
  if (duration.count() < 0) {
    request.tv_sec = 0;
    request.tv_nsec = 0;
  } else {
    const int64_t seconds = duration.count() / 1000000;
    if (seconds < std::numeric_limits<time_t>::max()) {
      request.tv_sec = static_cast<time_t>(seconds);
    } else {
      request.tv_sec = std::numeric_limits<time_t>::max();
    }
    duration %= Duration::seconds(1);
    request.tv_nsec = static_cast<long>(duration.count() * 1000);
  }
  // Note that ::nanosleep() requires -lrt option.
  ::nanosleep(&request, nullptr);
#else  // defined(GRNXX_HAS_NANOSLEEP)
  // Note that POSIX.1-2008 removes the specification of ::usleep().
  const int64_t microseconds = duration.count();
  if (microseconds < std::numeric_limits<useconds_t>::max()) {
    ::usleep(static_cast<useconds_t>(microseconds));
  } else {
    ::usleep(std::numeric_limits<useconds_t>::max());
  }
#endif  // defined(GRNXX_HAS_NANOSLEEP)
}

}  // namespace grnxx
