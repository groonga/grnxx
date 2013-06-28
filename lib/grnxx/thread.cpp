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
#include "grnxx/thread.hpp"

#ifdef GRNXX_WINDOWS
# include <process.h>
# include <windows.h>
#else  // GRNXX_WINDOWS
# include <pthread.h>
#endif  // GRNXX_WINDOWS

#ifdef GRNXX_HAS_SCHED_YIELD
# include <sched.h>
#endif  // GRNXX_HAS_SCHED_YIELD

#ifdef GRNXX_HAS_NANOSLEEP
# include <time.h>
#endif  // GRNXX_HAS_NANOSLEEP

#ifdef GRNXX_WINDOWS
# include <cerrno>
#endif  // GRNXX_WINDOWS

// TODO: Use the following in future.
//#include <chrono>
//#include <thread>

#include "grnxx/errno.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/system_clock.hpp"

namespace grnxx {
namespace {

class ThreadImpl : public Thread {
 public:
  using Routine = Thread::Routine;

  ThreadImpl();
  ~ThreadImpl();

  bool start(const Routine &routine);

  bool join();
  bool detach();

 private:
#ifdef GRNXX_WINDOWS
  HANDLE thread_;
#else  // GRNXX_WINDOWS
  pthread_t thread_;
#endif  // GRNXX_WINDOWS
  bool joinable_;

#ifdef GRNXX_WINDOWS
  static unsigned thread_main(void *arg);
#else  // GRNXX_WINDOWS
  static void *thread_main(void *arg);
#endif  // GRNXX_WINDOWS
};

ThreadImpl::ThreadImpl() : thread_(), joinable_(false) {}

ThreadImpl::~ThreadImpl() {
  // A thread must be join()ed or detach()ed.
  if (joinable_) {
    GRNXX_ERROR() << "running thread";
  }
}

bool ThreadImpl::start(const Routine &routine) {
  std::unique_ptr<Routine> routine_clone(new (std::nothrow) Routine(routine));
  if (!routine_clone) {
    GRNXX_ERROR() << "new std::function<void()> failed";
    return false;
  }
#ifdef GRNXX_WINDOWS
  const uintptr_t handle = ::_beginthreadex(nullptr, 0, thread_main,
                                            routine_clone.get(), 0, nullptr);
  if (handle == 0) {
    GRNXX_ERROR() << "failed to create thread: '_beginthreadex' "
                  << Errno(errno);
    return false;
  }
  thread_ = reinterpret_cast<HANDLE>(handle);
#else  // GRNXX_WINDOWS
  const int error = ::pthread_create(&thread_, nullptr, thread_main,
                                     routine_clone.get());
  if (error != 0) {
    GRNXX_ERROR() << "failed to create thread: '::pthread_create' "
                  << Errno(error);
    return false;
  }
#endif  // GRNXX_WINDOWS
  routine_clone.release();
  joinable_ = true;
  return true;
}

bool ThreadImpl::join() {
  if (!joinable_) {
    GRNXX_ERROR() << "invalid operation: joinable = false";
    return false;
  }
  bool result = true;
#ifdef GRNXX_WINDOWS
  if (::WaitForSingleObject(thread_, INFINITE) == WAIT_FAILED) {
    GRNXX_ERROR() << "failed to join thread: '::WaitForSingleObject' "
                  << Errno(::GetLastError());
    result = false;
  }
  if (::CloseHandle(thread_) != 0) {
    GRNXX_ERROR() << "failed to close thread: '::CloseHandle' "
                  << Errno(::GetLastError());
    result = false;
  }
#else  // GRNXX_WINDOWS
  const int error = ::pthread_join(thread_, nullptr);
  if (error != 0) {
    GRNXX_ERROR() << "failed to join thread: '::pthread_join' "
                  << Errno(error);
    result = false;
  }
#endif  // GRNXX_WINDOWS
  joinable_ = false;
  return result;
}

bool ThreadImpl::detach() {
  if (!joinable_) {
    GRNXX_ERROR() << "invalid operation: joinable = false";
    return false;
  }
  bool result = true;
#ifdef GRNXX_WINDOWS
  if (::CloseHandle(thread_) != 0) {
    GRNXX_ERROR() << "failed to detach thread: '::CloseHandle' "
                  << Errno(::GetLastError());
    result = false;
  }
#else  // GRNXX_WINDOWS
  const int error = ::pthread_detach(thread_);
  if (error != 0) {
    GRNXX_ERROR() << "failed to detach thread: '::pthread_detach' "
                  << Errno(error);
    result = false;
  }
#endif  // GRNXX_WINDOWS
  joinable_ = false;
  return result;
}

#ifdef GRNXX_WINDOWS
unsigned ThreadImpl::thread_main(void *arg) {
  std::unique_ptr<Routine> routine(static_cast<Routine *>(arg));
  (*routine)();
  ::_endthreadex(0);
  return 0;
}
#else  // GRNXX_WINDOWS
void *ThreadImpl::thread_main(void *arg) {
  std::unique_ptr<Routine> routine(static_cast<Routine *>(arg));
  (*routine)();
  return nullptr;
}
#endif  // GRNXX_WINDOWS

}  // namespace

Thread::Thread() {}
Thread::~Thread() {}

Thread *Thread::create(const Routine &routine) {
  std::unique_ptr<ThreadImpl> thread(new (std::nothrow) ThreadImpl);
  if (!thread) {
    GRNXX_ERROR() << "new grnxx::ThreadImpl failed";
    return nullptr;
  }
  if (!thread->start(routine)) {
    return nullptr;
  }
  return thread.release();
}

void Thread::yield() {
#ifdef GRNXX_WINDOWS
  ::SwitchToThread();
#elif defined(GRNXX_HAS_SCHED_YIELD)
  ::sched_yield();
#else  // defined(GRNXX_HAS_SCHED_YIELD)
  sleep_for(Duration(0));
#endif  // defined(GRNXX_HAS_SCHED_YIELD)
  // TODO: Use the following in future.
//  std::this_thread::yield();
}

void Thread::sleep_for(Duration duration) {
#ifdef GRNXX_WINDOWS
  if (duration.count() <= 0) {
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
  if (duration.count() <= 0) {
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
#else // defined(GRNXX_HAS_NANOSLEEP)
  // Note that POSIX.1-2008 removes the specification of ::usleep().
  if (duration.count() <= 0) {
    ::usleep(0);
  } else {
    const int64_t microseconds = duration.count();
    if (microseconds < std::numeric_limits<useconds_t>::max()) {
      ::usleep(static_cast<useconds_t>(microseconds));
    } else {
      ::usleep(std::numeric_limits<useconds_t>::max());
    }
  }
#endif // defined(GRNXX_HAS_NANOSLEEP)
  // TODO: Use the following in future.
//  std::this_thread::sleep_for(std::chrono::microseconds(duration.count()));
}

void Thread::sleep_until(Time time) {
  const Time now = SystemClock::now();
  sleep_for((time > now) ? (time - now) : Duration(0));
}

}  // namespace grnxx
