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

#include <limits>
#include <memory>
#include <new>

// TODO: Use the following in future.
//#include <chrono>
//#include <thread>

#include "grnxx/errno.hpp"
#include "grnxx/exception.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/string_builder.hpp"
#include "grnxx/system_clock.hpp"
#include "grnxx/types.hpp"

namespace grnxx {
namespace {

enum ThreadStatus : uint32_t {
  THREAD_INITIAL  = 0,
  THREAD_JOINABLE = 1,
  THREAD_JOINED   = 2,
  THREAD_DETACHED = 3
};

StringBuilder &operator<<(StringBuilder &builder, ThreadStatus status) {
  switch (status) {
    case THREAD_INITIAL: {
      return builder << "THREAD_INITIAL";
    }
    case THREAD_JOINABLE: {
      return builder << "THREAD_JOINABLE";
    }
    case THREAD_JOINED: {
      return builder << "THREAD_JOINED";
    }
    case THREAD_DETACHED: {
      return builder << "THREAD_DETACHED";
    }
    default: {
      return builder << "n/a";
    }
  }
}

class ThreadImpl : public Thread {
 public:
  using Routine = Thread::Routine;

  ThreadImpl();
  ~ThreadImpl();

  void start(const Routine &routine);

  void join();
  void detach();

 private:
#ifdef GRNXX_WINDOWS
  HANDLE thread_;
#else  // GRNXX_WINDOWS
  pthread_t thread_;
#endif  // GRNXX_WINDOWS
  ThreadStatus status_;

#ifdef GRNXX_WINDOWS
  static unsigned thread_main(void *arg);
#else  // GRNXX_WINDOWS
  static void *thread_main(void *arg);
#endif  // GRNXX_WINDOWS
};

ThreadImpl::ThreadImpl() : thread_(), status_(THREAD_INITIAL) {}

ThreadImpl::~ThreadImpl() {
  // A thread must be joined or detached before destruction.
  if (status_ == THREAD_JOINABLE) {
    GRNXX_WARNING() << "Bad thread destruction: status = " << status_;
  }
}

void ThreadImpl::start(const Routine &routine) {
  std::unique_ptr<Routine> routine_clone(new (std::nothrow) Routine(routine));
  if (!routine_clone) {
    GRNXX_ERROR() << "new grnxx::Thread::Routine failed";
    throw MemoryError();
  }
#ifdef GRNXX_WINDOWS
  const uintptr_t handle = ::_beginthreadex(nullptr, 0, thread_main,
                                            routine_clone.get(), 0, nullptr);
  if (handle == 0) {
    const Errno error_code(errno);
    GRNXX_ERROR() << "failed to create thread: call = _beginthreadex"
                  << ", errno = " << error_code;
    throw SystemError(error_code);
  }
  thread_ = reinterpret_cast<HANDLE>(handle);
#else  // GRNXX_WINDOWS
  const int error = ::pthread_create(&thread_, nullptr, thread_main,
                                     routine_clone.get());
  if (error != 0) {
    const Errno error_code(error);
    GRNXX_ERROR() << "failed to create thread: call = ::pthread_create"
                  << ", errno = " << error_code;
    throw SystemError(error_code);
  }
#endif  // GRNXX_WINDOWS
  routine_clone.release();
  status_ = THREAD_JOINABLE;
}

void ThreadImpl::join() {
  if (status_ != THREAD_JOINABLE) {
    GRNXX_ERROR() << "invalid operation: status = " << status_;
    throw LogicError();
  }
  status_ = THREAD_JOINED;
#ifdef GRNXX_WINDOWS
  if (::WaitForSingleObject(thread_, INFINITE) == WAIT_FAILED) {
    const Errno error_code(::GetLastError());
    GRNXX_ERROR() << "failed to join thread: call = ::WaitForSingleObject"
                  << ", errno = " << error_code;
    throw SystemError(error_code);
  }
  if (::CloseHandle(thread_) != 0) {
    const Errno error_code(::GetLastError());
    GRNXX_ERROR() << "failed to close thread: call = ::CloseHandle"
                  << ", errno = " << error_code;
    throw SystemError(error_code);
  }
#else  // GRNXX_WINDOWS
  const int error = ::pthread_join(thread_, nullptr);
  if (error != 0) {
    const Errno error_code(error);
    GRNXX_ERROR() << "failed to join thread: call = ::pthread_join"
                  << ", errno = " << error_code;
    throw SystemError(error_code);
  }
#endif  // GRNXX_WINDOWS
}

void ThreadImpl::detach() {
  if (status_ != THREAD_JOINABLE) {
    GRNXX_ERROR() << "invalid operation: status = " << status_;
    throw LogicError();
  }
  status_ = THREAD_DETACHED;
#ifdef GRNXX_WINDOWS
  if (::CloseHandle(thread_) != 0) {
    const Errno error_code(::GetLastError());
    GRNXX_ERROR() << "failed to detach thread: call = ::CloseHandle"
                  << ", errno = " << error_code;
    throw SystemError(error_code);
  }
#else  // GRNXX_WINDOWS
  const int error = ::pthread_detach(thread_);
  if (error != 0) {
    const Errno error_code(error);
    GRNXX_ERROR() << "failed to detach thread: call = ::pthread_detach"
                  << ", errno = " << error_code;
    throw SystemError(error_code);
  }
#endif  // GRNXX_WINDOWS
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
    throw MemoryError();
  }
  thread->start(routine);
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
