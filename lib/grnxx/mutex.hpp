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
#ifndef GRNXX_MUTEX_HPP
#define GRNXX_MUTEX_HPP

#include "grnxx/basic.hpp"
#include "grnxx/intrinsic.hpp"
#include "grnxx/time/duration.hpp"

namespace grnxx {

class StringBuilder;

enum MutexStatus : uint32_t {
  MUTEX_UNLOCKED = 0,
  MUTEX_LOCKED   = 1
};

constexpr int MUTEX_SPIN_COUNT           = 100;
constexpr int MUTEX_CONTEXT_SWITCH_COUNT = 100;
constexpr Duration MUTEX_SLEEP_DURATION  = Duration::milliseconds(10);

class Mutex {
 public:
  Mutex() = default;
  explicit constexpr Mutex(MutexStatus status) : status_(status) {}

  void lock() {
    if (!try_lock()) {
      lock_without_timeout();
    }
  }
  bool lock(Duration timeout) {
    if (try_lock()) {
      return true;
    }
    return lock_with_timeout(timeout);
  }
  bool try_lock() {
    if (locked()) {
      return false;
    }
    return atomic_compare_and_swap(MUTEX_UNLOCKED, MUTEX_LOCKED, &status_);
  }
  bool unlock() {
    if (!locked()) {
      return false;
    }
    status_ = MUTEX_UNLOCKED;
    return true;
  }

  constexpr bool locked() {
    return status_ != MUTEX_UNLOCKED;
  }

  void swap(Mutex &mutex) {
    using std::swap;
    std::swap(status_, mutex.status_);
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  MutexStatus status_;

  void lock_without_timeout();
  bool lock_with_timeout(Duration timeout);
};

GRNXX_ASSERT_POD(Mutex);

inline void swap(Mutex &lhs, Mutex &rhs) {
  lhs.swap(rhs);
}

inline StringBuilder &operator<<(StringBuilder &builder, const Mutex &mutex) {
  return mutex.write_to(builder);
}

}  // namespace grnxx

#endif  // GRNXX_MUTEX_HPP
