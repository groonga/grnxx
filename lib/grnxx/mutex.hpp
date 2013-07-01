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
#ifndef GRNXX_MUTEX_HPP
#define GRNXX_MUTEX_HPP

#include "grnxx/features.hpp"

#include "grnxx/intrinsic.hpp"
#include "grnxx/duration.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class StringBuilder;

enum MutexStatus : uint32_t {
  MUTEX_UNLOCKED = 0,
  MUTEX_LOCKED   = 1
};

class Mutex {
 public:
  constexpr Mutex() : status_(MUTEX_UNLOCKED) {}

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

  bool locked() const {
    return status_ != MUTEX_UNLOCKED;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  volatile MutexStatus status_;

  void lock_without_timeout();
  bool lock_with_timeout(Duration timeout);
};

inline StringBuilder &operator<<(StringBuilder &builder, const Mutex &mutex) {
  return mutex.write_to(builder);
}

}  // namespace grnxx

#endif  // GRNXX_MUTEX_HPP
