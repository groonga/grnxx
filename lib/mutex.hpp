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

#include "basic.hpp"
#include "intrinsic.hpp"
#include "string_builder.hpp"
#include "thread.hpp"

namespace grnxx {

const int      MUTEX_THREAD_SWITCH_COUNT_DEFAULT = 1000;
const int      MUTEX_SLEEP_COUNT_DEFAULT         = 6000;
const Duration MUTEX_SLEEP_DURATION_DEFAULT      = Duration::milliseconds(10);

class Mutex {
 public:
  typedef uint32_t Value;
  typedef volatile Value Object;

  static const Value UNLOCKED = 0;
  static const Value LOCKED = 1;

  Mutex() : object_(UNLOCKED) {}

  Mutex(Mutex &&mutex) : object_(std::move(mutex.object_)) {}
  Mutex &operator=(Mutex &&mutex) {
    object_ = std::move(mutex.object_);
    return *this;
  }

  bool lock(int thread_switch_count = MUTEX_THREAD_SWITCH_COUNT_DEFAULT,
            int sleep_count = MUTEX_SLEEP_COUNT_DEFAULT,
            Duration sleep_duration = MUTEX_SLEEP_DURATION_DEFAULT) {
    return lock(&object_, thread_switch_count, sleep_count, sleep_duration);
  }
  bool try_lock() {
    return try_lock(&object_);
  }
  bool unlock() {
    return unlock(&object_);
  }

  Value value() const {
    return object_;
  }

  static bool lock(Object *object,
                   int thread_switch_count = MUTEX_THREAD_SWITCH_COUNT_DEFAULT,
                   int sleep_count = MUTEX_SLEEP_COUNT_DEFAULT,
                   Duration sleep_duration = MUTEX_SLEEP_DURATION_DEFAULT) {
    for (int i = 0; i < thread_switch_count; ++i) {
      if (try_lock(object)) {
        return true;
      }
      Thread::switch_to_others();
    }
    for (int i = 0; i < sleep_count; ++i) {
      if (try_lock(object)) {
        return true;
      }
      Thread::sleep(sleep_duration);
    }
    return false;
  }
  static bool try_lock(Object *object) {
    return atomic_compare_and_swap(UNLOCKED, LOCKED, object);
  }
  static bool unlock(Object *object) {
    if (*object == UNLOCKED) {
      return false;
    }
    *object = UNLOCKED;
    return true;
  }

  void clear() {
    object_ = UNLOCKED;
  }
  void swap(Mutex &mutex) {
    using std::swap;
    swap(object_, mutex.object_);
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  Object object_;

  Mutex(const Mutex &);
  Mutex &operator=(const Mutex &);
};

inline void swap(Mutex &lhs, Mutex &rhs) {
  lhs.swap(rhs);
}

inline StringBuilder &operator<<(StringBuilder &builder, const Mutex &mutex) {
  return mutex.write_to(builder);
}

}  // namespace grnxx

#endif  // GRNXX_MUTEX_HPP
