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
#include "mutex.hpp"

#include "stopwatch.hpp"
#include "thread.hpp"

namespace grnxx {

void Mutex::lock_without_timeout() {
  for (int i = 0; i < MUTEX_SPIN_COUNT; ++i) {
    if (try_lock()) {
      return;
    }
  }

  for (int i = 0; i < MUTEX_CONTEXT_SWITCH_COUNT; ++i) {
    if (try_lock()) {
      return;
    }
    Thread::switch_to_others();
  }

  while (!try_lock()) {
    Thread::sleep(MUTEX_SLEEP_DURATION);
  }
}

bool Mutex::lock_with_timeout(Duration timeout) {
  if (timeout == Duration(0)) {
    return false;
  }

  for (int i = 0; i < MUTEX_SPIN_COUNT; ++i) {
    if (try_lock()) {
      return true;
    }
  }

  const bool has_deadline = timeout >= Duration(0);
  Stopwatch stopwatch(false);
  if (has_deadline) {
    stopwatch.start();
  }

  for (int i = 0; i < MUTEX_CONTEXT_SWITCH_COUNT; ++i) {
    if (has_deadline && (stopwatch.elapsed() >= timeout)) {
      return false;
    }
    if (try_lock()) {
      return true;
    }
    Thread::switch_to_others();
  }

  while (!has_deadline || (stopwatch.elapsed() < timeout)) {
    if (try_lock()) {
      return true;
    }
    Thread::sleep(MUTEX_SLEEP_DURATION);
  }

  return false;
}

StringBuilder &Mutex::write_to(StringBuilder &builder) const {
  switch (status_) {
    case MUTEX_UNLOCKED: {
      return builder << "unlocked";
    }
    case MUTEX_LOCKED: {
      return builder << "locked";
    }
    default: {
      return builder << "n/a";
    }
  }
}

}  // namespace grnxx
