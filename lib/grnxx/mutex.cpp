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
#include "grnxx/mutex.hpp"

#include "grnxx/string_builder.hpp"
#include "grnxx/thread.hpp"
#include "grnxx/stopwatch.hpp"

namespace grnxx {
namespace {

constexpr int MUTEX_SPIN_COUNT           = 100;
constexpr int MUTEX_CONTEXT_SWITCH_COUNT = 100;
constexpr Duration MUTEX_SLEEP_DURATION  = Duration::milliseconds(10);

}  // namespace

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
    Thread::yield();
  }
  while (!try_lock()) {
    Thread::sleep_for(MUTEX_SLEEP_DURATION);
  }
}

bool Mutex::lock_with_timeout(Duration timeout) {
  if (timeout <= Duration(0)) {
    return false;
  }
  for (int i = 0; i < MUTEX_SPIN_COUNT; ++i) {
    if (try_lock()) {
      return true;
    }
  }
  Stopwatch stopwatch(true);
  for (int i = 0; i < MUTEX_CONTEXT_SWITCH_COUNT; ++i) {
    if (stopwatch.elapsed() >= timeout) {
      return false;
    }
    if (try_lock()) {
      return true;
    }
    Thread::yield();
  }
  while (stopwatch.elapsed() < timeout) {
    if (try_lock()) {
      return true;
    }
    Thread::sleep_for(MUTEX_SLEEP_DURATION);
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
