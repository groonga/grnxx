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
#ifndef GRNXX_THREAD_HPP
#define GRNXX_THREAD_HPP

#include "grnxx/features.hpp"

#include <functional>

#include "grnxx/duration.hpp"
#include "grnxx/time.hpp"

namespace grnxx {

class Thread {
 public:
  using Routine = std::function<void()>;

  Thread();
  virtual ~Thread();

  // Create a thread.
  static Thread *create(const std::function<void()> &routine);

  // Yield the processor/core associated with the current thread.
  static void yield();

  // Sleep for "duration".
  static void sleep_for(Duration duration);
  // Sleep until "time".
  static void sleep_until(Time time);

  // Wait until the thread finishes.
  virtual bool join() = 0;
  // Separate the thread from this object.
  virtual bool detach() = 0;
};

}  // namespace grnxx

#endif  // GRNXX_THREAD_HPP
