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

#include <chrono>
#include <thread>

namespace grnxx {

void Thread::yield() {
  std::this_thread::yield();
}

void Thread::sleep_for(Duration duration) {
  std::this_thread::sleep_for(std::chrono::microseconds(duration.count()));
}

void Thread::sleep_until(Time time) {
  std::this_thread::sleep_until(std::chrono::system_clock::from_time_t(0) +
                                std::chrono::microseconds(time.count()));
}

}  // namespace grnxx
