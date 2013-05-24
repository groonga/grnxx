/*
  Copyright (C) 2013  Brazil, Inc.

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
#include "grnxx/system_clock.hpp"

#include <chrono>

namespace grnxx {

Time SystemClock::now() {
  // The epoch of std::chrono::system_clock is not guaranteed to be the Unix
  // epoch. So, (now() - from_time_t(0)) is used instead of time_since_epoch().
  return Time(std::chrono::duration_cast<std::chrono::microseconds>(
              (std::chrono::system_clock::now() -
               std::chrono::system_clock::from_time_t(0))).count());
}

}  // namespace grnxx
