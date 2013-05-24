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
#ifndef GRNXX_TIME_PERIODIC_CLOCK_HPP
#define GRNXX_TIME_PERIODIC_CLOCK_HPP

#include "grnxx/features.hpp"

#include "grnxx/system_clock.hpp"
#include "grnxx/time.hpp"

namespace grnxx {

class PeriodicClock {
 public:
  PeriodicClock();
  ~PeriodicClock();

  static Time now() {
    return (now_ == Time::min()) ? SystemClock::now() : now_;
  }

 private:
  static Time now_;

  static void routine();
};

}  // namespace grnxx

#endif  // GRNXX_TIME_PERIODIC_CLOCK_HPP
