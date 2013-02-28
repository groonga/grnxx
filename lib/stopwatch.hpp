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
#ifndef GRNXX_STOPWATCH_HPP
#define GRNXX_STOPWATCH_HPP

#include "basic.hpp"
#include "steady_clock.hpp"

namespace grnxx {

enum {
  STOPWATCH_RUNNING
};

// To measure the amount of time elapsed.
class Stopwatch {
 public:
  // Construct a stopwatch, which is started if is_running == true.
  explicit Stopwatch(bool is_running = false)
    : elapsed_(0),
      start_time_(),
      is_running_(is_running) {
    if (is_running) {
      start_time_ = SteadyClock::now();
    }
  }

  // Return true iff the stopwatch is running.
  bool is_running() const {
    return is_running_;
  }

  // Start measurement.
  void start() {
    if (!is_running_) {
      start_time_ = SteadyClock::now();
      is_running_ = true;
    }
  }
  // Stop measurement.
  void stop() {
    if (is_running_) {
      elapsed_ += SteadyClock::now() - start_time_;
      is_running_ = false;
    }
  }

  // Clear the elapsed time.
  void reset() {
    if (is_running_) {
      start_time_ = SteadyClock::now();
    }
    elapsed_ = Duration(0);
  }

  // Get the current elapsed time.
  Duration elapsed() const {
    if (is_running_) {
      return elapsed_ + (SteadyClock::now() - start_time_);
    } else {
      return elapsed_;
    }
  }

 private:
  Duration elapsed_;
  Time start_time_;
  bool is_running_;
};

}  // namespace grnxx

#endif  // GRNXX_STOPWATCH_HPP
