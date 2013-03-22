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
#ifndef GRNXX_TIME_STOPWATCH_HPP
#define GRNXX_TIME_STOPWATCH_HPP

#include "grnxx/basic.hpp"
#include "grnxx/time/duration.hpp"

namespace grnxx {

// To measure the amount of time elapsed.
class Stopwatch {
 public:
  // Disable the default constructor so that users don't forget to start a
  // stopwatch.
  Stopwatch() = delete;

  // Construct a stopwatch, which is started if is_running == true.
  explicit Stopwatch(bool is_running);

  // Return true iff the stopwatch is running.
  bool is_running() const {
    return is_running_;
  }

  // Start measurement.
  void start();
  // Stop measurement.
  void stop();

  // Clear the elapsed time.
  void reset();

  // Get the current elapsed time.
  Duration elapsed() const;

 private:
  Duration elapsed_;
  int64_t start_count_;
  bool is_running_;
};

}  // namespace grnxx

#endif  // GRNXX_TIME_STOPWATCH_HPP
