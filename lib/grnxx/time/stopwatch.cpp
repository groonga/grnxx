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
#include "grnxx/time/stopwatch.hpp"

#include <chrono>

namespace grnxx {
namespace {

int64_t now() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}

}  // namespace

Stopwatch::Stopwatch(bool is_running)
    : elapsed_(0),
      start_count_(is_running ? now() : 0),
      is_running_(is_running) {}

void Stopwatch::start() {
  if (!is_running_) {
    start_count_ = now();
    is_running_ = true;
  }
}

void Stopwatch::stop() {
  if (is_running_) {
    elapsed_ += Duration(now() - start_count_);
    is_running_ = false;
  }
}

void Stopwatch::reset() {
  if (is_running_) {
    start_count_ = now();
  }
  elapsed_ = Duration(0);
}

Duration Stopwatch::elapsed() const {
  if (is_running_) {
    return elapsed_ + Duration(now() - start_count_);
  } else {
    return elapsed_;
  }
}

}  // namespace grnxx
