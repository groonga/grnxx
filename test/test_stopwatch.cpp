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
#include <cassert>

#include "logger.hpp"
#include "thread.hpp"
#include "time/stopwatch.hpp"

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  grnxx::Stopwatch stopwatch(false);
  assert(stopwatch.elapsed() == grnxx::Duration(0));

  stopwatch.start();
  grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(1));
  grnxx::Duration elapsed = stopwatch.elapsed();
  assert(elapsed > grnxx::Duration(0));

  grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(1));
  assert(stopwatch.elapsed() > elapsed);

  stopwatch.stop();
  elapsed = stopwatch.elapsed();

  grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(1));
  assert(stopwatch.elapsed() == elapsed);

  stopwatch.start();
  grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(1));
  assert(stopwatch.elapsed() > elapsed);

  GRNXX_NOTICE() << "stopwatch.elapsed() = " << stopwatch.elapsed();

  elapsed = stopwatch.elapsed();
  stopwatch.reset();
  grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(1));
  assert(stopwatch.elapsed() < elapsed);
  assert(stopwatch.elapsed() > grnxx::Duration(0));

  stopwatch.stop();
  stopwatch.reset();
  assert(stopwatch.elapsed() == grnxx::Duration(0));

  grnxx::Thread::sleep_for(grnxx::Duration::milliseconds(1));
  assert(stopwatch.elapsed() == grnxx::Duration(0));

  return 0;
}
