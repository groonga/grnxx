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
#include <cassert>

#include "logger.hpp"
#include "recycler.hpp"
#include "steady_clock.hpp"

void test() {
  const grnxx::Duration FROZEN_DURATION = grnxx::Duration::minutes(10);

  grnxx::Recycler recycler(FROZEN_DURATION);

  assert(recycler.frozen_duration() == FROZEN_DURATION);

  for (uint16_t i = 1; i < grnxx::RECYCLER_STAMP_COUNT_PER_UPDATE; ++i) {
    assert(recycler.stamp() == 0);
  }

  for (uint16_t i = 0; i < grnxx::RECYCLER_STAMP_COUNT_PER_UPDATE; ++i) {
    assert(recycler.stamp() == 1);
  }

  for (uint16_t i = 0; i < grnxx::RECYCLER_STAMP_COUNT_PER_UPDATE; ++i) {
    assert(recycler.stamp() == 1);
  }

  GRNXX_NOTICE() << "recycler = " << recycler;
}

void benchmark(grnxx::Duration frozen_duration) {
  const int STAMP_COUNT = 1 << 20;
  const int CHECK_COUNT = 1 << 20;

  grnxx::Recycler recycler(frozen_duration);
  assert(recycler.frozen_duration() == frozen_duration);

  double stamp_elapsed;
  {
    const grnxx::Time start = grnxx::SteadyClock::now();
    for (int i = 0; i < STAMP_COUNT; ++i) {
      recycler.stamp();
    }
    const grnxx::Time end = grnxx::SteadyClock::now();
    stamp_elapsed = 1000.0 * (end - start).count() / STAMP_COUNT;
  }

  double check_elapsed;
  {
    const grnxx::Time start = grnxx::SteadyClock::now();
    for (int i = 0; i < CHECK_COUNT; ++i) {
      recycler.check(std::uint16_t(i));
    }
    const grnxx::Time end = grnxx::SteadyClock::now();
    check_elapsed = 1000.0 * (end - start).count() / CHECK_COUNT;
  }

  GRNXX_NOTICE() << "frozen_duration = " << frozen_duration
                 << ", stamp [ns] = " << stamp_elapsed
                 << ", check [ns] = " << check_elapsed
                 << ", stamp = " << recycler.stamp();
}

void benchmark() {
  benchmark(grnxx::Duration::seconds(1));
  benchmark(grnxx::Duration::milliseconds(100));
  benchmark(grnxx::Duration::milliseconds(10));
  benchmark(grnxx::Duration::milliseconds(1));
  benchmark(grnxx::Duration::microseconds(1));
}

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test();
  benchmark();

  return 0;
}
