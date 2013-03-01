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

#include "intrinsic.hpp"
#include "logger.hpp"
#include "stopwatch.hpp"

void test_basics() {
  assert(grnxx::bit_scan_reverse(std::uint8_t(100)) == 6);
  assert(grnxx::bit_scan_reverse(std::uint16_t(0xFFF)) == 11);
  assert(grnxx::bit_scan_reverse(std::uint32_t(1) << 30) == 30);
  assert(grnxx::bit_scan_reverse(std::uint64_t(-1)) == 63);

  assert(grnxx::bit_scan_forward(std::uint8_t(100)) == 2);
  assert(grnxx::bit_scan_forward(std::uint16_t(0xFFF)) == 0);
  assert(grnxx::bit_scan_forward(std::uint32_t(1) << 30) == 30);
  assert(grnxx::bit_scan_forward(std::uint64_t(1) << 63) == 63);

  volatile std::int32_t value_32 = 0;
  assert(grnxx::atomic_fetch_and_add(5, &value_32) == 0);
  assert(grnxx::atomic_fetch_and_add(-10, &value_32) == 5);
  assert(grnxx::atomic_fetch_and_add(5, &value_32) == -5);

  value_32 = 0;
  assert(grnxx::atomic_fetch_and_or(std::int32_t(0x15), &value_32) == 0);
  assert(grnxx::atomic_fetch_and_and(std::int32_t(0x10), &value_32) == 0x15);
  assert(grnxx::atomic_fetch_and_xor(std::int32_t(0xFF), &value_32) == 0x10);
  assert(value_32 == 0xEF);

  volatile std::int64_t value_64 = 0;
  assert(grnxx::atomic_fetch_and_add(std::int64_t(1) << 50, &value_64) == 0);
  assert(grnxx::atomic_fetch_and_add(
      std::int64_t(-1) << 51, &value_64) == std::int64_t(1) << 50);
  assert(grnxx::atomic_fetch_and_add(
      std::int64_t(1) << 50, &value_64) == std::int64_t(-1) << 50);

  value_64 = 0;
  assert(grnxx::atomic_fetch_and_or(std::int64_t(0x1515), &value_64) == 0);
  assert(grnxx::atomic_fetch_and_and(
      std::int64_t(0x130F), &value_64) == 0x1515);
  assert(grnxx::atomic_fetch_and_xor(
      std::int64_t(0x3327), &value_64) == 0x1105);
  assert(value_64 == 0x2222);

  std::uint64_t buf[16];
  std::uint64_t *ptr = &buf[0];
  assert(grnxx::atomic_fetch_and_add(1, &ptr) == &buf[0]);
  assert(grnxx::atomic_fetch_and_add(2, &ptr) == &buf[1]);
  assert(grnxx::atomic_fetch_and_add(3, &ptr) == &buf[3]);
  assert(ptr == &buf[6]);

  double value_float = 0.0;
  assert(grnxx::atomic_fetch_and_add(1, &value_float) == 0.0);
  assert(grnxx::atomic_fetch_and_add(2.0, &value_float) == 1.0);
  assert(value_float == 3.0);

  value_32 = 0;
  assert(grnxx::atomic_compare_and_swap(
      std::int32_t(0), std::int32_t(1), &value_32));
  assert(grnxx::atomic_compare_and_swap(
      std::int32_t(1), std::int32_t(2), &value_32));
  assert(!grnxx::atomic_compare_and_swap(
      std::int32_t(0), std::int32_t(1), &value_32));

  value_64 = 0;
  assert(grnxx::atomic_compare_and_swap(
      std::int64_t(0), std::int64_t(10), &value_64));
  assert(!grnxx::atomic_compare_and_swap(
      std::int64_t(0), std::int64_t(20), &value_64));
  assert(grnxx::atomic_compare_and_swap(
      std::int64_t(10), std::int64_t(20), &value_64));

  assert(grnxx::atomic_compare_and_swap(&buf[6], &buf[0], &ptr));
  assert(ptr == &buf[0]);
  assert(!grnxx::atomic_compare_and_swap(&buf[1], &buf[2], &ptr));

  assert(grnxx::atomic_compare_and_swap(3.0, 0.0, &value_float));
  assert(value_float == 0.0);
  assert(!grnxx::atomic_compare_and_swap(1.0, 2.0, &value_float));
}

void test_times() {
  enum { LOOP_COUNT = 1 << 20 };

  grnxx::Stopwatch stopwatch(true);
  std::uint64_t total = 0;
  for (std::uint32_t i = 1; i <= LOOP_COUNT; ++i) {
    total += grnxx::bit_scan_reverse(i);
  }
  grnxx::Duration elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "bit_scan_reverse<32>: total = " << total
                 << ", elapsed [ns] = "
                 << (1000.0 * elapsed.count() / LOOP_COUNT);

  stopwatch.reset();
  total = 0;
  for (std::uint64_t i = 1; i <= LOOP_COUNT; ++i) {
    total += grnxx::bit_scan_reverse(i << 20);
  }
  elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "bit_scan_reverse<64>: total = " << total
                 << ", elapsed [ns] = "
                 << (1000.0 * elapsed.count() / LOOP_COUNT);

  stopwatch.reset();
  volatile std::uint32_t count_32 = 0;
  for (std::uint32_t i = 0; i < LOOP_COUNT; ++i) {
    assert(grnxx::atomic_fetch_and_add(1, &count_32) == i);
  }
  elapsed = stopwatch.elapsed();
  assert(count_32 == LOOP_COUNT);
  GRNXX_NOTICE() << "atomic_fetch_and_add<32>: total = " << count_32
                 << ", elapsed [ns] = "
                 << (1000.0 * elapsed.count() / LOOP_COUNT);

  stopwatch.reset();
  volatile std::uint64_t count_64 = 0;
  for (std::uint32_t i = 0; i < LOOP_COUNT; ++i) {
    assert(grnxx::atomic_fetch_and_add(1, &count_64) == i);
  }
  elapsed = stopwatch.elapsed();
  assert(count_64 == LOOP_COUNT);
  GRNXX_NOTICE() << "atomic_fetch_and_add<64>: total = " << count_64
                 << ", elapsed [ns] = "
                 << (1000.0 * elapsed.count() / LOOP_COUNT);

  stopwatch.reset();
  const std::int32_t a_32 = 0, b_32 = 1;
  volatile std::int32_t value_32 = a_32;
  for (std::uint32_t i = 0; i < (LOOP_COUNT / 2); ++i) {
    assert(grnxx::atomic_compare_and_swap(a_32, b_32, &value_32));
    assert(grnxx::atomic_compare_and_swap(b_32, a_32, &value_32));
  }
  elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "atomic_compare_and_swap<32>: elapsed [ns] = "
                 << (1000.0 * elapsed.count() / LOOP_COUNT);

  stopwatch.reset();
  const std::int64_t a_64 = 0, b_64 = 1;
  volatile std::int64_t value_64 = a_64;
  for (std::uint32_t i = 0; i < (LOOP_COUNT / 2); ++i) {
    assert(grnxx::atomic_compare_and_swap(a_64, b_64, &value_64));
    assert(grnxx::atomic_compare_and_swap(b_64, a_64, &value_64));
  }
  elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "atomic_compare_and_swap<64>: elapsed [ns] = "
                 << (1000.0 * elapsed.count() / LOOP_COUNT);
}

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_basics();
  test_times();

  return 0;
}
