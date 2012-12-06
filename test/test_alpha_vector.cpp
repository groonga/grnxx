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
#include <cassert>
#include <cstdlib>
#include <random>
#include <vector>

#include "alpha/vector.hpp"
#include "logger.hpp"
#include "time.hpp"

struct Point {
  double x;
  double y;
};

void test_basics() {
  grnxx::io::Pool::unlink_if_exists("temp.grn");

  grnxx::io::Pool pool(grnxx::io::POOL_CREATE, "temp.grn");

  grnxx::alpha::Vector<std::uint32_t> vector(
      grnxx::alpha::VECTOR_CREATE, pool);

  assert(vector.block_id() == 0);
  assert(vector.value_size() == sizeof(std::uint32_t));
  assert(vector.page_size() == grnxx::alpha::VECTOR_DEFAULT_PAGE_SIZE);
  assert(vector.table_size() == grnxx::alpha::VECTOR_DEFAULT_TABLE_SIZE);
  assert(vector.secondary_table_size() ==
         grnxx::alpha::VECTOR_DEFAULT_SECONDARY_TABLE_SIZE);

  GRNXX_NOTICE() << "vector = " << vector;

  grnxx::alpha::Vector<std::uint32_t> vector2;

  vector.swap(vector2);
  vector2.swap(vector);

  assert(vector.block_id() != grnxx::io::BLOCK_INVALID_ID);

  vector[0] = 1;
  vector[1000] = 10;
  vector[1000000] = 100;
  vector[1000000000] = 1000;
  vector[1000000000000ULL] = 10000;
  vector[vector.max_id()] = 100000;

  assert(vector[0] == 1);
  assert(vector[1000] == 10);
  assert(vector[1000000] == 100);
  assert(vector[1000000000] == 1000);
  assert(vector[1000000000000ULL] == 10000);
  assert(vector[vector.max_id()] == 100000);

  const std::uint32_t block_id = vector.block_id();

  vector.close();
  pool.close();

  pool.open(grnxx::io::POOL_OPEN, "temp.grn");

  vector.open(pool, block_id);

  assert(vector[0] == 1);
  assert(vector[1000] == 10);
  assert(vector[1000000] == 100);
  assert(vector[1000000000] == 1000);
  assert(vector[1000000000000ULL] == 10000);
  assert(vector[vector.max_id()] == 100000);

  assert(grnxx::atomic_fetch_and_add(1, &vector[0]) == 1);
  assert(vector[0] == 2);

  assert(grnxx::atomic_fetch_and_add(10, &vector[0]) == 2);
  assert(vector[0] == 12);

  vector.create(pool, 56789);

  assert(vector[0] == 56789);
  assert(vector[1000] == 56789);
  assert(vector[1000000] == 56789);
  assert(vector[1000000000] == 56789);
  assert(vector[1000000000000ULL] == 56789);
  assert(vector[vector.max_id()] == 56789);

  assert(grnxx::atomic_compare_and_swap(
      std::uint32_t(56789), std::uint32_t(98765), &vector[0]));
  assert(!grnxx::atomic_compare_and_swap(
      std::uint32_t(56789), std::uint32_t(98765), &vector[0]));
  assert(grnxx::atomic_compare_and_swap(
      std::uint32_t(98765), std::uint32_t(56789), &vector[0]));
  assert(vector[0] == 56789);

  vector.close();

  grnxx::alpha::Vector<std::uint32_t>::unlink(pool, 0);

  grnxx::alpha::Vector<float> float_vector(grnxx::alpha::VECTOR_CREATE, pool);

  float_vector[0] = 1.0F;
  assert(float_vector[0] == 1.0F);
  float_vector[1 << 30] = 2.0F;
  assert(float_vector[1 << 30] == 2.0F);

  float_vector.close();

  grnxx::alpha::Vector<double> double_vector(
      grnxx::alpha::VECTOR_CREATE, pool);

  double_vector[0] = 1.0;
  assert(double_vector[0] == 1.0);
  double_vector[1 << 30] = 2.0;
  assert(double_vector[1 << 30] == 2.0);

  double_vector.close();

  grnxx::alpha::Vector<Point> point_vector(grnxx::alpha::VECTOR_CREATE, pool);

  point_vector[0].x = 123;
  point_vector[0].y = 456;
  assert(point_vector[0].x == 123);
  assert(point_vector[0].y == 456);

  point_vector[1 << 30].x = 987;
  point_vector[1 << 30].y = 654;
  assert(point_vector[1 << 30].x == 987);
  assert(point_vector[1 << 30].y == 654);

  point_vector.close();

  pool.close();
  grnxx::io::Pool::unlink_if_exists("temp.grn");
}

template <typename T>
void test_times() {
  enum { VECTOR_SIZE = 1 << 20 };

  grnxx::io::Pool pool(grnxx::io::POOL_TEMPORARY, "temp.grn");

  grnxx::alpha::Vector<T> vector(grnxx::alpha::VECTOR_CREATE, pool);

  grnxx::Time start, end;

  std::uint64_t total = 0;

  start = grnxx::Time::now();
  for (std::uint64_t id = 0; id < VECTOR_SIZE; ++id) {
    vector[id] = T(0);
  }
  end = grnxx::Time::now();
  double set_1st_elapsed = 1.0 * (end - start).nanoseconds() / VECTOR_SIZE;

  start = grnxx::Time::now();
  for (std::uint64_t id = 0; id < VECTOR_SIZE; ++id) {
    vector[id] = T(1);
  }
  end = grnxx::Time::now();
  double set_2nd_elapsed = 1.0 * (end - start).nanoseconds() / VECTOR_SIZE;

  start = grnxx::Time::now();
  for (std::uint64_t id = 0; id < VECTOR_SIZE; ++id) {
    total += vector[id];
  }
  end = grnxx::Time::now();
  double get_elapsed = 1.0 * (end - start).nanoseconds() / VECTOR_SIZE;


  start = grnxx::Time::now();
  for (std::uint64_t id = vector.max_id() - VECTOR_SIZE + 1;
       id <= vector.max_id(); ++id) {
    vector[id] = T(0);
  }
  end = grnxx::Time::now();
  double ex_set_1st_elapsed = 1.0 * (end - start).nanoseconds() / VECTOR_SIZE;

  start = grnxx::Time::now();
  for (std::uint64_t id = vector.max_id() - VECTOR_SIZE + 1;
       id <= vector.max_id(); ++id) {
    vector[id] = T(1);
  }
  end = grnxx::Time::now();
  double ex_set_2nd_elapsed = 1.0 * (end - start).nanoseconds() / VECTOR_SIZE;

  start = grnxx::Time::now();
  for (std::uint64_t id = vector.max_id() - VECTOR_SIZE + 1;
       id <= vector.max_id(); ++id) {
    total += vector[id];
  }
  end = grnxx::Time::now();
  double ex_get_elapsed = 1.0 * (end - start).nanoseconds() / VECTOR_SIZE;


  const std::uint64_t boundary = vector.page_size() * vector.table_size();
  const std::uint64_t range = 1 << 16;
  std::uint64_t id_begin = boundary - (range / 2);
  std::uint64_t id_end = boundary + (range / 2);

  for (uint64_t id = id_begin; id < id_end; ++id) {
    vector[id] = T(0);
  }

  std::mt19937 engine;
  std::vector<std::uint64_t> ids(VECTOR_SIZE);
  for (int i = 0; i < VECTOR_SIZE; ++i) {
    ids[i] = id_begin + (engine() % range);
  }

  start = grnxx::Time::now();
  for (int i = 0; i < VECTOR_SIZE; ++i) {
    vector[ids[i]] = T(0);
  }
  end = grnxx::Time::now();
  double boundary_set_1st_elapsed =
      1.0 * (end - start).nanoseconds() / VECTOR_SIZE;

  start = grnxx::Time::now();
  for (int i = 0; i < VECTOR_SIZE; ++i) {
    vector[ids[i]] = T(1);
  }
  end = grnxx::Time::now();
  double boundary_set_2nd_elapsed =
      1.0 * (end - start).nanoseconds() / VECTOR_SIZE;

  start = grnxx::Time::now();
  for (int i = 0; i < VECTOR_SIZE; ++i) {
    total += vector[ids[i]];
  }
  end = grnxx::Time::now();
  double boundary_get_elapsed =
      1.0 * (end - start).nanoseconds() / VECTOR_SIZE;

  const std::uint32_t block_id = vector.block_id();
  vector.close();

  start = grnxx::Time::now();
  grnxx::alpha::Vector<T>::unlink(pool, block_id);
  end = grnxx::Time::now();
  double unlink_elapsed = (end - start).nanoseconds();

  vector.create(pool, 0);

  start = grnxx::Time::now();
  for (std::uint64_t id = 0; id < VECTOR_SIZE; ++id) {
    vector[id] = T(0);
  }
  end = grnxx::Time::now();
  double default_elapsed = 1.0 * (end - start).nanoseconds() / VECTOR_SIZE;


  GRNXX_NOTICE() << "elapsed [ns]: set = " << set_2nd_elapsed << " ("
                                           << set_1st_elapsed << ", "
                                           << default_elapsed << ')'
                 << ", get = " << get_elapsed
                 << ", ex. set = " << ex_set_2nd_elapsed << " ("
                                   << ex_set_1st_elapsed << ')'
                 << ", ex. get = " << ex_get_elapsed
                 << ", boundary set = " << boundary_set_2nd_elapsed << " ("
                                        << boundary_set_1st_elapsed << ')'
                 << ", boundary get = " << boundary_get_elapsed
                 << ", unlink = " << unlink_elapsed
                 << ", total = " << total;
}

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_basics();
  test_times<std::uint8_t>();
  test_times<std::uint16_t>();
  test_times<std::uint32_t>();
  test_times<std::uint64_t>();
  test_times<float>();
  test_times<double>();

  return 0;
}
