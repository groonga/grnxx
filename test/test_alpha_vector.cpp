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

  grnxx::io::Pool pool("temp.grn", grnxx::io::GRNXX_IO_CREATE);

  grnxx::alpha::Vector<std::uint32_t> vector(
      pool, grnxx::alpha::VECTOR_CREATE);

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

  vector = grnxx::alpha::Vector<std::uint32_t>();
  pool = grnxx::io::Pool();

  pool = grnxx::io::Pool("temp.grn", grnxx::io::GRNXX_IO_OPEN);

  vector = grnxx::alpha::Vector<std::uint32_t>(
      pool, grnxx::alpha::VECTOR_OPEN, block_id);

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

  vector = grnxx::alpha::Vector<std::uint32_t>(
      pool, grnxx::alpha::VECTOR_CREATE, 56789);

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

  vector = grnxx::alpha::Vector<std::uint32_t>();

  grnxx::alpha::Vector<std::uint32_t>::unlink(pool, 0);

  grnxx::alpha::Vector<float> float_vector(pool, grnxx::alpha::VECTOR_CREATE);

  float_vector[0] = 1.0F;
  assert(float_vector[0] == 1.0F);
  float_vector[1 << 30] = 2.0F;
  assert(float_vector[1 << 30] == 2.0F);

  float_vector = grnxx::alpha::Vector<float>();

  grnxx::alpha::Vector<double> double_vector(
      pool, grnxx::alpha::VECTOR_CREATE);

  double_vector[0] = 1.0;
  assert(double_vector[0] == 1.0);
  double_vector[1 << 30] = 2.0;
  assert(double_vector[1 << 30] == 2.0);

  double_vector = grnxx::alpha::Vector<double>();

  grnxx::alpha::Vector<Point> point_vector(pool, grnxx::alpha::VECTOR_CREATE);

  point_vector[0].x = 123;
  point_vector[0].y = 456;
  assert(point_vector[0].x == 123);
  assert(point_vector[0].y == 456);

  point_vector[1 << 30].x = 987;
  point_vector[1 << 30].y = 654;
  assert(point_vector[1 << 30].x == 987);
  assert(point_vector[1 << 30].y == 654);

  point_vector = grnxx::alpha::Vector<Point>();

  pool = grnxx::io::Pool();
  grnxx::io::Pool::unlink_if_exists("temp.grn");
}

template <typename T>
void test_times() {
  enum { VECTOR_SIZE = 1 << 20 };

  grnxx::io::Pool pool("temp.grn", grnxx::io::GRNXX_IO_TEMPORARY);

  grnxx::alpha::Vector<T> vector(pool, grnxx::alpha::VECTOR_CREATE);

  grnxx::Time start, end;

  std::uint64_t total = 0;

  start = grnxx::Time::now();
  for (std::uint64_t id = 0; id < VECTOR_SIZE; ++id) {
    vector[id] = T(0);
  }
  end = grnxx::Time::now();
  grnxx::Duration set_1st_elapsed = (end - start) / VECTOR_SIZE;

  start = grnxx::Time::now();
  for (std::uint64_t id = 0; id < VECTOR_SIZE; ++id) {
    vector[id] = T(1);
  }
  end = grnxx::Time::now();
  grnxx::Duration set_2nd_elapsed = (end - start) / VECTOR_SIZE;

  start = grnxx::Time::now();
  for (std::uint64_t id = 0; id < VECTOR_SIZE; ++id) {
    total += vector[id];
  }
  end = grnxx::Time::now();
  grnxx::Duration get_elapsed = (end - start) / VECTOR_SIZE;


  start = grnxx::Time::now();
  for (std::uint64_t id = vector.max_id() - VECTOR_SIZE + 1;
       id <= vector.max_id(); ++id) {
    vector[id] = T(0);
  }
  end = grnxx::Time::now();
  grnxx::Duration ex_set_1st_elapsed = (end - start) / VECTOR_SIZE;

  start = grnxx::Time::now();
  for (std::uint64_t id = vector.max_id() - VECTOR_SIZE + 1;
       id <= vector.max_id(); ++id) {
    vector[id] = T(1);
  }
  end = grnxx::Time::now();
  grnxx::Duration ex_set_2nd_elapsed = (end - start) / VECTOR_SIZE;

  start = grnxx::Time::now();
  for (std::uint64_t id = vector.max_id() - VECTOR_SIZE + 1;
       id <= vector.max_id(); ++id) {
    total += vector[id];
  }
  end = grnxx::Time::now();
  grnxx::Duration ex_get_elapsed = (end - start) / VECTOR_SIZE;


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
  grnxx::Duration boundary_set_1st_elapsed = (end - start) / VECTOR_SIZE;

  start = grnxx::Time::now();
  for (int i = 0; i < VECTOR_SIZE; ++i) {
    vector[ids[i]] = T(1);
  }
  end = grnxx::Time::now();
  grnxx::Duration boundary_set_2nd_elapsed = (end - start) / VECTOR_SIZE;

  start = grnxx::Time::now();
  for (int i = 0; i < VECTOR_SIZE; ++i) {
    total += vector[ids[i]];
  }
  end = grnxx::Time::now();
  grnxx::Duration boundary_get_elapsed = (end - start) / VECTOR_SIZE;

  const std::uint32_t block_id = vector.block_id();
  vector = grnxx::alpha::Vector<T>();

  start = grnxx::Time::now();
  grnxx::alpha::Vector<T>::unlink(pool, block_id);
  end = grnxx::Time::now();
  grnxx::Duration unlink_elapsed = end - start;

  vector = grnxx::alpha::Vector<T>(pool, grnxx::alpha::VECTOR_CREATE, 0);

  start = grnxx::Time::now();
  for (std::uint64_t id = 0; id < VECTOR_SIZE; ++id) {
    vector[id] = T(0);
  }
  end = grnxx::Time::now();
  grnxx::Duration default_elapsed = (end - start) / VECTOR_SIZE;


  GRNXX_NOTICE() << "elapsed [ns]: set = "
                 << set_2nd_elapsed.nanoseconds() << " ("
                 << set_1st_elapsed.nanoseconds() << ", "
                 << default_elapsed.nanoseconds() << ')'
                 << ", get = "
                 << get_elapsed.nanoseconds()
                 << ", ex. set = "
                 << ex_set_2nd_elapsed.nanoseconds() << " ("
                 << ex_set_1st_elapsed.nanoseconds() << ')'
                 << ", ex. get = "
                 << ex_get_elapsed.nanoseconds()
                 << ", boundary set = "
                 << boundary_set_2nd_elapsed.nanoseconds() << " ("
                 << boundary_set_1st_elapsed.nanoseconds() << ')'
                 << ", boundary get = "
                 << boundary_get_elapsed.nanoseconds()
                 << ", unlink = "
                 << unlink_elapsed.nanoseconds()
                 << ", total = "
                 << total;
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
