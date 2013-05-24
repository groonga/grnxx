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

#include "grnxx/geo_point.hpp"
#include "grnxx/logger.hpp"

namespace {

void test_latitude() {
  grnxx::GeoPoint point(123, 456);
  assert(point.latitude() == 123);
}

void test_longitude() {
  grnxx::GeoPoint point(123, 456);
  assert(point.longitude() == 456);
}

void test_value() {
  grnxx::GeoPoint point(123, 456);
  const std::int32_t x[2] = { 123, 456 };
  assert(point.value() == *reinterpret_cast<const std::uint64_t *>(x));
}

void test_set_latitude() {
  grnxx::GeoPoint point(123, 456);
  point.set_latitude(789);
  assert(point.latitude() == 789);
  assert(point.longitude() == 456);
}

void test_set_longitude() {
  grnxx::GeoPoint point(123, 456);
  point.set_longitude(789);
  assert(point.latitude() == 123);
  assert(point.longitude() == 789);
}

void test_set_value() {
  grnxx::GeoPoint point(123, 456);
  const std::int32_t x[2] = { 987, 654 };
  point.set_value(*reinterpret_cast<const std::uint64_t *>(x));
  assert(point.latitude() == 987);
  assert(point.longitude() == 654);
}

void test_interleave() {
  grnxx::GeoPoint point_1(123, 456);
  grnxx::GeoPoint point_2(456, 789);
  assert(point_1.interleave() < point_2.interleave());
}

}  // namespace

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_latitude();
  test_longitude();
  test_value();
  test_set_latitude();
  test_set_longitude();
  test_set_value();
  test_interleave();

  return 0;
}
