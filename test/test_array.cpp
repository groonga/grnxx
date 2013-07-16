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
#include <cmath>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "grnxx/array.hpp"
//#include "grnxx/bytes.hpp"
#include "grnxx/geo_point.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/periodic_clock.hpp"
#include "grnxx/traits.hpp"
#include "grnxx/storage.hpp"

namespace {

constexpr std::uint64_t MIN_BYTES_SIZE = 0;
constexpr std::uint64_t MAX_BYTES_SIZE = 16;

std::mt19937_64 mersenne_twister;

// Generate a random value.
template <typename T>
T generate_random_value() {
  const std::uint64_t random_value = mersenne_twister();
  const T *value = reinterpret_cast<const T *>(&random_value);
  return *value;
}
template <>
bool generate_random_value<bool>() {
  return bool(mersenne_twister() & 1);
}
// Generate a random floating point number.
template <>
double generate_random_value<double>() {
  for ( ; ; ) {
    const std::uint64_t random_value = mersenne_twister();
    const double *value = reinterpret_cast<const double *>(&random_value);
    if (!std::isnan(*value)) {
      return *value;
    }
  }
}
// TODO: Generate a random value and it is valid until the next call.
//template <>
//grnxx::Bytes generate_random_value<grnxx::Bytes>() {
//  static uint8_t buf[MAX_BYTES_SIZE];
//  const std::uint64_t size = MIN_BYTES_SIZE +
//      (mersenne_twister() % (MAX_BYTES_SIZE - MIN_BYTES_SIZE + 1));
//  for (std::uint64_t i = 0; i < size; ++i) {
//    buf[i] = 'A' + (mersenne_twister() % 26);
//  }
//  return grnxx::Bytes(buf, size);
//}

// Generate random values.
template <typename T>
void generate_random_values(std::uint64_t num_values, std::vector<T> *values) {
  values->clear();
  while (values->size() < num_values) {
    values->push_back(generate_random_value<T>());
  }
}
// TODO: Generate random values and those are valid until the next call.
//template <>
//void generate_random_values(std::uint64_t num_values,
//                            std::vector<grnxx::Bytes> *values) {
//  static std::vector<std::string> buf;
//  while (buf.size() < num_values) {
//    const grnxx::Bytes value = generate_random_value<grnxx::Bytes>();
//    buf.push_back(std::string(reinterpret_cast<const char *>(value.ptr()),
//                              value.size()));
//  }
//  values->clear();
//  for (auto it : buf) {
//    values->push_back(grnxx::Bytes(it.data(), it.size()));
//  }
//}

template <typename T,
          std::uint64_t PAGE_SIZE = 0,
          std::uint64_t TABLE_SIZE = 0>
void test_array(std::uint64_t size) {
  using Array = grnxx::Array<T, PAGE_SIZE, TABLE_SIZE>;

  // Create an anonymous Storage.
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<Array> array;

  // Create an Array and test its member functions.
  array.reset(Array::create(storage.get(), grnxx::STORAGE_ROOT_NODE_ID, size));
  assert(array->size() == size);
  const std::uint32_t storage_node_id = array->storage_node_id();

  // Generate random data.
  std::vector<T> values;
  generate_random_values<T>(size, &values);

  // Set and get values.
  for (std::uint64_t i = 0; i < size; ++i) {
    array->set(i, values[i]);
    assert(array->get(i) == values[i]);
  }
  for (std::uint64_t i = 0; i < size; ++i) {
    assert(array->get(i) == values[i]);
  }

  // Open the Array and get values.
  assert(array->open(storage.get(), storage_node_id));
  for (std::uint64_t i = 0; i < size; ++i) {
    assert(array->get(i) == values[i]);
  }

  // Create an Array with default value.
  array.reset(array->create(storage.get(), grnxx::STORAGE_ROOT_NODE_ID,
                            size, values[0]));
  for (std::uint64_t i = 0; i < size; ++i) {
    assert(array->get(i) == values[0]);
  }

  // Set and get values.
  for (std::uint64_t i = 0; i < size; ++i) {
    array->get_value(i) = values[i];
    assert(array->get_value(i) == values[i]);
  }
  for (std::uint64_t i = 0; i < size; ++i) {
    assert(array->get_value(i) == values[i]);
  }
}

template <std::uint64_t PAGE_SIZE = 0,
          std::uint64_t TABLE_SIZE = 0>
void test_bit_array(std::uint64_t size) {
  using Array = grnxx::Array<bool, PAGE_SIZE, TABLE_SIZE>;
  using Unit = typename Array::Unit;
  constexpr uint64_t UNIT_SIZE = sizeof(Unit) * 8;

  // Create an anonymous Storage.
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<Array> array;

  // Create an Array and test its member functions.
  array.reset(Array::create(storage.get(), grnxx::STORAGE_ROOT_NODE_ID, size));
  assert(array->size() == size);
  const std::uint32_t storage_node_id = array->storage_node_id();

  // Generate random data.
  std::vector<bool> values;
  generate_random_values<bool>(size, &values);

  // Set and get values.
  for (std::uint64_t i = 0; i < size; ++i) {
    array->set(i, values[i]);
    assert(array->get(i) == values[i]);
  }
  for (std::uint64_t i = 0; i < size; ++i) {
    assert(array->get(i) == values[i]);
  }

  // Open the Array and get values.
  assert(array->open(storage.get(), storage_node_id));
  for (std::uint64_t i = 0; i < size; ++i) {
    assert(array->get(i) == values[i]);
  }

  // Create an Array with default value.
  array.reset(array->create(storage.get(), grnxx::STORAGE_ROOT_NODE_ID,
                            size, values[0]));
  for (std::uint64_t i = 0; i < size; ++i) {
    assert(array->get(i) == values[0]);
  }

  // Generate random data.
  std::vector<Unit> units;
  generate_random_values<Unit>(size, &units);

  // Set and get units.
  for (std::uint64_t i = 0; i < (size / UNIT_SIZE); ++i) {
    array->get_unit(i) = units[i];
    assert(array->get_unit(i) == units[i]);
  }
  for (std::uint64_t i = 0; i < (size / UNIT_SIZE); ++i) {
    assert(array->get_unit(i) == units[i]);
    for (std::uint64_t j = 0; j < UNIT_SIZE; ++j) {
      assert(array->get((i * UNIT_SIZE) + j) == ((units[i] >> j) & 1));
    }
  }
}

template <typename T>
void test_array() {
  GRNXX_NOTICE() << __PRETTY_FUNCTION__;

  test_array<T>(256);
  test_array<T, 256>(256 * 64);
  test_array<T, 256, 64>(256 * 64 * 16);
}

template <>
void test_array<bool>() {
  GRNXX_NOTICE() << __PRETTY_FUNCTION__;

  test_bit_array(256);
  test_bit_array<256>(256 * 64);
  test_bit_array<256, 64>(256 * 64 * 16);
}

}  // namesapce

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  // FIXME: Increment the reference count for grnxx::PeriodicClock.
  grnxx::PeriodicClock clock;

  test_array<std::int8_t>();
  test_array<std::uint8_t>();
  test_array<std::int16_t>();
  test_array<std::uint16_t>();
  test_array<std::int32_t>();
  test_array<std::uint32_t>();
  test_array<std::int64_t>();
  test_array<std::uint64_t>();
  test_array<double>();
  test_array<grnxx::GeoPoint>();
  // TODO
//  test_array<grnxx::Bytes>();
  test_array<bool>();

  return 0;
}
