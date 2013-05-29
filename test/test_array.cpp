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
          std::uint64_t PAGE_SIZE,
          std::uint64_t TABLE_SIZE,
          std::uint64_t SECONDARY_TABLE_SIZE>
void test_array() {
  using Array = grnxx::Array<T, PAGE_SIZE, TABLE_SIZE, SECONDARY_TABLE_SIZE>;

  // Create an anonymous Storage.
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<Array> array;

  // Create an Array and test its member functions.
  array.reset(array->create(storage.get(), grnxx::STORAGE_ROOT_NODE_ID));
  assert(array);
  assert(array->page_size() == PAGE_SIZE);
  assert(array->table_size() == TABLE_SIZE);
  assert(array->secondary_table_size() == SECONDARY_TABLE_SIZE);
  assert(array->size() == (PAGE_SIZE * TABLE_SIZE * SECONDARY_TABLE_SIZE));
  const std::uint32_t storage_node_id = array->storage_node_id();

  // Generate random data.
  std::vector<T> values;
  generate_random_values<T>(array->size(), &values);

  // Set and get values.
  for (std::uint64_t i = 0; i < array->size(); ++i) {
    assert(array->set(i, values[i]));
  }
  for (std::uint64_t i = 0; i < array->size(); ++i) {
    T value;
    assert(array->get(i, &value));
    assert(value == values[i]);
  }
  for (std::uint64_t i = 0; i < (array->size() / array->page_size()); ++i) {
    assert(array->get_page(i));
  }

  // Open the Array and get values.
  assert(array->open(storage.get(), storage_node_id));
  for (std::uint64_t i = 0; i < array->size(); ++i) {
    T value;
    assert(array->get(i, &value));
    assert(value == values[i]);
  }

  // Create an Array with default value.
  array.reset(array->create(storage.get(), grnxx::STORAGE_ROOT_NODE_ID,
                            values[0]));
  assert(array);
  for (std::uint64_t i = 0; i < array->size(); ++i) {
    T value;
    assert(array->get(i, &value));
    assert(value == values[0]);

    T * const pointer = array->get_pointer(i);
    assert(pointer);
    assert(*pointer == values[0]);
  }
}

template <std::uint64_t PAGE_SIZE,
          std::uint64_t TABLE_SIZE,
          std::uint64_t SECONDARY_TABLE_SIZE>
void test_bit_array() {
  using Array = grnxx::Array<bool, PAGE_SIZE, TABLE_SIZE, SECONDARY_TABLE_SIZE>;
  using Unit = typename Array::Unit;

  // Create an anonymous Storage.
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<Array> array;

  // Create an Array and test its member functions.
  array.reset(array->create(storage.get(), grnxx::STORAGE_ROOT_NODE_ID));
  assert(array);
  assert(array->unit_size() == (sizeof(Unit) * 8));
  assert(array->page_size() == PAGE_SIZE);
  assert(array->table_size() == TABLE_SIZE);
  assert(array->secondary_table_size() == SECONDARY_TABLE_SIZE);
  assert(array->size() == (PAGE_SIZE * TABLE_SIZE * SECONDARY_TABLE_SIZE));
  const std::uint32_t storage_node_id = array->storage_node_id();

  // Generate an array of random units.
  std::vector<Unit> units;
  generate_random_values(array->size() / array->unit_size(), &units);

  // Set and get values.
  for (std::uint64_t i = 0; i < array->size(); ++i) {
    const bool bit = (units[i / 64] >> (i % 64)) & 1;
    assert(array->set(i, bit));
  }
  for (std::uint64_t i = 0; i < array->size(); ++i) {
    const bool expected_bit = (units[i / 64] >> (i % 64)) & 1;
    bool stored_bit;
    assert(array->get(i, &stored_bit));
    assert(stored_bit == expected_bit);
  }
  for (std::uint64_t i = 0; i < (array->size() / array->unit_size()); ++i) {
    const Unit * const unit = array->get_unit(i);
    assert(unit);
    assert(*unit == units[i]);
  }
  for (std::uint64_t i = 0; i < (array->size() / array->page_size()); ++i) {
    assert(array->get_page(i));
  }

  // Open the Array and get values.
  array.reset(array->open(storage.get(), storage_node_id));
  assert(array);
  for (std::uint64_t i = 0; i < array->size(); ++i) {
    const bool expected_bit = (units[i / 64] >> (i % 64)) & 1;
    bool stored_bit;
    assert(array->get(i, &stored_bit));
    assert(stored_bit == expected_bit);
  }

  // Create Arrays with default value.
  array.reset(array->create(storage.get(), grnxx::STORAGE_ROOT_NODE_ID, false));
  assert(array);
  for (std::uint64_t i = 0; i < array->size(); ++i) {
    bool bit;
    assert(array->get(i, &bit));
    assert(!bit);
  }
  array.reset(array->create(storage.get(), grnxx::STORAGE_ROOT_NODE_ID, true));
  assert(array);
  for (std::uint64_t i = 0; i < array->size(); ++i) {
    bool bit;
    assert(array->get(i, &bit));
    assert(bit);
  }
}

template <typename T>
void test_array() {
  GRNXX_NOTICE() << __PRETTY_FUNCTION__;

  test_array<T, 256,  1,  1>();
  test_array<T, 256, 64,  1>();
  test_array<T, 256, 64, 16>();
}

template <>
void test_array<bool>() {
  GRNXX_NOTICE() << __PRETTY_FUNCTION__;

  test_bit_array<256,  1,  1>();
  test_bit_array<256, 64,  1>();
  test_bit_array<256, 64, 16>();
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
