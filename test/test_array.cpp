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
#include <memory>

#include "grnxx/array.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/storage.hpp"

namespace {

template <std::uint64_t PAGE_SIZE,
          std::uint64_t TABLE_SIZE,
          std::uint64_t SECONDARY_TABLE_SIZE>
void test_array() {
  GRNXX_NOTICE() << __PRETTY_FUNCTION__;

  // Create an anonymous Storage.
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  grnxx::Array<int, PAGE_SIZE, TABLE_SIZE, SECONDARY_TABLE_SIZE> array;
  uint32_t storage_node_id;

  // Create an Array and test its member functions.
  assert(array.create(storage.get(), grnxx::STORAGE_ROOT_NODE_ID));
  assert(array);
  assert(array.page_size() == PAGE_SIZE);
  assert(array.table_size() == TABLE_SIZE);
  assert(array.secondary_table_size() == SECONDARY_TABLE_SIZE);
  assert(array.size() == (PAGE_SIZE * TABLE_SIZE * SECONDARY_TABLE_SIZE));
  storage_node_id = array.storage_node_id();

  for (std::uint64_t i = 0; i < array.size(); ++i) {
    assert(array.set(i, static_cast<int>(i)));
  }
  for (std::uint64_t i = 0; i < array.size(); ++i) {
    int value;
    assert(array.get(i, &value));
    assert(value == static_cast<int>(i));
  }
  for (std::uint64_t i = 0; i < (array.size() / array.page_size()); ++i) {
    assert(array.get_page(i));
  }

  // Open the Array.
  assert(array.open(storage.get(), storage_node_id));
  for (std::uint64_t i = 0; i < array.size(); ++i) {
    int value;
    assert(array.get(i, &value));
    assert(value == static_cast<int>(i));
  }

  // Create an Array with default value.
  assert(array.create(storage.get(), grnxx::STORAGE_ROOT_NODE_ID, 1));
  assert(array);
  for (std::uint64_t i = 0; i < array.size(); ++i) {
    assert(array[i] == 1);
    array[i] = static_cast<int>(i);
  }
  for (std::uint64_t i = 0; i < array.size(); ++i) {
    assert(array[i] == static_cast<int>(i));
  }
  for (std::uint64_t i = 0; i < array.size(); ++i) {
    assert(&array[i] == array.get_value(i));
  }
}

void test_bit_array() {
  constexpr std::uint64_t PAGE_SIZE            = 64;
  constexpr std::uint64_t TABLE_SIZE           = 32;
  constexpr std::uint64_t SECONDARY_TABLE_SIZE = 16;

  GRNXX_NOTICE() << __PRETTY_FUNCTION__;

  // Create an anonymous Storage.
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  grnxx::Array<bool, PAGE_SIZE, TABLE_SIZE, SECONDARY_TABLE_SIZE> array;
  uint32_t storage_node_id;

  // Create an Array and test its member functions.
  assert(array.create(storage.get(), grnxx::STORAGE_ROOT_NODE_ID));
  assert(array);
  assert(array.page_size() == PAGE_SIZE);
  assert(array.table_size() == TABLE_SIZE);
  assert(array.secondary_table_size() == SECONDARY_TABLE_SIZE);
  assert(array.size() == (PAGE_SIZE * TABLE_SIZE * SECONDARY_TABLE_SIZE));
  storage_node_id = array.storage_node_id();

  for (std::uint64_t i = 0; i < array.size(); ++i) {
    const bool value = (i % 3) != 0;
    assert(array.set(i, value));
  }
  for (std::uint64_t i = 0; i < array.size(); ++i) {
    const bool expected_value = (i % 3) != 0;
    bool value;
    assert(array.get(i, &value));
    assert(value == expected_value);
  }
  for (std::uint64_t i = 0; i < array.size(); ++i) {
    const bool expected_value = (i % 3) != 0;
    assert(array[i] == expected_value);
  }
  for (std::uint64_t i = 0; i < (array.size() / array.unit_size()); ++i) {
    assert(array.get_unit(i));
  }
  for (std::uint64_t i = 0; i < (array.size() / array.page_size()); ++i) {
    assert(array.get_page(i));
  }

  // Open the Array.
  assert(array.open(storage.get(), storage_node_id));
  for (std::uint64_t i = 0; i < array.size(); ++i) {
    const bool expected_value = (i % 3) != 0;
    bool value;
    assert(array.get(i, &value));
    assert(value == expected_value);
  }

  // Create an Array with default value.
  assert(array.create(storage.get(), grnxx::STORAGE_ROOT_NODE_ID, true));
  assert(array);
  for (std::uint64_t i = 0; i < array.size(); ++i) {
    assert(array[i]);
  }
}

}  // namesapce

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_array<64,  1,  1>();
  test_array<64, 32,  1>();
  test_array<64, 32, 16>();
  test_bit_array();

  return 0;
}
