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
#include <random>
#include <vector>

#include "grnxx/bit_array.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/storage.hpp"

namespace {

std::mt19937_64 mersenne_twister;

template <std::uint64_t PAGE_SIZE,
          std::uint64_t TABLE_SIZE,
          std::uint64_t SECONDARY_TABLE_SIZE>
void test_bit_array() {
  GRNXX_NOTICE() << __PRETTY_FUNCTION__;

  using BitArray = grnxx::BitArray<PAGE_SIZE, TABLE_SIZE, SECONDARY_TABLE_SIZE>;
  using Unit = typename BitArray::Unit;

  // Generate an array of random units.
  std::vector<Unit> units(BitArray::size() / BitArray::unit_size());
  for (std::size_t i = 0; i < units.size(); ++i) {
    units[i] = mersenne_twister();
  }

  // Create an anonymous Storage.
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  grnxx::BitArray<PAGE_SIZE, TABLE_SIZE, SECONDARY_TABLE_SIZE> array;
  std::uint32_t storage_node_id;

  // Create an Array and test its member functions.
  assert(array.create(storage.get(), grnxx::STORAGE_ROOT_NODE_ID));
  assert(array);
  assert(array.page_size() == PAGE_SIZE);
  assert(array.table_size() == TABLE_SIZE);
  assert(array.secondary_table_size() == SECONDARY_TABLE_SIZE);
  assert(array.size() == (PAGE_SIZE * TABLE_SIZE * SECONDARY_TABLE_SIZE));
  storage_node_id = array.storage_node_id();

  for (std::uint64_t i = 0; i < array.size(); ++i) {
    const bool bit = (units[i / 64] >> (i % 64)) & 1;
    assert(array.set(i, bit));
  }
  for (std::uint64_t i = 0; i < array.size(); ++i) {
    const bool expected_bit = (units[i / 64] >> (i % 64)) & 1;
    bool stored_bit;
    assert(array.get(i, &stored_bit));
    assert(stored_bit == expected_bit);
  }
  for (std::uint64_t i = 0; i < array.size(); ++i) {
    const bool expected_bit = (units[i / 64] >> (i % 64)) & 1;
    assert(array[i] == expected_bit);
  }
  for (std::uint64_t i = 0; i < (array.size() / array.unit_size()); ++i) {
    const Unit * const unit = array.get_unit(i);
    assert(unit);
    assert(*unit == units[i]);
  }
  for (std::uint64_t i = 0; i < (array.size() / array.page_size()); ++i) {
    assert(array.get_page(i));
  }

  // Open the Array.
  assert(array.open(storage.get(), storage_node_id));
  for (std::uint64_t i = 0; i < array.size(); ++i) {
    const bool expected_bit = (units[i / 64] >> (i % 64)) & 1;
    bool stored_bit;
    assert(array.get(i, &stored_bit));
    assert(stored_bit == expected_bit);
  }

  // Create an Array with the default bit.
  assert(array.create(storage.get(), grnxx::STORAGE_ROOT_NODE_ID, false));
  assert(array);
  for (std::uint64_t i = 0; i < array.size(); ++i) {
    assert(!array[i]);
  }
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

  test_bit_array<256,  1,  1>();
  test_bit_array<256, 64,  1>();
  test_bit_array<256, 64, 16>();

  return 0;
}
