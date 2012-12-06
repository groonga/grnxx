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
#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <random>
#include <vector>

#include "db/array.hpp"
#include "logger.hpp"
#include "time.hpp"

void test_array_1() {
  enum { VECTOR_SIZE = 1 << 24 };

  grnxx::io::File::unlink_if_exists("temp.grn");
  grnxx::io::File::unlink_if_exists("temp_000.grn");
  grnxx::io::File::unlink_if_exists("temp_E000.grn");

  std::mt19937 random;
  std::vector<std::uint32_t> vector(VECTOR_SIZE);
  for (std::size_t i = 0; i < vector.size(); ++i) {
    vector[i] = random();
  }

  grnxx::io::Pool pool(grnxx::io::POOL_CREATE, "temp.grn");

  grnxx::db::Array<std::uint32_t> array;
  array.create(&pool, VECTOR_SIZE);

  const std::uint32_t block_id = array.block_id();

  GRNXX_NOTICE() << "array = " << array;

  assert(array.size() == VECTOR_SIZE);

  for (std::uint64_t i = 0; i < VECTOR_SIZE; ++i) {
    array[i] = vector[i];
  }

  array.close();
  array.open(&pool, block_id);

  for (std::uint64_t i = 0; i < VECTOR_SIZE; ++i) {
    assert(array[i] == vector[i]);
  }

  array.close();

  grnxx::db::Array<std::uint32_t>::unlink(&pool, block_id);
  assert(pool.get_block_info(block_id)->status() ==
         grnxx::io::BLOCK_FROZEN);

  grnxx::io::File::unlink_if_exists("temp.grn");
  grnxx::io::File::unlink_if_exists("temp_000.grn");
  grnxx::io::File::unlink_if_exists("temp_E000.grn");
}

void test_array_2() {
  enum { VECTOR_SIZE = 1 << 24 };

  grnxx::io::Pool pool(grnxx::io::POOL_TEMPORARY, "temp.grn");

  grnxx::db::Array<std::uint8_t[3]> array;
  array.create(&pool, VECTOR_SIZE);

  GRNXX_NOTICE() << "array = " << array;

  assert(array.size() == VECTOR_SIZE);

  for (std::uint64_t i = 0; i < VECTOR_SIZE; ++i) {
    array[i][0] = 'X';
    array[i][1] = 'Y';
    array[i][2] = 'Z';
  }

  for (std::uint64_t i = 0; i < VECTOR_SIZE; ++i) {
    assert(array[i][0] == 'X');
    assert(array[i][1] == 'Y');
    assert(array[i][2] == 'Z');
  }

  array.close();
}

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_array_1();
  test_array_2();

  return 0;
}
