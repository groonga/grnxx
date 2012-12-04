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
#include <unordered_map>

#include "alpha/blob_vector.hpp"
#include "logger.hpp"
#include "time.hpp"

void test_basics() {
  grnxx::io::Pool::unlink_if_exists("temp.grn");

  grnxx::io::Pool pool("temp.grn", grnxx::io::GRNXX_IO_CREATE);

  grnxx::alpha::BlobVector vector(grnxx::alpha::BLOB_VECTOR_CREATE, pool);

  GRNXX_NOTICE() << "blob_vector = " << vector;

  assert(vector.block_id() == 0);

  grnxx::alpha::BlobVector vector2;

  vector.swap(vector2);
  vector2.swap(vector);

  assert(vector.block_id() == 0);

  std::string values[5];
  values[0].resize(0);
  values[1].resize(1 << 2, 'S');
  values[2].resize(1 << 4, 'M');
  values[3].resize(1 << 12, 'M');
  values[4].resize(1 << 20, 'L');

  vector.set_value(0, values[0].c_str(), values[0].length());
  vector.set_value(1000, values[1].c_str(), values[1].length());
  vector.set_value(1000000, values[2].c_str(), values[2].length());
  vector.set_value(1000000000, values[3].c_str(), values[3].length());
  vector.set_value(1000000000000ULL, values[4].c_str(), values[4].length());

  std::uint64_t length = 0;

  assert(std::memcmp(vector.get_value(0, &length),
                     values[0].c_str(), values[0].length()) == 0);
  assert(length == values[0].length());
  assert(std::memcmp(vector.get_value(1000, &length),
                     values[1].c_str(), values[1].length()) == 0);
  assert(length == values[1].length());
  assert(std::memcmp(vector.get_value(1000000, &length),
                     values[2].c_str(), values[2].length()) == 0);
  assert(length == values[2].length());
  assert(std::memcmp(vector.get_value(1000000000, &length),
                     values[3].c_str(), values[3].length()) == 0);
  assert(length == values[3].length());
  assert(std::memcmp(vector.get_value(1000000000000ULL, &length),
                     values[4].c_str(), values[4].length()) == 0);
  assert(length == values[4].length());

  std::uint32_t block_id = vector.block_id();

  vector = grnxx::alpha::BlobVector();
  pool = grnxx::io::Pool();

  pool = grnxx::io::Pool("temp.grn", grnxx::io::GRNXX_IO_OPEN);
  vector = grnxx::alpha::BlobVector(grnxx::alpha::BLOB_VECTOR_OPEN,
                                    pool, block_id);

  GRNXX_NOTICE() << "blob_vector = " << vector;

  assert(std::memcmp(vector.get_value(0, &length),
                     values[0].c_str(), values[0].length()) == 0);
  assert(length == values[0].length());
  assert(std::memcmp(vector.get_value(1000, &length),
                     values[1].c_str(), values[1].length()) == 0);
  assert(length == values[1].length());
  assert(std::memcmp(vector.get_value(1000000, &length),
                     values[2].c_str(), values[2].length()) == 0);
  assert(length == values[2].length());
  assert(std::memcmp(vector.get_value(1000000000, &length),
                     values[3].c_str(), values[3].length()) == 0);
  assert(length == values[3].length());
  assert(std::memcmp(vector.get_value(1000000000000ULL, &length),
                     values[4].c_str(), values[4].length()) == 0);
  assert(length == values[4].length());

  vector = grnxx::alpha::BlobVector();
  pool = grnxx::io::Pool();

  grnxx::io::Pool::unlink_if_exists("temp.grn");
}

void test_random_values(std::size_t num_values,
                        std::size_t min_value_length,
                        std::size_t max_value_length) {
  std::mt19937 random;

  std::vector<std::string> values(num_values);
  for (std::size_t i = 0; i < values.size(); ++i) {
    const size_t length = min_value_length
        + (random() % (max_value_length - min_value_length + 1));
    values[i].resize(length);
    for (std::uint32_t j = 0; j < length; ++j) {
      values[i][j] = 'A' + (random() % 26);
    }
  }

  grnxx::io::Pool pool("temp.grn", grnxx::io::GRNXX_IO_TEMPORARY);

  grnxx::alpha::BlobVector vector(grnxx::alpha::BLOB_VECTOR_CREATE, pool);

  for (std::size_t i = 0; i < values.size(); ++i) {
    vector.set_value(i, values[i].c_str(), values[i].length());

    std::uint64_t length;
    const char *address =
        static_cast<const char *>(vector.get_value(i, &length));

    assert(length == values[i].length());
    assert(std::memcmp(address, values[i].c_str(), length) == 0);
  }

  for (std::size_t i = 0; i < values.size(); ++i) {
    std::uint64_t length;
    const char *address =
        static_cast<const char *>(vector.get_value(i, &length));

    assert(length == values[i].length());
    assert(std::memcmp(address, values[i].c_str(), length) == 0);
  }

//  GRNXX_NOTICE() << "pool = " << pool;
  GRNXX_NOTICE() << "blob_vector = " << vector;
}

void test_small_values() {
  test_random_values(1 << 20,
                     0,
                     grnxx::alpha::BLOB_VECTOR_SMALL_VALUE_MAX_LENGTH);
}

void test_medium_values() {
  test_random_values(1 << 8,
                     grnxx::alpha::BLOB_VECTOR_MEDIUM_VALUE_MIN_LENGTH,
                     grnxx::alpha::BLOB_VECTOR_MEDIUM_VALUE_MAX_LENGTH);
}

void test_large_values() {
  test_random_values(1 << 6,
                     grnxx::alpha::BLOB_VECTOR_LARGE_VALUE_MIN_LENGTH,
                     grnxx::alpha::BLOB_VECTOR_LARGE_VALUE_MIN_LENGTH * 4);
}

void test_random_updates(grnxx::Duration frozen_duration) {
  std::mt19937 random;

  std::unordered_map<std::uint64_t, std::string> map;

  grnxx::io::PoolOptions options;
  options.set_frozen_duration(frozen_duration);

  grnxx::io::Pool pool("temp.grn", grnxx::io::GRNXX_IO_TEMPORARY, options);

  grnxx::alpha::BlobVector vector(grnxx::alpha::BLOB_VECTOR_CREATE, pool);

  const std::uint8_t  LENGTH_BITS_MIN = 3;
  const std::uint8_t  LENGTH_BITS_MAX = 10;
  const std::uint32_t LOOP_COUNT = 1 << 16;
  const std::uint32_t ID_MASK = (LOOP_COUNT >> 4) - 1;

  std::string query;
  for (std::uint32_t i = 0; i < LOOP_COUNT; ++i) {
    const std::uint64_t id = random() & ID_MASK;

    std::uint64_t length;
    const char *address =
        static_cast<const char *>(vector.get_value(id, &length));

    auto it = map.find(id);
    if (it == map.end()) {
      assert(length == 0);
    } else {
      assert(length == it->second.length());
      assert(std::memcmp(address, it->second.c_str(), length) == 0);
    }

    const size_t query_length_bits = LENGTH_BITS_MIN
        + (random() % (LENGTH_BITS_MAX - LENGTH_BITS_MIN + 1));
    const size_t query_length = random() & ((1 << query_length_bits) - 1);
    query.resize(query_length);
    for (std::uint32_t j = 0; j < query_length; ++j) {
      query[j] = 'A' + (random() % 26);
    }

    vector.set_value(id, query.c_str(), query.length());
    map[id] = query;
  }

  for (auto it = map.begin(); it != map.end(); ++it) {
    const uint64_t key = it->first;
    const std::string &value = it->second;

    std::uint64_t length;
    const char *address =
        static_cast<const char *>(vector.get_value(key, &length));

    assert(length == value.length());
    assert(std::memcmp(address, value.c_str(), length) == 0);
  }

//  GRNXX_NOTICE() << "pool = " << pool;
  GRNXX_NOTICE() << "blob_vector = " << vector;
}

void test_rewrite() {
  test_random_updates(grnxx::Duration::days(1));
}

void test_reuse() {
  test_random_updates(grnxx::Duration(0));
}

void test_random() {
  std::mt19937 random;

  std::vector<std::string> values(1 << 10);
  for (std::size_t i = 0; i < values.size(); ++i) {
    const std::uint32_t length_bits = 4 + (random() % 14);
    const std::uint32_t length = random() & ((1 << length_bits) - 1);
    values[i].resize(length);
    for (std::uint32_t j = 0; j < length; ++j) {
      values[i][j] = 'A' + (random() % 26);
    }
  }

  grnxx::io::Pool pool("temp.grn", grnxx::io::GRNXX_IO_TEMPORARY);

  grnxx::alpha::BlobVector vector(grnxx::alpha::BLOB_VECTOR_CREATE, pool);

  for (std::size_t i = 0; i < values.size(); ++i) {
    vector.set_value(i, values[i].c_str(), values[i].length());

    std::uint64_t length;
    const char *address =
        static_cast<const char *>(vector.get_value(i, &length));

    assert(length == values[i].length());
    assert(std::memcmp(address, values[i].c_str(), length) == 0);
  }

  for (std::size_t i = 0; i < values.size(); ++i) {
    std::uint64_t length;
    const char *address =
        static_cast<const char *>(vector.get_value(i, &length));

    assert(length == values[i].length());
    assert(std::memcmp(address, values[i].c_str(), length) == 0);
  }
}

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_basics();

  test_small_values();
  test_medium_values();
  test_large_values();

  test_rewrite();
  test_reuse();

  test_random();

  return 0;
}
