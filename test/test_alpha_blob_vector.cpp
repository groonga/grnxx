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
#include <random>
#include <vector>

#include "alpha/blob_vector.hpp"
#include "logger.hpp"
#include "time.hpp"

void test_basics() {
  grnxx::io::Pool::unlink_if_exists("temp.grn");

  grnxx::io::Pool pool("temp.grn", grnxx::io::GRNXX_IO_CREATE);

  grnxx::alpha::BlobVector vector(grnxx::alpha::BLOB_VECTOR_CREATE, pool);

  GRNXX_NOTICE() << "blob_vector = " << vector;

  assert(vector.block_id() == 0);
  assert(!vector.get_value(0));

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

  vector.set_value(0, nullptr, 0);
  assert(!vector.get_value(0));

  vector = grnxx::alpha::BlobVector();
  pool = grnxx::io::Pool();

  grnxx::io::Pool::unlink_if_exists("temp.grn");
}

void test_sequential_access(int num_loops,
                            std::size_t num_values,
                            std::size_t min_value_length,
                            std::size_t max_value_length) {
  std::mt19937 random;

  grnxx::io::PoolOptions options;
  options.set_frozen_duration(grnxx::Duration(0));
  grnxx::io::Pool pool("temp.grn", grnxx::io::GRNXX_IO_TEMPORARY, options);
  grnxx::alpha::BlobVector vector(grnxx::alpha::BLOB_VECTOR_CREATE, pool);

  for (int loop_id = 0; loop_id < num_loops; ++loop_id) {
    std::vector<std::string> values(num_values);
    for (std::size_t i = 0; i < values.size(); ++i) {
      const size_t length = min_value_length
          + (random() % (max_value_length - min_value_length + 1));
      values[i].resize(length, '0' + (random() % 10));
      if (length != 0) {
        values[i][0] = 'a' + (random() % 26);
        values[i][length - 1] = 'A' + (random() % 26);
      }
    }

    for (std::size_t i = 0; i < values.size(); ++i) {
      vector.set_value(i, values[i].c_str(), values[i].length());
      std::uint64_t length;
      const char *address =
          static_cast<const char *>(vector.get_value(i, &length));
      assert(address);
      assert(length == values[i].length());
      assert(std::memcmp(address, values[i].c_str(), length) == 0);
    }

    for (std::size_t i = 0; i < values.size(); ++i) {
      std::uint64_t length;
      const char *address =
          static_cast<const char *>(vector.get_value(i, &length));
      assert(address);
      assert(length == values[i].length());
      assert(std::memcmp(address, values[i].c_str(), length) == 0);
    }

    GRNXX_NOTICE() << "total_size = " << pool.header().total_size();
  }
}

void test_random_access(int num_loops,
                        std::size_t num_values,
                        std::size_t min_value_length,
                        std::size_t max_value_length) {
  std::mt19937 random;

  grnxx::io::PoolOptions options;
  options.set_frozen_duration(grnxx::Duration(0));
  grnxx::io::Pool pool("temp.grn", grnxx::io::GRNXX_IO_TEMPORARY, options);
  grnxx::alpha::BlobVector vector(grnxx::alpha::BLOB_VECTOR_CREATE, pool);

  std::vector<std::uint32_t> ids(num_values);
  for (std::size_t i = 0; i < ids.size(); ++i) {
    ids[i] = static_cast<std::uint32_t>(i);
  }

  for (int loop_id = 0; loop_id < num_loops; ++loop_id) {
    std::random_shuffle(ids.begin(), ids.end());

    std::vector<std::string> values(num_values);
    for (std::size_t i = 0; i < values.size(); ++i) {
      const size_t length = min_value_length
          + (random() % (max_value_length - min_value_length + 1));
      values[i].resize(length);
      for (std::size_t j = 0; j < length; ++j) {
        values[i][j] = 'A' + (random() % 26);
      }
    }

    for (std::size_t i = 0; i < values.size(); ++i) {
      vector.set_value(ids[i], values[i].c_str(), values[i].length());
      std::uint64_t length;
      const char *address =
          static_cast<const char *>(vector.get_value(ids[i], &length));
      assert(address);
      assert(length == values[i].length());
      assert(std::memcmp(address, values[i].c_str(), length) == 0);
    }

    for (std::size_t i = 0; i < values.size(); ++i) {
      std::uint64_t length;
      const char *address =
          static_cast<const char *>(vector.get_value(ids[i], &length));
      assert(address);
      assert(length == values[i].length());
      assert(std::memcmp(address, values[i].c_str(), length) == 0);
    }

    GRNXX_NOTICE() << "total_size = " << pool.header().total_size();
  }
}

void test_access_patterns(int num_loops,
                          std::size_t num_values,
                          std::size_t min_value_length,
                          std::size_t max_value_length) {
  GRNXX_NOTICE() << "num_loops = " << num_loops
                 << ", num_values = " << num_values
                 << ", min_value_length = " << min_value_length
                 << ", max_value_length = " << max_value_length;

  test_sequential_access(num_loops, num_values,
                         min_value_length, max_value_length);
  test_random_access(num_loops, num_values,
                     min_value_length, max_value_length);
}

void test_small_values() {
  test_access_patterns(3, 1 << 17,
                       0,
                       grnxx::alpha::BLOB_VECTOR_SMALL_VALUE_MAX_LENGTH);
}

void test_medium_values() {
  test_access_patterns(3, 1 << 14,
                       grnxx::alpha::BLOB_VECTOR_MEDIUM_VALUE_MIN_LENGTH,
                       1 << 10);
  test_access_patterns(3, 1 << 8,
                       1 << 10,
                       grnxx::alpha::BLOB_VECTOR_MEDIUM_VALUE_MAX_LENGTH);
}

void test_large_values() {
  test_access_patterns(3, 1 << 6,
                       grnxx::alpha::BLOB_VECTOR_LARGE_VALUE_MIN_LENGTH,
                       grnxx::alpha::BLOB_VECTOR_LARGE_VALUE_MIN_LENGTH * 2);
}

void test_reuse(bool enable_reuse) {
  const uint32_t NUM_LOOPS = 3;
  const uint32_t NUM_VALUES = 1 << 14;
  const uint32_t MAX_LENGTH = 1024;

  GRNXX_NOTICE() << "enable_reuse = " << enable_reuse;

  std::mt19937 random;

  grnxx::io::PoolOptions options;
  options.set_frozen_duration(
      enable_reuse ? grnxx::Duration(0) : grnxx::Duration::days(1));
  grnxx::io::Pool pool("temp.grn", grnxx::io::GRNXX_IO_TEMPORARY, options);
  grnxx::alpha::BlobVector vector(grnxx::alpha::BLOB_VECTOR_CREATE, pool);

  std::string value(MAX_LENGTH, 'X');

  for (uint32_t loop_id = 0; loop_id < NUM_LOOPS; ++loop_id) {
    for (uint32_t i = 0; i < NUM_VALUES; ++i) {
      vector.set_value(0, &value[0], random() % MAX_LENGTH);
    }
    GRNXX_NOTICE() << "total_size = " << pool.header().total_size();
  }
}

void test_mixed() {
  const uint32_t NUM_LOOPS = 3;
  const uint32_t NUM_VALUES = 1 << 11;
  const uint32_t VECTOR_SIZE = 1 << 10;

  std::mt19937 random;

  grnxx::io::Pool pool("temp.grn", grnxx::io::GRNXX_IO_TEMPORARY);
  grnxx::alpha::BlobVector vector(grnxx::alpha::BLOB_VECTOR_CREATE, pool);

  std::string value(grnxx::alpha::BLOB_VECTOR_LARGE_VALUE_MIN_LENGTH, 'X');

  for (uint32_t loop_id = 0; loop_id < NUM_LOOPS; ++loop_id) {
    for (uint32_t i = 0; i < NUM_VALUES; ++i) {
      const uint32_t value_id = random() % VECTOR_SIZE;
      switch (random() & 3) {
        case 0: {
          vector.set_value(value_id, nullptr, 0);
          break;
        }
        case 1: {
          const uint32_t value_length =
              random() % (grnxx::alpha::BLOB_VECTOR_SMALL_VALUE_MAX_LENGTH + 1);
          vector.set_value(value_id, &value[0], value_length);
          break;
        }
        case 2: {
          const uint32_t value_length_range =
              grnxx::alpha::BLOB_VECTOR_MEDIUM_VALUE_MAX_LENGTH
              - grnxx::alpha::BLOB_VECTOR_MEDIUM_VALUE_MIN_LENGTH + 1;
          const uint32_t value_length = (random() % value_length_range)
              + grnxx::alpha::BLOB_VECTOR_MEDIUM_VALUE_MIN_LENGTH;
          vector.set_value(value_id, &value[0], value_length);
          break;
        }
        case 3: {
          vector.set_value(value_id, &value[0], value.length());
          break;
        }
      }
    }
    GRNXX_NOTICE() << "total_size = " << pool.header().total_size();
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

  test_reuse(false);
  test_reuse(true);

  test_mixed();

  return 0;
}
