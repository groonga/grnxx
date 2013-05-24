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
#include <algorithm>
#include <cassert>
#include <random>
#include <vector>

#include "grnxx/db/blob_vector.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/stopwatch.hpp"

void test_basics() {
  grnxx::io::Pool::unlink_if_exists("temp.grn");

  grnxx::io::Pool pool(grnxx::io::POOL_CREATE, "temp.grn");

  grnxx::db::BlobVector vector(grnxx::db::BLOB_VECTOR_CREATE, pool);

  GRNXX_NOTICE() << "blob_vector = " << vector;

  assert(vector.block_id() == 0);
  assert(!vector.get_value(0));

  grnxx::db::BlobVector vector2;

  vector.swap(vector2);
  vector2.swap(vector);

  assert(vector.block_id() == 0);

  std::string values[5];
  values[0].resize(0);
  values[1].resize(1 << 2, 'S');
  values[2].resize(1 << 4, 'M');
  values[3].resize(1 << 12, 'M');
  values[4].resize(1 << 20, 'L');

  vector[0].set(values[0].c_str(), values[0].length());
  vector[1000].set(values[1].c_str(), values[1].length());
  vector[1000000].set(values[2].c_str(), values[2].length());
  vector[1000000000].set(values[3].c_str(), values[3].length());
  vector[1000000000000ULL].set(values[4].c_str(), values[4].length());

  grnxx::db::Blob blob;

  assert(blob = vector[0]);
  assert(blob.length() == values[0].length());
  assert(std::memcmp(blob.address(), values[0].c_str(), blob.length()) == 0);
  assert(blob = vector[1000]);
  assert(blob.length() == values[1].length());
  assert(std::memcmp(blob.address(), values[1].c_str(), blob.length()) == 0);
  assert(blob = vector[1000000]);
  assert(blob.length() == values[2].length());
  assert(std::memcmp(blob.address(), values[2].c_str(), blob.length()) == 0);
  assert(blob = vector[1000000000]);
  assert(blob.length() == values[3].length());
  assert(std::memcmp(blob.address(), values[3].c_str(), blob.length()) == 0);
  assert(blob = vector[1000000000000ULL]);
  assert(blob.length() == values[4].length());
  assert(std::memcmp(blob.address(), values[4].c_str(), blob.length()) == 0);

  std::uint32_t block_id = vector.block_id();

  vector.close();
  pool.close();

  pool.open(grnxx::io::POOL_OPEN, "temp.grn");
  vector.open(pool, block_id);


  GRNXX_NOTICE() << "blob_vector = " << vector;

  assert(blob = vector[0]);
  assert(blob.length() == values[0].length());
  assert(std::memcmp(blob.address(), values[0].c_str(), blob.length()) == 0);
  assert(blob = vector[1000]);
  assert(blob.length() == values[1].length());
  assert(std::memcmp(blob.address(), values[1].c_str(), blob.length()) == 0);
  assert(blob = vector[1000000]);
  assert(blob.length() == values[2].length());
  assert(std::memcmp(blob.address(), values[2].c_str(), blob.length()) == 0);
  assert(blob = vector[1000000000]);
  assert(blob.length() == values[3].length());
  assert(std::memcmp(blob.address(), values[3].c_str(), blob.length()) == 0);
  assert(blob = vector[1000000000000ULL]);
  assert(blob.length() == values[4].length());
  assert(std::memcmp(blob.address(), values[4].c_str(), blob.length()) == 0);

  vector[0] = grnxx::db::Blob("banana", 6);
  blob = vector[0];
  assert(blob);
  assert(blob.length() == 6);
  assert(std::memcmp(blob.address(), "banana", 6) == 0);

  vector[0] = grnxx::db::Blob("xyz", 3);
  assert(std::memcmp(blob.address(), "banana", 6) == 0);

  grnxx::db::Blob blob2 = blob;
  blob = nullptr;
  assert(blob2);
  assert(blob2.length() == 6);
  assert(std::memcmp(blob2.address(), "banana", 6) == 0);

  vector[0] = nullptr;
  blob = vector[0];
  assert(!blob);

  vector[0].append("ABC", 3);
  blob = vector[0];
  assert(blob.length() == 3);
  assert(std::memcmp(blob.address(), "ABC", 3) == 0);

  vector[0].append("XYZ", 3);
  blob = vector[0];
  assert(blob.length() == 6);
  assert(std::memcmp(blob.address(), "ABCXYZ", 6) == 0);

  vector[0].prepend("123", 3);
  blob = vector[0];
  assert(blob.length() == 9);
  assert(std::memcmp(blob.address(), "123ABCXYZ", 9) == 0);

  vector.close();
  pool.close();

  grnxx::io::Pool::unlink_if_exists("temp.grn");
}

void test_sequential_access(int num_loops,
                            std::size_t num_values,
                            std::size_t min_value_length,
                            std::size_t max_value_length) {
  std::mt19937 random;

  grnxx::io::PoolOptions options;
  options.set_frozen_duration(grnxx::Duration(0));
  grnxx::io::Pool pool(grnxx::io::POOL_TEMPORARY, "temp.grn", options);
  grnxx::db::BlobVector vector(grnxx::db::BLOB_VECTOR_CREATE, pool);

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
      vector[i].set(values[i].c_str(), values[i].length());
      grnxx::db::Blob blob = vector[i];
      assert(blob);
      assert(blob.length() == values[i].length());
      assert(std::memcmp(blob.address(), values[i].c_str(),
                         blob.length()) == 0);
    }

    for (std::size_t i = 0; i < values.size(); ++i) {
      grnxx::db::Blob blob = vector[i];
      assert(blob);
      assert(blob.length() == values[i].length());
      assert(std::memcmp(blob.address(), values[i].c_str(),
                         blob.length()) == 0);
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
  grnxx::io::Pool pool(grnxx::io::POOL_TEMPORARY, "temp.grn", options);
  grnxx::db::BlobVector vector(grnxx::db::BLOB_VECTOR_CREATE, pool);

  std::vector<std::uint32_t> ids(num_values);
  for (std::size_t i = 0; i < ids.size(); ++i) {
    ids[i] = static_cast<std::uint32_t>(i);
  }

  for (int loop_id = 0; loop_id < num_loops; ++loop_id) {
    std::random_shuffle(ids.begin(), ids.end(),
        [&random](std::uint32_t limit) { return random() % limit; });

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
      vector[ids[i]].set(values[i].c_str(), values[i].length());
      grnxx::db::Blob blob = vector[ids[i]];
      assert(blob);
      assert(blob.length() == values[i].length());
      assert(std::memcmp(blob.address(), values[i].c_str(),
                         blob.length()) == 0);
    }

    for (std::size_t i = 0; i < values.size(); ++i) {
      grnxx::db::Blob blob = vector[ids[i]];
      assert(blob);
      assert(blob.length() == values[i].length());
      assert(std::memcmp(blob.address(), values[i].c_str(),
                         blob.length()) == 0);
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
                       grnxx::db::BLOB_VECTOR_SMALL_VALUE_MAX_LENGTH);
}

void test_medium_values() {
  test_access_patterns(3, 1 << 14,
                       grnxx::db::BLOB_VECTOR_MEDIUM_VALUE_MIN_LENGTH,
                       1 << 10);
  test_access_patterns(3, 1 << 8,
                       1 << 10,
                       grnxx::db::BLOB_VECTOR_MEDIUM_VALUE_MAX_LENGTH);
}

void test_large_values() {
  test_access_patterns(3, 1 << 6,
                       grnxx::db::BLOB_VECTOR_LARGE_VALUE_MIN_LENGTH,
                       grnxx::db::BLOB_VECTOR_LARGE_VALUE_MIN_LENGTH * 2);
}

void test_reuse(bool enable_reuse) {
  const std::uint32_t NUM_LOOPS = 3;
  const std::uint32_t NUM_VALUES = 1 << 14;
  const std::uint32_t MAX_LENGTH = 1024;

  GRNXX_NOTICE() << "enable_reuse = " << enable_reuse;

  std::mt19937 random;

  grnxx::io::PoolOptions options;
  options.set_frozen_duration(
      enable_reuse ? grnxx::Duration(0) : grnxx::Duration::days(1));
  grnxx::io::Pool pool(grnxx::io::POOL_TEMPORARY, "temp.grn", options);
  grnxx::db::BlobVector vector(grnxx::db::BLOB_VECTOR_CREATE, pool);

  std::string value(MAX_LENGTH, 'X');

  for (std::uint32_t loop_id = 0; loop_id < NUM_LOOPS; ++loop_id) {
    for (std::uint32_t i = 0; i < NUM_VALUES; ++i) {
      vector[0] = grnxx::db::Blob(&value[0], random() % MAX_LENGTH);
    }
    GRNXX_NOTICE() << "total_size = " << pool.header().total_size();
  }
}

void test_mixed() {
  const std::uint32_t NUM_LOOPS = 3;
  const std::uint32_t NUM_VALUES = 1 << 11;
  const std::uint32_t VECTOR_SIZE = 1 << 10;

  std::mt19937 random;

  grnxx::io::Pool pool(grnxx::io::POOL_TEMPORARY, "temp.grn");
  grnxx::db::BlobVector vector(grnxx::db::BLOB_VECTOR_CREATE, pool);

  std::string value(grnxx::db::BLOB_VECTOR_LARGE_VALUE_MIN_LENGTH, 'X');

  for (std::uint32_t loop_id = 0; loop_id < NUM_LOOPS; ++loop_id) {
    for (std::uint32_t i = 0; i < NUM_VALUES; ++i) {
      const std::uint32_t value_id = random() % VECTOR_SIZE;
      switch (random() % 5) {
        case 0: {
          vector[value_id] = nullptr;
          break;
        }
        case 1: {
          const std::uint32_t value_length =
              random() % (grnxx::db::BLOB_VECTOR_SMALL_VALUE_MAX_LENGTH + 1);
          vector[value_id] = grnxx::db::Blob(&value[0], value_length);
          break;
        }
        case 2: {
          const std::uint32_t value_length_range =
              grnxx::db::BLOB_VECTOR_MEDIUM_VALUE_MAX_LENGTH
              - grnxx::db::BLOB_VECTOR_MEDIUM_VALUE_MIN_LENGTH + 1;
          const std::uint32_t value_length = (random() % value_length_range)
              + grnxx::db::BLOB_VECTOR_MEDIUM_VALUE_MIN_LENGTH;
          vector[value_id] = grnxx::db::Blob(&value[0], value_length);
          break;
        }
        case 3: {
          vector[value_id] = grnxx::db::Blob(&value[0], value.length());
          break;
        }
        case 4: {
          const std::uint32_t block_id = vector.block_id();
          vector.close();
          vector.open(pool, block_id);
          break;
        }
      }
    }
    GRNXX_NOTICE() << "total_size = " << pool.header().total_size();
  }
}

void test_defrag() {
  const std::uint32_t NUM_VALUES = 1 << 18;
  const std::uint32_t MAX_LENGTH = 1 << 6;

  std::mt19937 random;

  grnxx::io::Pool pool(grnxx::io::POOL_TEMPORARY, "temp.grn");
  grnxx::db::BlobVector vector(grnxx::db::BLOB_VECTOR_CREATE, pool);

  std::vector<std::uint32_t> ids(NUM_VALUES);
  std::random_shuffle(ids.begin(), ids.end(), [&random](std::uint32_t limit) {
    return random() % limit;
  });

  std::string value(grnxx::db::BLOB_VECTOR_LARGE_VALUE_MIN_LENGTH, 'X');
  for (std::uint32_t i = 0; i < NUM_VALUES; ++i) {
    const std::uint32_t length = random() % MAX_LENGTH;
    vector[ids[i]].set(&value[0], length);
  }

  grnxx::Stopwatch stopwatch(true);
  for (std::uint32_t i = 0; i < NUM_VALUES; ++i) {
    grnxx::db::Blob blob = vector[i];
    if (blob.length() > 0) {
      assert(*static_cast<const char *>(blob.address()) == 'X');
    };
  }
  grnxx::Duration elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "before defrag: elapsed [ns] = "
                 << (elapsed.count() * 1000 / NUM_VALUES);

  vector.defrag();

  stopwatch.reset();
  for (std::uint32_t i = 0; i < NUM_VALUES; ++i) {
    grnxx::db::Blob blob = vector[i];
    if (blob.length() > 0) {
      assert(*static_cast<const char *>(blob.address()) == 'X');
    };
  }
  elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "after defrag: elapsed [ns] = "
                 << (elapsed.count() * 1000 / NUM_VALUES);
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

  test_defrag();

  return 0;
}
