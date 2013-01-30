/*
  Copyright (C) 2013  Brazil, Inc.

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
#include <random>
#include <string>
#include <unordered_set>

#include "alpha/double_array.hpp"
#include "logger.hpp"
#include "time.hpp"

void test_basics() {
  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  grnxx::alpha::DoubleArray da;
  da.create(pool);

  std::uint64_t key_id;
  assert(!da.search("apple", 5, &key_id));

  assert(da.insert("apple", 5, &key_id));
  GRNXX_NOTICE() << "key_id = " << key_id;

  assert(!da.insert("apple", 5, &key_id));

  assert(da.search("apple", 5, &key_id));
  GRNXX_NOTICE() << "key_id = " << key_id;

  assert(da.insert("banana", 6, &key_id));
  GRNXX_NOTICE() << "key_id = " << key_id;

  assert(!da.insert("banana", 6, &key_id));

  assert(da.insert("orange", 6, &key_id));
  GRNXX_NOTICE() << "key_id = " << key_id;

  assert(!da.insert("orange", 6, &key_id));

  assert(da.search("banana", 6, &key_id));
  GRNXX_NOTICE() << "key_id = " << key_id;

  assert(da.search("orange", 6, &key_id));
  GRNXX_NOTICE() << "key_id = " << key_id;

  assert(da.insert("oracle", 6, &key_id));
  GRNXX_NOTICE() << "key_id = " << key_id;

  assert(!da.insert("oracle", 6, &key_id));
}

void test_insert() {
  constexpr std::size_t NUM_KEYS = 1 << 16;
  constexpr std::size_t MIN_LENGTH = 1;
  constexpr std::size_t MAX_LENGTH = 10;

  std::mt19937 random;

  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  grnxx::alpha::DoubleArray da;
  da.create(pool);

  std::vector<std::string> true_keys(NUM_KEYS);
  std::vector<std::string> false_keys(NUM_KEYS);
  {
    std::unordered_set<std::string> keys;
    while (keys.size() < (NUM_KEYS * 2)) {
      std::string key;
      key.resize(MIN_LENGTH + (random() % (MAX_LENGTH - MIN_LENGTH + 1)));
      for (std::size_t j = 0; j < key.length(); ++j) {
        key[j] = '0' + (random() % 10);
      }
      keys.insert(key);
    }
    auto it = keys.begin();
    for (std::size_t i = 0; i < NUM_KEYS; ++i) {
      true_keys[i] = *it;
      ++it;
      false_keys[i] = *it;
      ++it;
    }
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    std::uint64_t key_id;
    assert(da.insert(true_keys[i].c_str(), true_keys[i].length(), &key_id));
    assert(key_id == i);

    assert(!da.insert(true_keys[i].c_str(), true_keys[i].length(), &key_id));

    key_id = i + 1;
    assert(da.search(true_keys[i].c_str(), true_keys[i].length(), &key_id));
    assert(key_id == i);
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    std::uint64_t key_id;
    assert(da.search(true_keys[i].c_str(), true_keys[i].length(), &key_id));
    assert(key_id == i);

    assert(!da.search(false_keys[i].c_str(), false_keys[i].length(), &key_id));
  }
}

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_basics();

  test_insert();

  return 0;
}
