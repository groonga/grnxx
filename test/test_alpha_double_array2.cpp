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
#include <vector>

#include "alpha/double_array2.hpp"
#include "logger.hpp"
#include "time.hpp"

void test_basics() {
  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  std::unique_ptr<grnxx::alpha::DoubleArray2> da(
      grnxx::alpha::DoubleArray2::create(pool));

  std::vector<std::string> keys;
  keys.push_back("apple");
  keys.push_back("banana");
  keys.push_back("strawberry");

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(!da->search_by_key(keys[i].c_str(), keys[i].length()));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    std::int64_t key_id;
    assert(da->insert(keys[i].c_str(), keys[i].length(), &key_id));
    assert(key_id == static_cast<std::int64_t>(i));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    std::int64_t key_id;
    assert(da->search_by_key(keys[i].c_str(), keys[i].length(), &key_id));
    assert(key_id == static_cast<std::int64_t>(i));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(!da->insert(keys[i].c_str(), keys[i].length()));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(da->remove_by_key(keys[i].c_str(), keys[i].length()));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(!da->search_by_key(keys[i].c_str(), keys[i].length()));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(!da->remove_by_key(keys[i].c_str(), keys[i].length()));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(da->insert(keys[i].c_str(), keys[i].length()));
  }

  std::vector<std::string> new_keys;
  new_keys.push_back("dog");
  new_keys.push_back("monkey");
  new_keys.push_back("bird");

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(da->update_by_key(keys[i].c_str(), keys[i].length(),
                             new_keys[i].c_str(), new_keys[i].length()));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(!da->search_by_key(keys[i].c_str(), keys[i].length()));
    assert(da->search_by_key(new_keys[i].c_str(), new_keys[i].length()));
  }
}

void test_insert() {
  constexpr std::size_t NUM_KEYS = 1 << 12;
  constexpr std::size_t MIN_LENGTH = 1;
  constexpr std::size_t MAX_LENGTH = 10;

  std::mt19937 random;

  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  std::unique_ptr<grnxx::alpha::DoubleArray2> da(
      grnxx::alpha::DoubleArray2::create(pool));

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
    std::int64_t key_id;
    assert(da->insert(true_keys[i].c_str(), true_keys[i].length(), &key_id));
    assert(key_id == static_cast<std::int64_t>(i));

    assert(!da->insert(true_keys[i].c_str(), true_keys[i].length(), &key_id));

    key_id = i + 1;
    assert(da->search_by_key(true_keys[i].c_str(), true_keys[i].length(),
                             &key_id));
    assert(key_id == static_cast<std::int64_t>(i));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    std::int64_t key_id;
    assert(da->search_by_key(true_keys[i].c_str(), true_keys[i].length(),
                             &key_id));
    assert(key_id == static_cast<std::int64_t>(i));

    assert(!da->search_by_key(false_keys[i].c_str(), false_keys[i].length(),
                              &key_id));
  }
}

void test_remove() {
  constexpr std::size_t NUM_KEYS = 1 << 12;
  constexpr std::size_t MIN_LENGTH = 1;
  constexpr std::size_t MAX_LENGTH = 10;

  std::mt19937 random;

  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  std::unique_ptr<grnxx::alpha::DoubleArray2> da(
      grnxx::alpha::DoubleArray2::create(pool));

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
    std::int64_t key_id;
    assert(da->insert(true_keys[i].c_str(), true_keys[i].length(), &key_id));
    assert(key_id == static_cast<std::int64_t>(i * 2));
    assert(da->insert(false_keys[i].c_str(), false_keys[i].length(), &key_id));
    assert(key_id == static_cast<std::int64_t>((i * 2) + 1));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(da->remove_by_id((i * 2) + 1));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(da->search_by_key(true_keys[i].c_str(), true_keys[i].length()));
    assert(!da->search_by_key(false_keys[i].c_str(), false_keys[i].length()));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(da->insert(false_keys[i].c_str(), false_keys[i].length()));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(da->search_by_key(true_keys[i].c_str(), true_keys[i].length()));
    assert(da->search_by_key(false_keys[i].c_str(), false_keys[i].length()));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(da->remove_by_key(false_keys[i].c_str(), false_keys[i].length()));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(da->search_by_key(true_keys[i].c_str(), true_keys[i].length()));
    assert(!da->search_by_key(false_keys[i].c_str(), false_keys[i].length()));
  }
}

void test_update() {
  constexpr std::size_t NUM_KEYS = 1 << 12;
  constexpr std::size_t MIN_LENGTH = 1;
  constexpr std::size_t MAX_LENGTH = 10;

  std::mt19937 random;

  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  std::unique_ptr<grnxx::alpha::DoubleArray2> da(
      grnxx::alpha::DoubleArray2::create(pool));

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
    std::int64_t key_id;
    assert(da->insert(true_keys[i].c_str(), true_keys[i].length(), &key_id));
    assert(key_id == static_cast<std::int64_t>(i));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(!da->update_by_id(i, true_keys[i].c_str(), true_keys[i].length()));
    assert(da->update_by_id(i, false_keys[i].c_str(), false_keys[i].length()));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(!da->search_by_key(true_keys[i].c_str(), true_keys[i].length()));
    assert(da->search_by_key(false_keys[i].c_str(), false_keys[i].length()));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(!da->update_by_key(true_keys[i].c_str(), true_keys[i].length(),
                              false_keys[i].c_str(), false_keys[i].length()));
    assert(da->update_by_key(false_keys[i].c_str(), false_keys[i].length(),
                             true_keys[i].c_str(), true_keys[i].length()));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(da->search_by_key(true_keys[i].c_str(), true_keys[i].length()));
    assert(!da->search_by_key(false_keys[i].c_str(), false_keys[i].length()));
  }
}

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_basics();

  test_insert();
  test_remove();
  test_update();

  return 0;
}
