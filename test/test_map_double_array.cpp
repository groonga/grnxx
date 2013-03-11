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

#include "map/double_array.hpp"
#include "logger.hpp"
#include "time/time.hpp"

void test_basics() {
  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  grnxx::MapOptions options;
  std::unique_ptr<grnxx::map::DoubleArray> da(
      grnxx::map::DoubleArray::create(options, pool));

  std::vector<grnxx::Slice> keys;
  keys.push_back("apple");
  keys.push_back("banana");
  keys.push_back("strawberry");

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(!da->search(keys[i]));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    std::int64_t key_id;
    assert(da->insert(keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    std::int64_t key_id;
    assert(da->search(keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i));
  }

  const std::uint32_t block_id = da->block_id();
  da.reset(grnxx::map::DoubleArray::open(pool, block_id));

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(!da->insert(keys[i]));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(da->remove(keys[i]));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(!da->search(keys[i]));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(!da->remove(keys[i]));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(da->insert(keys[i]));
  }

  std::vector<grnxx::Slice> new_keys;
  new_keys.push_back("dog");
  new_keys.push_back("monkey");
  new_keys.push_back("bird");

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(da->update(keys[i], new_keys[i]));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(!da->search(keys[i]));
    assert(da->search(new_keys[i]));
  }
}

void test_lcp_search() {
  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  grnxx::MapOptions options;
  std::unique_ptr<grnxx::map::DoubleArray> da(
      grnxx::map::DoubleArray::create(options, pool));

  assert(da->insert("AB"));
  assert(da->insert("ABCD"));
  assert(da->insert("ABE"));

  std::int64_t key_id;
  grnxx::MapKey key;

  assert(!da->lcp_search("", &key_id, &key));
  assert(!da->lcp_search("A", &key_id, &key));
  assert(da->lcp_search("AB", &key_id, &key));
  assert(key_id == 0);
  assert(key == "AB");
  assert(da->lcp_search("ABC", &key_id, &key));
  assert(key_id == 0);
  assert(key == "AB");
  assert(da->lcp_search("ABCD", &key_id, &key));
  assert(key_id == 1);
  assert(key == "ABCD");
  assert(da->lcp_search("ABCDE", &key_id, &key));
  assert(key_id == 1);
  assert(key == "ABCD");
  assert(da->lcp_search("ABE", &key_id, &key));
  assert(key_id == 2);
  assert(key == "ABE");
  assert(da->lcp_search("ABEF", &key_id, &key));
  assert(key_id == 2);
  assert(key == "ABE");
  assert(!da->lcp_search("BCD", &key_id, &key));
}

void create_keys(std::size_t num_keys,
                 std::size_t min_size, std::size_t max_size,
                 std::unordered_set<std::string> *both_keys,
                 std::vector<grnxx::Slice> *true_keys,
                 std::vector<grnxx::Slice> *false_keys) {
  both_keys->clear();
  true_keys->resize(num_keys);
  false_keys->resize(num_keys);
  {
    while (both_keys->size() < (num_keys * 2)) {
      std::string key;
      key.resize(min_size + (random() % (max_size - min_size + 1)));
      for (std::size_t i = 0; i < key.size(); ++i) {
        key[i] = '0' + (random() % 10);
      }
      both_keys->insert(key);
    }
    auto it = both_keys->begin();
    for (std::size_t i = 0; i < num_keys; ++i) {
      (*true_keys)[i] = grnxx::Slice(it->data(), it->size());
      ++it;
      (*false_keys)[i] = grnxx::Slice(it->data(), it->size());
      ++it;
    }
  }
}

void test_insert() {
  constexpr std::size_t NUM_KEYS = 1 << 15;
  constexpr std::size_t MIN_SIZE = 1;
  constexpr std::size_t MAX_SIZE = 10;

  std::mt19937 random;

  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  grnxx::MapOptions options;
  std::unique_ptr<grnxx::map::DoubleArray> da(
      grnxx::map::DoubleArray::create(options, pool));

  std::unordered_set<std::string> both_keys;
  std::vector<grnxx::Slice> true_keys;
  std::vector<grnxx::Slice> false_keys;
  create_keys(NUM_KEYS, MIN_SIZE, MAX_SIZE,
              &both_keys, &true_keys, &false_keys);

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    std::int64_t key_id;
    assert(da->insert(true_keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i));

    assert(!da->insert(true_keys[i], &key_id));

    key_id = i + 1;
    assert(da->search(true_keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    std::int64_t key_id;
    assert(da->search(true_keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i));

    assert(!da->search(false_keys[i], &key_id));
  }
}

void test_remove() {
  constexpr std::size_t NUM_KEYS = 1 << 15;
  constexpr std::size_t MIN_SIZE = 1;
  constexpr std::size_t MAX_SIZE = 10;

  std::mt19937 random;

  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  grnxx::MapOptions options;
  std::unique_ptr<grnxx::map::DoubleArray> da(
      grnxx::map::DoubleArray::create(options, pool));

  std::unordered_set<std::string> both_keys;
  std::vector<grnxx::Slice> true_keys;
  std::vector<grnxx::Slice> false_keys;
  create_keys(NUM_KEYS, MIN_SIZE, MAX_SIZE,
              &both_keys, &true_keys, &false_keys);

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    std::int64_t key_id;
    assert(da->insert(true_keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i * 2));
    assert(da->insert(false_keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>((i * 2) + 1));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(da->remove((i * 2) + 1));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(da->search(true_keys[i]));
    assert(!da->search(false_keys[i]));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(da->insert(false_keys[i]));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(da->search(true_keys[i]));
    assert(da->search(false_keys[i]));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(da->remove(false_keys[i]));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(da->search(true_keys[i]));
    assert(!da->search(false_keys[i]));
  }
}

void test_update() {
  constexpr std::size_t NUM_KEYS = 1 << 15;
  constexpr std::size_t MIN_SIZE = 1;
  constexpr std::size_t MAX_SIZE = 10;

  std::mt19937 random;

  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  grnxx::MapOptions options;
  std::unique_ptr<grnxx::map::DoubleArray> da(
      grnxx::map::DoubleArray::create(options, pool));

  std::unordered_set<std::string> both_keys;
  std::vector<grnxx::Slice> true_keys;
  std::vector<grnxx::Slice> false_keys;
  create_keys(NUM_KEYS, MIN_SIZE, MAX_SIZE,
              &both_keys, &true_keys, &false_keys);

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    std::int64_t key_id;
    assert(da->insert(true_keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(!da->update(i, true_keys[i]));
    assert(da->update(i, false_keys[i]));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(!da->search(true_keys[i]));
    assert(da->search(false_keys[i]));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(!da->update(true_keys[i], false_keys[i]));
    assert(da->update(false_keys[i], true_keys[i]));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(da->search(true_keys[i]));
    assert(!da->search(false_keys[i]));
  }
}

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_basics();
  test_lcp_search();

  test_insert();
  test_remove();
  test_update();

  return 0;
}
