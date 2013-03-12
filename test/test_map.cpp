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

#include "charset.hpp"
#include "map.hpp"
#include "logger.hpp"
#include "time/time.hpp"

void test_basics() {
  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  grnxx::MapOptions options;
  options.type = grnxx::MAP_DOUBLE_ARRAY;
  std::unique_ptr<grnxx::Map> map(grnxx::Map::create(options, pool));

  std::vector<grnxx::Slice> keys;
  keys.push_back("apple");
  keys.push_back("banana");
  keys.push_back("strawberry");

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(!map->search(keys[i]));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    std::int64_t key_id;
    assert(map->insert(keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    std::int64_t key_id;
    assert(map->search(keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i));
  }

  const std::uint32_t block_id = map->block_id();
  map.reset(grnxx::Map::open(pool, block_id));

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(!map->insert(keys[i]));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(map->remove(keys[i]));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(!map->search(keys[i]));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(!map->remove(keys[i]));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(map->insert(keys[i]));
  }

  std::vector<grnxx::Slice> new_keys;
  new_keys.push_back("dog");
  new_keys.push_back("monkey");
  new_keys.push_back("bird");

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(map->update(keys[i], new_keys[i]));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(!map->search(keys[i]));
    assert(map->search(new_keys[i]));
  }
}

void test_lcp_search() {
  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  grnxx::MapOptions options;
  options.type = grnxx::MAP_DOUBLE_ARRAY;
  std::unique_ptr<grnxx::Map> map(grnxx::Map::create(options, pool));

  assert(map->insert("AB"));
  assert(map->insert("ABCD"));
  assert(map->insert("ABE"));

  std::int64_t key_id;
  grnxx::MapKey key;

  assert(!map->lcp_search("", &key_id, &key));
  assert(!map->lcp_search("A", &key_id, &key));
  assert(map->lcp_search("AB", &key_id, &key));
  assert(key_id == 0);
  assert(key == "AB");
  assert(map->lcp_search("ABC", &key_id, &key));
  assert(key_id == 0);
  assert(key == "AB");
  assert(map->lcp_search("ABCD", &key_id, &key));
  assert(key_id == 1);
  assert(key == "ABCD");
  assert(map->lcp_search("ABCDE", &key_id, &key));
  assert(key_id == 1);
  assert(key == "ABCD");
  assert(map->lcp_search("ABE", &key_id, &key));
  assert(key_id == 2);
  assert(key == "ABE");
  assert(map->lcp_search("ABEF", &key_id, &key));
  assert(key_id == 2);
  assert(key == "ABE");
  assert(!map->lcp_search("BCD", &key_id, &key));
}

void test_scan() {
  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  grnxx::MapOptions options;
  options.type = grnxx::MAP_DOUBLE_ARRAY;
  std::unique_ptr<grnxx::Map> map(grnxx::Map::create(options, pool));

  assert(map->insert("AB"));
  assert(map->insert("ABCD"));
  assert(map->insert("BCD"));
  assert(map->insert("CDE"));
  assert(map->insert("EF"));
  assert(map->insert("EFG"));
  assert(map->insert("EFGH"));
  assert(map->insert("FG"));

  grnxx::Slice query = "ABCDXEFG";

  std::unique_ptr<grnxx::MapScan> scan(map->scan(query));

  assert(scan->next());
  assert(scan->offset() == 0);
  assert(scan->size() == 4);
  assert(scan->key_id() == 1);
  assert(scan->key() == "ABCD");

  assert(scan->next());
  assert(scan->offset() == 5);
  assert(scan->size() == 3);
  assert(scan->key_id() == 5);
  assert(scan->key() == "EFG");

  assert(!scan->next());

  scan.reset(map->scan(query, grnxx::Charset::open(grnxx::CHARSET_UTF_8)));

  assert(scan->next());
  assert(scan->offset() == 0);
  assert(scan->size() == 4);
  assert(scan->key_id() == 1);
  assert(scan->key() == "ABCD");

  assert(scan->next());
  assert(scan->offset() == 5);
  assert(scan->size() == 3);
  assert(scan->key_id() == 5);
  assert(scan->key() == "EFG");

  assert(!scan->next());

  map.reset(grnxx::Map::create(options, pool));

  assert(map->insert("今"));
  assert(map->insert("今日"));
  assert(map->insert("明日"));
  assert(map->insert("良い"));
  assert(map->insert("悪い"));
  assert(map->insert("天気"));
  assert(map->insert("です"));

  query = "今日は良い天気ですね";

  scan.reset(map->scan(query, grnxx::Charset::open(grnxx::CHARSET_UTF_8)));

  assert(scan->next());
  assert(scan->offset() == 0);
  assert(scan->size() == 6);
  assert(scan->key_id() == 1);
  assert(scan->key() == "今日");

  assert(scan->next());
  assert(scan->offset() == 9);
  assert(scan->size() == 6);
  assert(scan->key_id() == 3);
  assert(scan->key() == "良い");

  assert(scan->next());
  assert(scan->offset() == 15);
  assert(scan->size() == 6);
  assert(scan->key_id() == 5);
  assert(scan->key() == "天気");

  assert(scan->next());
  assert(scan->offset() == 21);
  assert(scan->size() == 6);
  assert(scan->key_id() == 6);
  assert(scan->key() == "です");

  assert(!scan->next());
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
  constexpr std::size_t NUM_KEYS = 1 << 16;
  constexpr std::size_t MIN_SIZE = 1;
  constexpr std::size_t MAX_SIZE = 10;

  std::mt19937 random;

  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  grnxx::MapOptions options;
  options.type = grnxx::MAP_DOUBLE_ARRAY;
  std::unique_ptr<grnxx::Map> map(grnxx::Map::create(options, pool));

  std::unordered_set<std::string> both_keys;
  std::vector<grnxx::Slice> true_keys;
  std::vector<grnxx::Slice> false_keys;
  create_keys(NUM_KEYS, MIN_SIZE, MAX_SIZE,
              &both_keys, &true_keys, &false_keys);

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    std::int64_t key_id;
    assert(map->insert(true_keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i));

    assert(!map->insert(true_keys[i], &key_id));

    key_id = i + 1;
    assert(map->search(true_keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    std::int64_t key_id;
    assert(map->search(true_keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i));

    assert(!map->search(false_keys[i], &key_id));
  }
}

void test_remove() {
  constexpr std::size_t NUM_KEYS = 1 << 16;
  constexpr std::size_t MIN_SIZE = 1;
  constexpr std::size_t MAX_SIZE = 10;

  std::mt19937 random;

  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  grnxx::MapOptions options;
  options.type = grnxx::MAP_DOUBLE_ARRAY;
  std::unique_ptr<grnxx::Map> map(grnxx::Map::create(options, pool));

  std::unordered_set<std::string> both_keys;
  std::vector<grnxx::Slice> true_keys;
  std::vector<grnxx::Slice> false_keys;
  create_keys(NUM_KEYS, MIN_SIZE, MAX_SIZE,
              &both_keys, &true_keys, &false_keys);

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    std::int64_t key_id;
    assert(map->insert(true_keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i * 2));
    assert(map->insert(false_keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>((i * 2) + 1));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(map->remove((i * 2) + 1));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(map->search(true_keys[i]));
    assert(!map->search(false_keys[i]));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(map->insert(false_keys[i]));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(map->search(true_keys[i]));
    assert(map->search(false_keys[i]));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(map->remove(false_keys[i]));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(map->search(true_keys[i]));
    assert(!map->search(false_keys[i]));
  }
}

void test_update() {
  constexpr std::size_t NUM_KEYS = 1 << 16;
  constexpr std::size_t MIN_SIZE = 1;
  constexpr std::size_t MAX_SIZE = 10;

  std::mt19937 random;

  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  grnxx::MapOptions options;
  options.type = grnxx::MAP_DOUBLE_ARRAY;
  std::unique_ptr<grnxx::Map> map(grnxx::Map::create(options, pool));

  std::unordered_set<std::string> both_keys;
  std::vector<grnxx::Slice> true_keys;
  std::vector<grnxx::Slice> false_keys;
  create_keys(NUM_KEYS, MIN_SIZE, MAX_SIZE,
              &both_keys, &true_keys, &false_keys);

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    std::int64_t key_id;
    assert(map->insert(true_keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(!map->update(i, true_keys[i]));
    assert(map->update(i, false_keys[i]));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(!map->search(true_keys[i]));
    assert(map->search(false_keys[i]));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(!map->update(true_keys[i], false_keys[i]));
    assert(map->update(false_keys[i], true_keys[i]));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(map->search(true_keys[i]));
    assert(!map->search(false_keys[i]));
  }
}

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_basics();
  test_lcp_search();
  test_scan();

  test_insert();
  test_remove();
  test_update();

  return 0;
}
