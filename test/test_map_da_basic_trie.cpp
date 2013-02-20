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

#include "map/da/basic_trie.hpp"
#include "logger.hpp"
#include "time.hpp"

void test_basics() {
  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  grnxx::map::da::TrieOptions options;
  std::unique_ptr<grnxx::map::da::Trie> trie(
      grnxx::map::da::basic::Trie::create(options, pool));

  std::vector<grnxx::Slice> keys;
  keys.push_back("apple");
  keys.push_back("banana");
  keys.push_back("strawberry");

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(!trie->search(keys[i]));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    std::int64_t key_id;
    assert(trie->insert(keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    std::int64_t key_id;
    assert(trie->search(keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i));
  }

  const std::uint32_t block_id = trie->block_id();
  trie.reset(grnxx::map::da::basic::Trie::open(pool, block_id));

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(!trie->insert(keys[i]));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(trie->remove(keys[i]));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(!trie->search(keys[i]));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(!trie->remove(keys[i]));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(trie->insert(keys[i]));
  }

  std::vector<grnxx::Slice> new_keys;
  new_keys.push_back("dog");
  new_keys.push_back("monkey");
  new_keys.push_back("bird");

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(trie->update(keys[i], new_keys[i]));
  }

  for (std::size_t i = 0; i < keys.size(); ++i) {
    assert(!trie->search(keys[i]));
    assert(trie->search(new_keys[i]));
  }
}

void test_lcp_search() {
  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  grnxx::map::da::TrieOptions options;
  std::unique_ptr<grnxx::map::da::Trie> trie(
      grnxx::map::da::basic::Trie::create(options, pool));

  assert(trie->insert("AB"));
  assert(trie->insert("ABCD"));
  assert(trie->insert("ABE"));

  std::int64_t key_id;
  grnxx::Slice key;

  assert(!trie->lcp_search("", &key_id, &key));
  assert(!trie->lcp_search("A", &key_id, &key));
  assert(trie->lcp_search("AB", &key_id, &key));
  assert(key_id == 0);
  assert(key == "AB");
  assert(trie->lcp_search("ABC", &key_id, &key));
  assert(key_id == 0);
  assert(key == "AB");
  assert(trie->lcp_search("ABCD", &key_id, &key));
  assert(key_id == 1);
  assert(key == "ABCD");
  assert(trie->lcp_search("ABCDE", &key_id, &key));
  assert(key_id == 1);
  assert(key == "ABCD");
  assert(trie->lcp_search("ABE", &key_id, &key));
  assert(key_id == 2);
  assert(key == "ABE");
  assert(trie->lcp_search("ABEF", &key_id, &key));
  assert(key_id == 2);
  assert(key == "ABE");
  assert(!trie->lcp_search("BCD", &key_id, &key));
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
  constexpr std::size_t NUM_KEYS = 1 << 12;
  constexpr std::size_t MIN_SIZE = 1;
  constexpr std::size_t MAX_SIZE = 10;

  std::mt19937 random;

  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  grnxx::map::da::TrieOptions options;
  std::unique_ptr<grnxx::map::da::Trie> trie(
      grnxx::map::da::basic::Trie::create(options, pool));

  std::unordered_set<std::string> both_keys;
  std::vector<grnxx::Slice> true_keys;
  std::vector<grnxx::Slice> false_keys;
  create_keys(NUM_KEYS, MIN_SIZE, MAX_SIZE,
              &both_keys, &true_keys, &false_keys);

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    std::int64_t key_id;
    assert(trie->insert(true_keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i));

    assert(!trie->insert(true_keys[i], &key_id));

    key_id = i + 1;
    assert(trie->search(true_keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    std::int64_t key_id;
    assert(trie->search(true_keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i));

    assert(!trie->search(false_keys[i], &key_id));
  }
}

void test_remove() {
  constexpr std::size_t NUM_KEYS = 1 << 12;
  constexpr std::size_t MIN_SIZE = 1;
  constexpr std::size_t MAX_SIZE = 10;

  std::mt19937 random;

  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  grnxx::map::da::TrieOptions options;
  std::unique_ptr<grnxx::map::da::Trie> trie(
      grnxx::map::da::basic::Trie::create(options, pool));

  std::unordered_set<std::string> both_keys;
  std::vector<grnxx::Slice> true_keys;
  std::vector<grnxx::Slice> false_keys;
  create_keys(NUM_KEYS, MIN_SIZE, MAX_SIZE,
              &both_keys, &true_keys, &false_keys);

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    std::int64_t key_id;
    assert(trie->insert(true_keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i * 2));
    assert(trie->insert(false_keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>((i * 2) + 1));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(trie->remove((i * 2) + 1));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(trie->search(true_keys[i]));
    assert(!trie->search(false_keys[i]));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(trie->insert(false_keys[i]));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(trie->search(true_keys[i]));
    assert(trie->search(false_keys[i]));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(trie->remove(false_keys[i]));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(trie->search(true_keys[i]));
    assert(!trie->search(false_keys[i]));
  }
}

void test_update() {
  constexpr std::size_t NUM_KEYS = 1 << 12;
  constexpr std::size_t MIN_SIZE = 1;
  constexpr std::size_t MAX_SIZE = 10;

  std::mt19937 random;

  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  grnxx::map::da::TrieOptions options;
  std::unique_ptr<grnxx::map::da::Trie> trie(
      grnxx::map::da::basic::Trie::create(options, pool));

  std::unordered_set<std::string> both_keys;
  std::vector<grnxx::Slice> true_keys;
  std::vector<grnxx::Slice> false_keys;
  create_keys(NUM_KEYS, MIN_SIZE, MAX_SIZE,
              &both_keys, &true_keys, &false_keys);

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    std::int64_t key_id;
    assert(trie->insert(true_keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(!trie->update(i, true_keys[i]));
    assert(trie->update(i, false_keys[i]));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(!trie->search(true_keys[i]));
    assert(trie->search(false_keys[i]));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(!trie->update(true_keys[i], false_keys[i]));
    assert(trie->update(false_keys[i], true_keys[i]));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    assert(trie->search(true_keys[i]));
    assert(!trie->search(false_keys[i]));
  }
}

void test_defrag() {
  constexpr std::size_t NUM_KEYS = 1 << 12;
  constexpr std::size_t MIN_SIZE = 1;
  constexpr std::size_t MAX_SIZE = 10;

  std::mt19937 random;

  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_TEMPORARY);

  grnxx::map::da::TrieOptions options;
  std::unique_ptr<grnxx::map::da::Trie> trie(
      grnxx::map::da::basic::Trie::create(options, pool));

  std::unordered_set<std::string> both_keys;
  std::vector<grnxx::Slice> true_keys;
  std::vector<grnxx::Slice> false_keys;
  create_keys(NUM_KEYS, MIN_SIZE, MAX_SIZE,
              &both_keys, &true_keys, &false_keys);

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    std::int64_t key_id;
    assert(trie->insert(true_keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i));
  }

  options.nodes_size = grnxx::map::da::basic::INITIAL_NODES_SIZE;
  options.entries_size = grnxx::map::da::basic::INITIAL_ENTRIES_SIZE;
  options.keys_size = grnxx::map::da::basic::INITIAL_KEYS_SIZE;
  std::unique_ptr<grnxx::map::da::Trie> new_trie(
      trie->defrag(options));

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    std::int64_t key_id;
    assert(new_trie->search(true_keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i));

    assert(!new_trie->search(false_keys[i], &key_id));
  }

  for (std::size_t i = 0; i < NUM_KEYS; ++i) {
    std::int64_t key_id;
    assert(new_trie->insert(false_keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(NUM_KEYS + i));
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

  test_defrag();

  return 0;
}
