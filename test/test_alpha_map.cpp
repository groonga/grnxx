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
#include <cmath>
#include <random>
#include <unordered_map>

#include "grnxx/alpha/map.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/time/time.hpp"

template <typename T>
void compare_maps(const std::unique_ptr<grnxx::alpha::Map<T>> &map,
                  const std::unordered_map<T, std::int64_t> &hash_map) {
  for (auto it = hash_map.begin(); it != hash_map.end(); ++it) {
    const T key = it->first;
    const std::int64_t key_id = it->second;

    T stored_key;
    assert(map->get(key_id, &stored_key));
    assert(stored_key == key);

    std::int64_t stored_key_id;
    assert(map->search(key, &stored_key_id));
    assert(stored_key_id == key_id);
  }
}

template <typename T>
void test_map() {
  GRNXX_NOTICE() << __PRETTY_FUNCTION__;

  std::mt19937_64 rng;

  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_ANONYMOUS);

  std::unique_ptr<grnxx::alpha::Map<T>> map(
      grnxx::alpha::Map<T>::create(grnxx::alpha::MAP_ARRAY, pool));

  constexpr std::size_t size = (sizeof(T) == 1) ? 128 : 1024;
  std::unordered_map<T, std::int64_t> hash_map;
  while (hash_map.size() < size) {
    const std::uint64_t key_src = rng();
    const T key = *reinterpret_cast<const T *>(&key_src);
    if (std::isnan(static_cast<double>(key))) {
      continue;
    }

    auto pair = hash_map.insert(std::make_pair(key, hash_map.size()));
    const int64_t key_id = pair.first->second;
    const bool is_new = pair.second;

    std::int64_t stored_key_id;
    assert(map->insert(key, &stored_key_id) == is_new);
    assert(stored_key_id == key_id);
    assert(!map->insert(key, &stored_key_id));

    T stored_key;
    assert(map->get(key_id, &stored_key));
    assert(stored_key == key);

    assert(map->search(key, &stored_key_id));
    assert(stored_key_id == key_id);
  }

  std::uint32_t block_id = map->block_id();
  map.reset();
  map.reset(grnxx::alpha::Map<T>::open(pool, block_id));

  compare_maps(map, hash_map);

  for (auto it = hash_map.begin(); it != hash_map.end(); ++it) {
    assert(map->unset(it->second));
    assert(!map->unset(it->second));
  }

  for (auto it = hash_map.begin(); it != hash_map.end(); ++it) {
    assert(map->insert(it->first));
  }
  for (auto it = hash_map.begin(); it != hash_map.end(); ++it) {
    assert(map->remove(it->first));
    assert(!map->remove(it->first));
  }

  for (auto it = hash_map.begin(); it != hash_map.end(); ++it) {
    assert(map->insert(it->first));
  }
  map->truncate();
  for (auto it = hash_map.begin(); it != hash_map.end(); ++it) {
    assert(!map->get(it->second));
  }

  map->truncate();
  for (auto it = hash_map.begin(); it != hash_map.end(); ++it) {
    assert(map->insert(it->first, &it->second));
  }
  for (auto it = hash_map.begin(); it != hash_map.end(); ++it) {
    auto old_it = it;
    auto new_it = ++it;
    assert(map->unset(new_it->second));
    assert(map->reset(old_it->second, new_it->first));

    T key;
    assert(map->get(old_it->second, &key));
    assert(key == new_it->first);
    std::int64_t key_id;
    assert(map->search(key, &key_id));
    assert(key_id == old_it->second);
  }

  map->truncate();
  for (auto it = hash_map.begin(); it != hash_map.end(); ++it) {
    assert(map->insert(it->first, &it->second));
  }
  for (auto it = hash_map.begin(); it != hash_map.end(); ++it) {
    auto old_it = it;
    auto new_it = ++it;
    assert(map->remove(new_it->first));
    assert(map->update(old_it->first, new_it->first));

    T key;
    assert(map->get(old_it->second, &key));
    assert(key == new_it->first);
    std::int64_t key_id;
    assert(map->search(key, &key_id));
    assert(key_id == old_it->second);
  }
}

void test_nan() {
  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_ANONYMOUS);

  std::unique_ptr<grnxx::alpha::Map<double>> map;
  map.reset(map->create(grnxx::alpha::MAP_ARRAY, pool));

  const double nan = std::numeric_limits<double>::quiet_NaN();

  std::int64_t key_id;
  assert(map->insert(nan, &key_id));
  assert(key_id == 0);
  assert(!map->insert(nan));

  double key;
  assert(map->get(key_id, &key));
  assert(std::isnan(key));
  assert(map->search(nan, &key_id));
  assert(key_id == 0);

  assert(map->unset(key_id));
  assert(!map->unset(key_id));

  assert(map->insert(nan));
  assert(map->remove(nan));
  assert(!map->remove(nan));

  assert(!map->reset(nan, nan));
  assert(map->insert(nan, &key_id));
  assert(!map->reset(key_id, nan));
  assert(map->reset(key_id, 0.0));
  assert(map->reset(key_id, nan));

  assert(!map->update(nan, nan));
  assert(map->update(nan, 0.0));
  assert(map->update(0.0, nan));
}

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_map<int8_t>();
  test_map<int16_t>();
  test_map<int32_t>();
  test_map<int64_t>();
  test_map<uint8_t>();
  test_map<uint16_t>();
  test_map<uint32_t>();
  test_map<uint64_t>();
  test_map<double>();

  test_nan();

  return 0;
}
