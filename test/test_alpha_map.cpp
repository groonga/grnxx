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
#include <functional>
#include <random>
#include <vector>
#include <unordered_map>

#include "grnxx/alpha/map.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/time/time.hpp"

template <typename T>
bool isNaN(T) {
  return false;
}

template<>
bool isNaN(double key) {
  return std::isnan(key);
}

template <typename T>
struct Hash {
  std::size_t operator()(T key) const {
    return std::hash<T>()(key);
  }
};

template <>
struct Hash<grnxx::alpha::GeoPoint> {
  std::size_t operator()(grnxx::alpha::GeoPoint key) const {
    return std::hash<std::int64_t>()(key.latitude()) ^
           std::hash<std::int64_t>()(key.longitude());
  }
};

template <>
struct Hash<grnxx::Slice> {
  std::size_t operator()(const grnxx::Slice &key) const {
    std::size_t hash_value = 14695981039346656037ULL;
    for (std::size_t i = 0; i < key.size(); ++i) {
      hash_value *= 1099511628211ULL;
      hash_value ^= key[i];
    }
    return hash_value;
  }
};

template <typename T>
using Map = grnxx::alpha::Map<T>;

template <typename T>
using MapCursor = grnxx::alpha::MapCursor<T>;

template <typename T>
using HashMap = std::unordered_map<T, std::int64_t, Hash<T>>;

template <typename T>
void generate_key(T *key) {
  static std::mt19937_64 rng;

  std::uint64_t key_src = rng();
  *key = *reinterpret_cast<const T *>(&key_src);
  while (isNaN(*key)) {
    key_src = rng();
    *key = *reinterpret_cast<const T *>(&key_src);
  }
}

void generate_key(grnxx::Slice *key) {
  static std::mt19937_64 rng;
  static std::vector<std::string> keys;

  const std::size_t MIN_SIZE = 1;
  const std::size_t MAX_SIZE = 16;  // TODO: should be 4096 in future.

  std::size_t size = MIN_SIZE + (rng() % (MAX_SIZE - MIN_SIZE + 1));
  std::string key_buf;
  key_buf.resize(size);
  for (std::size_t i = 0; i < size; ++i) {
    key_buf[i] = 'A' + (rng() % 26);
  }
  keys.push_back(key_buf);

  *key = grnxx::Slice(keys.back().data(), keys.back().size());
}

template <typename T>
void compare_maps(const std::unique_ptr<Map<T>> &map,
                  const HashMap<T> &hash_map) {
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
void test_basic_cursor(const std::unique_ptr<Map<T>> &map,
                       std::size_t MAP_SIZE) {
  std::unique_ptr<MapCursor<T>> cursor(
      map->open_basic_cursor());
  for (std::size_t i = 0; i < MAP_SIZE; ++i) {
    assert(cursor->next());
  }
  assert(!cursor->next());

  grnxx::alpha::MapCursorOptions options;
  options.flags |= grnxx::alpha::MAP_CURSOR_EXCEPT_MIN |
                   grnxx::alpha::MAP_CURSOR_EXCEPT_MAX;
  cursor.reset(map->open_basic_cursor(options));
  for (std::size_t i = 0; i < MAP_SIZE; ++i) {
    assert(cursor->next());
  }
  assert(!cursor->next());

  options.flags = grnxx::alpha::MAP_CURSOR_ORDER_BY_KEY;
  cursor.reset(map->open_basic_cursor(options));
  assert(cursor->next());
  T prev_key = cursor->key();
  for (std::size_t i = 1; i < MAP_SIZE; ++i) {
    assert(cursor->next());
    assert(prev_key < cursor->key());
    prev_key = cursor->key();
  }
  assert(!cursor->next());
}

template <>
void test_basic_cursor(
    const std::unique_ptr<Map<grnxx::alpha::GeoPoint>> &map,
    std::size_t MAP_SIZE) {
  std::unique_ptr<MapCursor<grnxx::alpha::GeoPoint>> cursor(
      map->open_basic_cursor());
  for (std::size_t i = 0; i < MAP_SIZE; ++i) {
    assert(cursor->next());
  }
  assert(!cursor->next());

  grnxx::alpha::MapCursorOptions options;
  options.flags |= grnxx::alpha::MAP_CURSOR_EXCEPT_MIN |
                   grnxx::alpha::MAP_CURSOR_EXCEPT_MAX;
  cursor.reset(map->open_basic_cursor(options));
  for (std::size_t i = 0; i < MAP_SIZE; ++i) {
    assert(cursor->next());
  }
  assert(!cursor->next());
}

template <typename T>
void test_id_cursor(const std::unique_ptr<Map<T>> &map,
                    std::size_t MAP_SIZE) {
  const std::int64_t MIN_ID = MAP_SIZE / 4;
  const std::int64_t MAX_ID = (MAP_SIZE * 3) / 4;

  grnxx::alpha::MapCursorOptions options;
  options.flags |= grnxx::alpha::MAP_CURSOR_ORDER_BY_ID;
  std::unique_ptr<MapCursor<T>> cursor(
      map->open_id_cursor(MIN_ID, MAX_ID, options));
  for (std::int64_t i = MIN_ID; i <= MAX_ID; ++i) {
    assert(cursor->next());
    assert(cursor->key_id() == i);
    T key;
    assert(map->get(i, &key));
    assert(cursor->key() == key);
  }
  assert(!cursor->next());

  options.flags |= grnxx::alpha::MAP_CURSOR_EXCEPT_MIN |
                   grnxx::alpha::MAP_CURSOR_EXCEPT_MAX;
  cursor.reset(map->open_id_cursor(MIN_ID, MAX_ID, options));
  for (std::int64_t i = MIN_ID + 1; i <= (MAX_ID - 1); ++i) {
    assert(cursor->next());
    assert(cursor->key_id() == i);
    T key;
    assert(map->get(i, &key));
    assert(cursor->key() == key);
  }
  assert(!cursor->next());

  options.flags = grnxx::alpha::MAP_CURSOR_ORDER_BY_KEY;
  cursor.reset(map->open_id_cursor(MIN_ID, MAX_ID, options));
  assert(cursor->next());
  T prev_key = cursor->key();
  for (std::int64_t i = MIN_ID + 1; i <= MAX_ID; ++i) {
    assert(cursor->next());
    assert(prev_key < cursor->key());
    prev_key = cursor->key();
  }
  assert(!cursor->next());
}

template <>
void test_id_cursor(const std::unique_ptr<Map<grnxx::alpha::GeoPoint>> &map,
                    std::size_t MAP_SIZE) {
  const std::int64_t MIN_ID = MAP_SIZE / 4;
  const std::int64_t MAX_ID = (MAP_SIZE * 3) / 4;

  grnxx::alpha::MapCursorOptions options;
  options.flags |= grnxx::alpha::MAP_CURSOR_ORDER_BY_ID;
  std::unique_ptr<MapCursor<grnxx::alpha::GeoPoint>> cursor(
      map->open_id_cursor(MIN_ID, MAX_ID, options));
  for (std::int64_t i = MIN_ID; i <= MAX_ID; ++i) {
    assert(cursor->next());
    assert(cursor->key_id() == i);
    grnxx::alpha::GeoPoint key;
    assert(map->get(i, &key));
    assert(cursor->key() == key);
  }
  assert(!cursor->next());

  options.flags |= grnxx::alpha::MAP_CURSOR_EXCEPT_MIN |
                   grnxx::alpha::MAP_CURSOR_EXCEPT_MAX;
  cursor.reset(map->open_id_cursor(MIN_ID, MAX_ID, options));
  for (std::int64_t i = MIN_ID + 1; i <= (MAX_ID - 1); ++i) {
    assert(cursor->next());
    assert(cursor->key_id() == i);
    grnxx::alpha::GeoPoint key;
    assert(map->get(i, &key));
    assert(cursor->key() == key);
  }
  assert(!cursor->next());
}

template <typename T>
void test_key_cursor(const std::unique_ptr<Map<T>> &map) {
  T min_key, max_key;
  generate_key(&min_key);
  generate_key(&max_key);
  if (min_key > max_key) {
    std::swap(min_key, max_key);
  }

  std::unique_ptr<MapCursor<T>> cursor(
      map->open_key_cursor(min_key, max_key));
  while (cursor->next()) {
    assert(cursor->key() >= min_key);
    assert(cursor->key() <= max_key);
  }
  assert(!cursor->next());

  grnxx::alpha::MapCursorOptions options;
  options.flags |= grnxx::alpha::MAP_CURSOR_EXCEPT_MIN |
                   grnxx::alpha::MAP_CURSOR_EXCEPT_MAX;
  cursor.reset(map->open_key_cursor(min_key, max_key, options));
  while (cursor->next()) {
    assert(cursor->key() > min_key);
    assert(cursor->key() < max_key);
  }
  assert(!cursor->next());

  options.flags = grnxx::alpha::MAP_CURSOR_ORDER_BY_KEY;
  cursor.reset(map->open_key_cursor(min_key, max_key, options));
  if (cursor->next()) {
    assert(cursor->key() >= min_key);
    assert(cursor->key() <= max_key);
  }
  T prev_key = cursor->key();
  while (cursor->next()) {
    assert(cursor->key() >= min_key);
    assert(cursor->key() <= max_key);
    assert(prev_key < cursor->key());
    prev_key = cursor->key();
  }
  assert(!cursor->next());
}

void test_key_cursor(
    const std::unique_ptr<Map<grnxx::alpha::GeoPoint>> &) {
  // Not supported.
}

template <typename T>
void test_map_array() {
  GRNXX_NOTICE() << __PRETTY_FUNCTION__;

  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_ANONYMOUS);

  std::unique_ptr<Map<T>> map(
      Map<T>::create(grnxx::alpha::MAP_ARRAY, pool));

  constexpr std::size_t MAP_SIZE = (sizeof(T) == 1) ? 128 : 1024;
  HashMap<T> hash_map;
  while (hash_map.size() < MAP_SIZE) {
    T key;
    generate_key(&key);

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

  compare_maps(map, hash_map);

  test_basic_cursor(map, MAP_SIZE);
  test_id_cursor(map, MAP_SIZE);
  test_key_cursor(map);

  std::uint32_t block_id = map->block_id();
  map.reset();
  map.reset(Map<T>::open(pool, block_id));

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

  std::unique_ptr<Map<double>> map;
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

template <typename T>
void test_map_double_array() {
  GRNXX_NOTICE() << __PRETTY_FUNCTION__;

  grnxx::io::Pool pool;
  pool.open(grnxx::io::POOL_ANONYMOUS);

  std::unique_ptr<Map<T>> map(
      Map<T>::create(grnxx::alpha::MAP_DOUBLE_ARRAY, pool));

  constexpr std::size_t MAP_SIZE = (sizeof(T) == 1) ? 128 : 1024;
  HashMap<T> hash_map;
  while (hash_map.size() < MAP_SIZE) {
    T key;
    generate_key(&key);

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

  compare_maps(map, hash_map);

  test_basic_cursor(map, MAP_SIZE);
  test_id_cursor(map, MAP_SIZE);
  test_key_cursor(map);

  std::uint32_t block_id = map->block_id();
  map.reset();
  map.reset(Map<T>::open(pool, block_id));

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

  // FIXME: truncate() is not supported.

//  for (auto it = hash_map.begin(); it != hash_map.end(); ++it) {
//    assert(map->insert(it->first));
//  }
//  map->truncate();
//  for (auto it = hash_map.begin(); it != hash_map.end(); ++it) {
//    assert(!map->get(it->second));
//  }

//  map->truncate();
  map.reset(Map<T>::create(grnxx::alpha::MAP_DOUBLE_ARRAY, pool));
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

//  map->truncate();
  map.reset(Map<T>::create(grnxx::alpha::MAP_DOUBLE_ARRAY, pool));
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

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_map_array<int8_t>();
  test_map_array<int16_t>();
  test_map_array<int32_t>();
  test_map_array<int64_t>();
  test_map_array<uint8_t>();
  test_map_array<uint16_t>();
  test_map_array<uint32_t>();
  test_map_array<uint64_t>();
  test_map_array<double>();
  test_map_array<grnxx::alpha::GeoPoint>();
  test_map_array<grnxx::Slice>();

  test_nan();

  test_map_double_array<grnxx::Slice>();

  return 0;
}
