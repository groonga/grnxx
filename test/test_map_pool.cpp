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
#include <memory>
#include <random>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include "grnxx/bytes.hpp"
#include "grnxx/geo_point.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/map.hpp"
#include "grnxx/map/hash.hpp"
#include "grnxx/map/helper.hpp"
#include "grnxx/map/pool.hpp"
#include "grnxx/periodic_clock.hpp"
#include "grnxx/storage.hpp"

namespace {

constexpr std::uint64_t MIN_KEY_SIZE = 0;
constexpr std::uint64_t MAX_KEY_SIZE = 16;

std::random_device random_device;
std::uint64_t mersenne_twister_seed = random_device();
std::mt19937_64 mersenne_twister(mersenne_twister_seed);

// For std::unordered_set.
template <typename T>
using Hash = grnxx::map::Hash<T>;

// For std::random_shuffle().
struct RandomNumberGenerator {
  uint64_t operator()(uint64_t upper_limit) const {
    return mersenne_twister() % upper_limit;
  }
};

// Generate a random key.
template <typename T>
T generate_random_key() {
  return static_cast<T>(mersenne_twister());
}
template <>
bool generate_random_key() {
  return mersenne_twister() & 1;
}
template <>
double generate_random_key() {
  const std::uint64_t random_value = mersenne_twister();
  const double *key = reinterpret_cast<const double *>(&random_value);
  return grnxx::map::Helper<double>::normalize(*key);
}
template <>
grnxx::GeoPoint generate_random_key() {
  const std::uint64_t random_value = mersenne_twister();
  return *reinterpret_cast<const grnxx::GeoPoint *>(&random_value);
}
// Generate a random key.
// Note that it is valid until the next call.
template <>
grnxx::Bytes generate_random_key() {
  static uint8_t buf[MAX_KEY_SIZE];
  const std::uint64_t key_size = MIN_KEY_SIZE +
      (mersenne_twister() % (MAX_KEY_SIZE - MIN_KEY_SIZE + 1));
  for (std::uint64_t i = 0; i < key_size; ++i) {
    buf[i] = 'A' + (mersenne_twister() % 26);
  }
  return grnxx::Bytes(buf, key_size);
}

// Generate random keys.
template <typename T>
void generate_random_keys(std::uint64_t num_keys, std::vector<T> *keys) {
  std::unordered_set<T, Hash<T>> keyset;
  while (keyset.size() < num_keys) {
    keyset.insert(generate_random_key<T>());
  }
  *keys = std::vector<T>(keyset.begin(), keyset.end());
  std::random_shuffle(keys->begin(), keys->end(), RandomNumberGenerator());
}
// Generate random floating point keys.
template <>
void generate_random_keys<double>(std::uint64_t num_keys,
                                  std::vector<double> *keys) {
  std::unordered_set<double, Hash<double>> keyset;
  bool contains_nan = false;
  while ((keyset.size() + (contains_nan ? 1 : 0)) < num_keys) {
    const double key = generate_random_key<double>();
    if (std::isnan(key)) {
      contains_nan = true;
    } else {
      keyset.insert(key);
    }
  }
  *keys = std::vector<double>(keyset.begin(), keyset.end());
  if (contains_nan) {
    keys->insert(keys->begin() + (mersenne_twister() % keys->size()),
                 std::numeric_limits<double>::quiet_NaN());
  }
  std::random_shuffle(keys->begin(), keys->end(), RandomNumberGenerator());
}
// Generate random keys.
// Note that the generated keys are valid until the next call.
template <>
void generate_random_keys(std::uint64_t num_keys,
                          std::vector<grnxx::Bytes> *keys) {
  static std::unordered_set<std::string> keyset;
  keyset.clear();
  while (keyset.size() < num_keys) {
    const grnxx::Bytes key = generate_random_key<grnxx::Bytes>();
    keyset.insert(std::string(reinterpret_cast<const char *>(key.data()),
                              key.size()));
  }
  keys->clear();
  for (auto it = keyset.begin(); it != keyset.end(); ++it) {
    keys->push_back(grnxx::Bytes(it->data(), it->size()));
  }
  std::random_shuffle(keys->begin(), keys->end(), RandomNumberGenerator());
}

// Get the number of keys.
template <typename T>
std::uint64_t get_num_keys() {
  switch (sizeof(T)) {
    case 1: {
      return 1ULL << 6;
    }
    case 2: {
      return 1ULL << 12;
    }
    default: {
      return 1ULL << 17;
    }
  }
}

template <typename T>
void test_map_pool_create() {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::map::Pool<T>> pool(
      grnxx::map::Pool<T>::create(storage.get(),
                                  grnxx::STORAGE_ROOT_NODE_ID));
}

template <typename T>
void test_map_pool_open() {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::map::Pool<T>> pool(
      grnxx::map::Pool<T>::create(storage.get(),
                                  grnxx::STORAGE_ROOT_NODE_ID));
  const std::uint32_t storage_node_id = pool->storage_node_id();
  const T key = generate_random_key<T>();
  const std::int64_t key_id = pool->add(key);
  pool.reset(grnxx::map::Pool<T>::open(storage.get(), storage_node_id));
  assert(pool->storage_node_id() == storage_node_id);
  T stored_key;
  assert(pool->get(key_id, &stored_key));
  assert(key == stored_key);
}

template <typename T>
void test_map_pool_unlink() {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::map::Pool<T>> pool(
      grnxx::map::Pool<T>::create(storage.get(),
                                  grnxx::STORAGE_ROOT_NODE_ID));
  grnxx::StorageNode storage_node =
      storage->open_node(pool->storage_node_id());
  grnxx::map::Pool<T>::unlink(storage.get(), storage_node.id());
  assert(storage_node.status() == grnxx::STORAGE_NODE_UNLINKED);
}

template <typename T>
void test_map_pool_storage_node_id() {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::map::Pool<T>> pool(
      grnxx::map::Pool<T>::create(storage.get(),
                                  grnxx::STORAGE_ROOT_NODE_ID));
  grnxx::StorageNode storage_node =
      storage->open_node(pool->storage_node_id());
  assert(storage_node.status() == grnxx::STORAGE_NODE_ACTIVE);
}

template <typename T>
void test_map_pool_min_key_id() {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::map::Pool<T>> pool(
      grnxx::map::Pool<T>::create(storage.get(),
                                  grnxx::STORAGE_ROOT_NODE_ID));
  assert(pool->min_key_id() == grnxx::map::POOL_MIN_KEY_ID);
  for (std::uint64_t i = 0; i < get_num_keys<T>(); ++i) {
    pool->add(generate_random_key<T>());
    assert(pool->min_key_id() == grnxx::map::POOL_MIN_KEY_ID);
  }
}

template <typename T>
void test_map_pool_max_key_id() {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::map::Pool<T>> pool(
      grnxx::map::Pool<T>::create(storage.get(),
                                  grnxx::STORAGE_ROOT_NODE_ID));
  assert(pool->max_key_id() == (grnxx::map::POOL_MIN_KEY_ID - 1));
  for (std::uint64_t i = 0; i < get_num_keys<T>(); ++i) {
    const std::int64_t key_id = pool->add(generate_random_key<T>());
    assert(pool->max_key_id() == key_id);
  }
}

template <typename T>
void test_map_pool_num_keys() {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::map::Pool<T>> pool(
      grnxx::map::Pool<T>::create(storage.get(),
                                  grnxx::STORAGE_ROOT_NODE_ID));
  assert(pool->num_keys() == 0);
  for (std::uint64_t i = 0; i < get_num_keys<T>(); ++i) {
    pool->add(generate_random_key<T>());
    assert(pool->num_keys() == (i + 1));
  }
}

template <typename T>
void test_map_pool_get() {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::map::Pool<T>> pool(
      grnxx::map::Pool<T>::create(storage.get(),
                                  grnxx::STORAGE_ROOT_NODE_ID));
  std::vector<T> keys;
  std::vector<std::int64_t> key_ids;
  generate_random_keys(get_num_keys<T>(), &keys);
  for (std::uint64_t i = 0; i < keys.size(); ++i) {
    const std::int64_t key_id = pool->add(keys[i]);
    T stored_key;
    assert(pool->get(key_id, &stored_key));
    assert(grnxx::map::Helper<T>::equal_to(stored_key, keys[i]));
    key_ids.push_back(key_id);
  }
  for (std::uint64_t i = 0; i < keys.size(); ++i) {
    T stored_key;
    assert(pool->get(key_ids[i], &stored_key));
    assert(grnxx::map::Helper<T>::equal_to(stored_key, keys[i]));
  }
  for (std::uint64_t i = 0; i < keys.size(); i += 2) {
    pool->unset(key_ids[i]);
    T stored_key;
    assert(!pool->get(key_ids[i], &stored_key));
  }
  for (std::uint64_t i = 0; i < keys.size(); i += 2) {
    T stored_key;
    assert(!pool->get(key_ids[i], &stored_key));
  }
  for (std::uint64_t i = 1; i < keys.size(); i += 2) {
    T stored_key;
    assert(pool->get(key_ids[i], &stored_key));
    assert(grnxx::map::Helper<T>::equal_to(stored_key, keys[i]));
  }
}

template <typename T>
void test_map_pool_get_key() {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::map::Pool<T>> pool(
      grnxx::map::Pool<T>::create(storage.get(),
                                  grnxx::STORAGE_ROOT_NODE_ID));
  std::vector<T> keys;
  std::vector<std::int64_t> key_ids;
  generate_random_keys(get_num_keys<T>(), &keys);
  for (std::uint64_t i = 0; i < keys.size(); ++i) {
    const std::int64_t key_id = pool->add(keys[i]);
    assert(grnxx::map::Helper<T>::equal_to(pool->get_key(key_id), keys[i]));
    key_ids.push_back(key_id);
  }
  for (std::uint64_t i = 0; i < keys.size(); ++i) {
    assert(grnxx::map::Helper<T>::equal_to(pool->get_key(key_ids[i]), keys[i]));
  }
  for (std::uint64_t i = 0; i < keys.size(); ++i) {
    pool->unset(key_ids[i]);
  }
  for (std::uint64_t i = 0; i < keys.size(); ++i) {
    assert(grnxx::map::Helper<T>::equal_to(pool->get_key(key_ids[i]), keys[i]));
  }
}

template <typename T>
void test_map_pool_get_bit() {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::map::Pool<T>> pool(
      grnxx::map::Pool<T>::create(storage.get(),
                                  grnxx::STORAGE_ROOT_NODE_ID));
  std::vector<T> keys;
  std::vector<std::int64_t> key_ids;
  generate_random_keys(get_num_keys<T>(), &keys);
  for (std::uint64_t i = 0; i < keys.size(); ++i) {
    const std::int64_t key_id = pool->add(keys[i]);
    assert(pool->get_bit(key_id));
    key_ids.push_back(key_id);
  }
  for (std::uint64_t i = 0; i < keys.size(); ++i) {
    assert(pool->get_bit(key_ids[i]));
  }
  for (std::uint64_t i = 0; i < keys.size(); i += 2) {
    pool->unset(key_ids[i]);
    assert(!pool->get_bit(key_ids[i]));
  }
  for (std::uint64_t i = 0; i < keys.size(); i += 2) {
    assert(!pool->get_bit(key_ids[i]));
  }
  for (std::uint64_t i = 1; i < keys.size(); i += 2) {
    assert(pool->get_bit(key_ids[i]));
  }
}

template <typename T>
void test_map_pool_unset() {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::map::Pool<T>> pool(
      grnxx::map::Pool<T>::create(storage.get(),
                                  grnxx::STORAGE_ROOT_NODE_ID));
  std::vector<T> keys;
  std::vector<std::int64_t> key_ids;
  generate_random_keys(get_num_keys<T>(), &keys);
  for (std::uint64_t i = 0; i < keys.size(); ++i) {
    const std::int64_t key_id = pool->add(keys[i]);
    assert(pool->get_bit(key_id));
    pool->unset(key_id);
    assert(!pool->get_bit(key_id));
  }
  for (std::uint64_t i = 0; i < keys.size(); ++i) {
    const std::int64_t key_id = pool->add(keys[i]);
    key_ids.push_back(key_id);
  }
  for (std::uint64_t i = 0; i < keys.size(); ++i) {
    pool->unset(key_ids[i]);
    assert(!pool->get_bit(key_ids[i]));
  }
}

template <typename T>
void test_map_pool_add() {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::map::Pool<T>> pool(
      grnxx::map::Pool<T>::create(storage.get(),
                                  grnxx::STORAGE_ROOT_NODE_ID));
  std::vector<T> keys;
  std::vector<std::int64_t> key_ids;
  generate_random_keys(get_num_keys<T>(), &keys);
  for (std::uint64_t i = 0; i < keys.size(); ++i) {
    const std::int64_t key_id = pool->add(keys[i]);
    assert(key_id >= pool->min_key_id());
    assert(key_id <= pool->max_key_id());
    T stored_key;
    assert(pool->get(key_id, &stored_key));
    assert(grnxx::map::Helper<T>::equal_to(stored_key, keys[i]));
    key_ids.push_back(key_id);
  }
  for (std::uint64_t i = 0; i < keys.size(); ++i) {
    T stored_key;
    assert(pool->get(key_ids[i], &stored_key));
    assert(grnxx::map::Helper<T>::equal_to(stored_key, keys[i]));
  }
}

template <typename T>
void test_map_pool_defrag() {
  // TODO
}

template <typename T>
void test_map_pool_sweep() {
  // TODO
}

template <typename T>
void test_map_pool() {
  test_map_pool_create<T>();
  test_map_pool_open<T>();
  test_map_pool_unlink<T>();
  test_map_pool_storage_node_id<T>();
  test_map_pool_min_key_id<T>();
  test_map_pool_max_key_id<T>();
  test_map_pool_num_keys<T>();
  test_map_pool_get<T>();
  test_map_pool_get_key<T>();
  test_map_pool_get_bit<T>();
  test_map_pool_unset<T>();
  test_map_pool_add<T>();
  test_map_pool_defrag<T>();
  test_map_pool_sweep<T>();
}

}  // namespace

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  // FIXME: Increment the reference count for grnxx::PeriodicClock.
  grnxx::PeriodicClock clock;

  GRNXX_NOTICE() << "mersenne_twister_seed = " << mersenne_twister_seed;

  test_map_pool<std::int8_t>();
  test_map_pool<std::int16_t>();
  test_map_pool<std::int32_t>();
  test_map_pool<std::int64_t>();
  test_map_pool<std::uint8_t>();
  test_map_pool<std::uint16_t>();
  test_map_pool<std::uint32_t>();
  test_map_pool<std::uint64_t>();
  test_map_pool<double>();
  test_map_pool<grnxx::GeoPoint>();
  // TODO
//  test_map_pool<grnxx::Bytes>();

  return 0;
}
