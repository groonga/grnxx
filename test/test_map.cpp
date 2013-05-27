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
#include "grnxx/map/bytes_store.hpp"
#include "grnxx/map/helper.hpp"
#include "grnxx/periodic_clock.hpp"
#include "grnxx/storage.hpp"

namespace {

constexpr std::uint64_t MIN_KEY_SIZE = 0;
constexpr std::uint64_t MAX_KEY_SIZE = 16;

constexpr std::uint64_t MAP_NUM_KEYS         = 100;
constexpr std::uint64_t BYTES_STORE_NUM_KEYS = 1 << 14;

std::random_device random_device;
std::mt19937_64 mersenne_twister(random_device());

// For std::unordered_set.
template <typename T>
struct Hash {
  uint64_t operator()(T x) const {
    return std::hash<T>()(x);
  }
};
template <>
struct Hash<grnxx::GeoPoint> {
  using ValueArg = typename grnxx::Traits<grnxx::GeoPoint>::ArgumentType;
  uint64_t operator()(ValueArg x) const {
    // TODO: To be replaced with grnxx's hash function.
    return std::hash<std::uint64_t>()(x.value());
  }
};

// For std::random_shuffle().
struct RandomNumberGenerator {
  uint64_t operator()(uint64_t upper_limit) const {
    return mersenne_twister() % upper_limit;
  }
};

// Generate a random key.
template <typename T>
T generate_random_key() {
  const std::uint64_t random_value = mersenne_twister();
  const T *key = reinterpret_cast<const T *>(&random_value);
  return grnxx::map::Helper<T>::normalize(*key);
}
// Generate a random key and it is valid until the next call.
template <>
grnxx::Bytes generate_random_key() {
  static uint8_t buf[MAX_KEY_SIZE];
  const std::uint64_t key_size =
      MIN_KEY_SIZE + (mersenne_twister() % (MAX_KEY_SIZE - MIN_KEY_SIZE + 1));
  for (std::uint64_t i = 0; i < key_size; ++i) {
    buf[i] = 'A' + (mersenne_twister() % 26);
  }
  return grnxx::Bytes(buf, key_size);
}

// Generate random keys.
template <typename T>
void generate_random_keys(std::vector<T> *keys, std::uint64_t num_keys) {
  std::unordered_set<T, Hash<T>> keyset;
  while (keyset.size() < num_keys) {
    keyset.insert(generate_random_key<T>());
  }
  *keys = std::vector<T>(keyset.begin(), keyset.end());
  std::random_shuffle(keys->begin(), keys->end(), RandomNumberGenerator());
}
// Generate random keys and those are valid until the next call.
template <>
void generate_random_keys(std::vector<grnxx::Bytes> *keys,
                          std::uint64_t num_keys) {
  static std::unordered_set<std::string> keyset;
  keyset.clear();
  while (keyset.size() < num_keys) {
    const grnxx::Bytes key = generate_random_key<grnxx::Bytes>();
    keyset.insert(std::string(reinterpret_cast<const char *>(key.ptr()),
                              key.size()));
  }
  keys->clear();
  for (auto it = keyset.begin(); it != keyset.end(); ++it) {
    keys->push_back(grnxx::Bytes(it->data(), it->size()));
  }
  std::random_shuffle(keys->begin(), keys->end(), RandomNumberGenerator());
}

void test_bytes_store_create() {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::map::BytesStore> store(
      grnxx::map::BytesStore::create(storage.get(),
                                     grnxx::STORAGE_ROOT_NODE_ID));
  assert(store);
}

void test_bytes_store_open() {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::map::BytesStore> store(
      grnxx::map::BytesStore::create(storage.get(),
                                     grnxx::STORAGE_ROOT_NODE_ID));
  assert(store);
  const std::uint32_t storage_node_id = store->storage_node_id();
  store.reset(grnxx::map::BytesStore::open(storage.get(), storage_node_id));
  assert(store);
}

void test_bytes_store_unlink() {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::map::BytesStore> store(
      grnxx::map::BytesStore::create(storage.get(),
                                     grnxx::STORAGE_ROOT_NODE_ID));
  assert(store);
  grnxx::StorageNode storage_node =
      storage->open_node(store->storage_node_id());
  assert(storage_node);
  assert(grnxx::map::BytesStore::unlink(storage.get(), storage_node.id()));
  assert(storage_node.status() == grnxx::STORAGE_NODE_UNLINKED);
}

void test_bytes_store_storage_node_id() {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::map::BytesStore> store(
      grnxx::map::BytesStore::create(storage.get(),
                                     grnxx::STORAGE_ROOT_NODE_ID));
  assert(store);
  grnxx::StorageNode storage_node =
      storage->open_node(store->storage_node_id());
  assert(storage_node);
  assert(storage_node.status() == grnxx::STORAGE_NODE_ACTIVE);
}

void test_bytes_store_get() {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::map::BytesStore> store(
      grnxx::map::BytesStore::create(storage.get(),
                                     grnxx::STORAGE_ROOT_NODE_ID));
  assert(store);
  std::vector<grnxx::Bytes> keys;
  std::vector<std::uint64_t> key_ids;
  generate_random_keys(&keys, BYTES_STORE_NUM_KEYS);

  for (std::uint64_t i = 0; i < BYTES_STORE_NUM_KEYS; ++i) {
    std::uint64_t key_id;
    assert(store->add(keys[i], &key_id));
    grnxx::Bytes stored_key;
    assert(store->get(key_id, &stored_key));
    assert(keys[i] == stored_key);
    key_ids.push_back(key_id);
  }
  for (std::uint64_t i = 0; i < BYTES_STORE_NUM_KEYS; ++i) {
    grnxx::Bytes stored_key;
    assert(store->get(key_ids[i], &stored_key));
    assert(keys[i] == stored_key);
  }
}

void test_bytes_store_unset() {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::map::BytesStore> store(
      grnxx::map::BytesStore::create(storage.get(),
                                     grnxx::STORAGE_ROOT_NODE_ID));
  assert(store);
  std::vector<grnxx::Bytes> keys;
  std::vector<std::uint64_t> key_ids;
  generate_random_keys(&keys, BYTES_STORE_NUM_KEYS);

  for (std::uint64_t i = 0; i < BYTES_STORE_NUM_KEYS; ++i) {
    std::uint64_t key_id;
    assert(store->add(keys[i], &key_id));
    assert(store->unset(key_id));
  }
  for (std::uint64_t i = 0; i < BYTES_STORE_NUM_KEYS; ++i) {
    std::uint64_t key_id;
    assert(store->add(keys[i], &key_id));
    key_ids.push_back(key_id);
  }
  for (std::uint64_t i = 0; i < BYTES_STORE_NUM_KEYS; ++i) {
    assert(store->unset(key_ids[i]));
  }
}

void test_bytes_store_add() {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::map::BytesStore> store(
      grnxx::map::BytesStore::create(storage.get(),
                                     grnxx::STORAGE_ROOT_NODE_ID));
  assert(store);
  std::vector<grnxx::Bytes> keys;
  generate_random_keys(&keys, BYTES_STORE_NUM_KEYS);

  for (std::uint64_t i = 0; i < BYTES_STORE_NUM_KEYS; ++i) {
    std::uint64_t key_id;
    assert(store->add(keys[i], &key_id));
  }
}

void test_bytes_store_sweep() {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::map::BytesStore> store(
      grnxx::map::BytesStore::create(storage.get(),
                                     grnxx::STORAGE_ROOT_NODE_ID));
  assert(store);
  std::vector<grnxx::Bytes> keys;
  std::vector<std::uint64_t> key_ids;
  generate_random_keys(&keys, BYTES_STORE_NUM_KEYS);

  for (std::uint64_t i = 0; i < BYTES_STORE_NUM_KEYS; ++i) {
    std::uint64_t key_id;
    assert(store->add(keys[i], &key_id));
    assert(store->unset(key_id));
  }
  assert(store->sweep(grnxx::Duration(0)));
  for (std::uint64_t i = 0; i < BYTES_STORE_NUM_KEYS; ++i) {
    std::uint64_t key_id;
    assert(store->add(keys[i], &key_id));
    key_ids.push_back(key_id);
  }
  for (std::uint64_t i = 0; i < BYTES_STORE_NUM_KEYS; ++i) {
    assert(store->unset(key_ids[i]));
  }
  assert(store->sweep(grnxx::Duration(0)));
}

template <typename T>
void test_map_create(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
}

template <typename T>
void test_map_open(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  const std::uint32_t storage_node_id = map->storage_node_id();
  map.reset(grnxx::Map<T>::open(storage.get(), storage_node_id));
  assert(map);
}

template <typename T>
void test_map_unlink(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  grnxx::StorageNode storage_node = storage->open_node(map->storage_node_id());
  assert(storage_node);
  assert(grnxx::Map<T>::unlink(storage.get(), storage_node.id()));
  assert(storage_node.status() == grnxx::STORAGE_NODE_UNLINKED);
}

template <typename T>
void test_map_type(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  assert(map->type() == map_type);
}

template <typename T>
void test_map_storage_node_id(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  grnxx::StorageNode storage_node = storage->open_node(map->storage_node_id());
  assert(storage_node);
  assert(storage_node.status() == grnxx::STORAGE_NODE_ACTIVE);
}

template <typename T>
void test_map_min_key_id(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  assert(map->min_key_id() == grnxx::MAP_MIN_KEY_ID);
  assert(map->add(generate_random_key<T>()));
  assert(map->min_key_id() == grnxx::MAP_MIN_KEY_ID);
  assert(map->unset(grnxx::MAP_MIN_KEY_ID));
  assert(map->min_key_id() == grnxx::MAP_MIN_KEY_ID);
}

template <typename T>
void test_map_max_key_id(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  assert(map->max_key_id() == (grnxx::MAP_MIN_KEY_ID - 1));
  assert(map->add(generate_random_key<T>()));
  assert(map->max_key_id() == grnxx::MAP_MIN_KEY_ID);
  assert(map->unset(grnxx::MAP_MIN_KEY_ID));
  assert(map->max_key_id() == grnxx::MAP_MIN_KEY_ID);
}

template <typename T>
void test_map_next_key_id(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  assert(map->next_key_id() == grnxx::MAP_MIN_KEY_ID);
  assert(map->add(generate_random_key<T>()));
  assert(map->next_key_id() == (grnxx::MAP_MIN_KEY_ID + 1));
  assert(map->unset(grnxx::MAP_MIN_KEY_ID));
  assert(map->max_key_id() == grnxx::MAP_MIN_KEY_ID);
}

template <typename T>
void test_map_num_keys(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  assert(map->num_keys() == 0);
  assert(map->add(generate_random_key<T>()));
  assert(map->num_keys() == 1);
  assert(map->unset(grnxx::MAP_MIN_KEY_ID));
  assert(map->num_keys() == 0);
}

template <typename T>
void test_map_get(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  std::vector<T> keys;
  generate_random_keys(&keys, MAP_NUM_KEYS);

  for (std::uint64_t i = 0; i < MAP_NUM_KEYS; ++i) {
    assert(!map->get(i));
    assert(map->add(keys[i]));
    assert(map->get(i));
  }
  for (std::uint64_t i = 0; i < MAP_NUM_KEYS; ++i) {
    T key;
    assert(map->get(i, &key));
    assert(grnxx::map::Helper<T>::equal_to(key, keys[i]));
  }
}

template <typename T>
void test_map_get_next(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  std::int64_t key_id;
  std::vector<T> keys;
  generate_random_keys(&keys, MAP_NUM_KEYS);

  assert(!map->get_next(grnxx::MAP_INVALID_KEY_ID));
  generate_random_keys(&keys, MAP_NUM_KEYS);
  for (std::uint64_t i = 0; i < MAP_NUM_KEYS; ++i) {
    assert(map->add(keys[i]));
  }
  key_id = grnxx::MAP_INVALID_KEY_ID;
  for (std::uint64_t i = 0; i < MAP_NUM_KEYS; ++i) {
    T key;
    assert(map->get_next(key_id, &key_id, &key));
    assert(key_id == static_cast<std::int64_t>(i));
    assert(grnxx::map::Helper<T>::equal_to(key, keys[i]));
  }
  assert(!map->get_next(key_id));
  for (std::uint64_t i = 0; i < MAP_NUM_KEYS; i += 2) {
    assert(map->unset(i));
  }
  key_id = grnxx::MAP_INVALID_KEY_ID;
  for (std::uint64_t i = 1; i < MAP_NUM_KEYS; i += 2) {
    T key;
    assert(map->get_next(key_id, &key_id, &key));
    assert(key_id == static_cast<std::int64_t>(i));
    assert(grnxx::map::Helper<T>::equal_to(key, keys[i]));
  }
  assert(!map->get_next(key_id));
}

template <typename T>
void test_map_unset(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  std::vector<T> keys;
  generate_random_keys(&keys, MAP_NUM_KEYS);

  for (std::uint64_t i = 0; i < MAP_NUM_KEYS; ++i) {
    assert(!map->unset(i));
    assert(map->add(keys[i]));
  }
  for (std::uint64_t i = 0; i < MAP_NUM_KEYS; ++i) {
    assert(map->unset(i));
    assert(!map->get(i));
    assert(!map->unset(i));
  }
}

template <typename T>
void test_map_reset(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  std::vector<T> keys;
  generate_random_keys(&keys, MAP_NUM_KEYS);

  for (std::uint64_t i = 0; i < (MAP_NUM_KEYS / 2); ++i) {
    assert(!map->reset(i, keys[i]));
    assert(map->add(keys[i]));
  }
  assert(!map->reset(grnxx::MAP_MIN_KEY_ID, keys[0]));
  for (std::uint64_t i = (MAP_NUM_KEYS / 2); i < MAP_NUM_KEYS; ++i) {
    const std::int64_t key_id = i - (MAP_NUM_KEYS / 2);
    T key;
    assert(!map->reset(key_id, keys[key_id]));
    assert(map->reset(key_id, keys[i]));
    assert(map->get(key_id, &key));
    assert(grnxx::map::Helper<T>::equal_to(key, keys[i]));
  }
}

template <typename T>
void test_map_find(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  std::vector<T> keys;
  generate_random_keys(&keys, MAP_NUM_KEYS);

  for (std::uint64_t i = 0; i < MAP_NUM_KEYS; ++i) {
    assert(!map->find(keys[i]));
    assert(map->add(keys[i]));
  }
  for (std::uint64_t i = 0; i < MAP_NUM_KEYS; ++i) {
    std::int64_t key_id;
    assert(map->find(keys[i], &key_id));
    assert(key_id == static_cast<int64_t>(i));
  }
}

template <typename T>
void test_map_add(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  std::vector<T> keys;
  generate_random_keys(&keys, MAP_NUM_KEYS);

  for (std::uint64_t i = 0; i < MAP_NUM_KEYS; ++i) {
    std::int64_t key_id;
    assert(map->add(keys[i], &key_id));
    assert(key_id == static_cast<std::int64_t>(i));
    assert(!map->add(keys[i]));
  }
}

template <typename T>
void test_map_remove(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  std::vector<T> keys;
  generate_random_keys(&keys, MAP_NUM_KEYS);

  for (std::uint64_t i = 0; i < MAP_NUM_KEYS; ++i) {
    assert(!map->remove(keys[i]));
    assert(map->add(keys[i]));
  }
  for (std::uint64_t i = 0; i < MAP_NUM_KEYS; ++i) {
    assert(map->remove(keys[i]));
    assert(!map->find(keys[i]));
    assert(!map->remove(keys[i]));
  }
}

template <typename T>
void test_map_replace(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  std::vector<T> keys;
  generate_random_keys(&keys, MAP_NUM_KEYS);

  for (std::uint64_t i = 0; i < (MAP_NUM_KEYS / 2); ++i) {
    assert(!map->replace(keys[i], keys[i]));
    assert(map->add(keys[i]));
  }
  for (std::uint64_t i = (MAP_NUM_KEYS / 2); i < MAP_NUM_KEYS; ++i) {
    const std::int64_t key_id = i - (MAP_NUM_KEYS / 2);
    assert(!map->replace(keys[key_id], keys[key_id]));
    assert(map->replace(keys[key_id], keys[i]));
    T key;
    assert(map->get(key_id, &key));
    assert(grnxx::map::Helper<T>::equal_to(key, keys[i]));
  }
}

template <typename T>
void test_map_find_longest_prefix_match(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  std::vector<T> keys;
  generate_random_keys(&keys, MAP_NUM_KEYS);

  // TODO
}

template <typename T>
void test_map_truncate(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  std::vector<T> keys;
  generate_random_keys(&keys, MAP_NUM_KEYS);

  for (std::uint64_t i = 0; i < (MAP_NUM_KEYS / 2); ++i) {
    assert(map->add(keys[i]));
  }
  assert(map->truncate());
  assert(map->max_key_id() == (grnxx::MAP_MIN_KEY_ID - 1));
  assert(map->next_key_id() == grnxx::MAP_MIN_KEY_ID);
  assert(map->num_keys() == 0);
  for (std::uint64_t i = 0; i < (MAP_NUM_KEYS / 2); ++i) {
    assert(map->add(keys[i]));
  }
}

template <typename T>
void test_map_all_keys(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);

  map->all_keys();
}

template <typename T>
void test_map_key_id(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  constexpr std::int64_t min = 10;
  constexpr std::int64_t max = 100;

  map->key_id() > min;
  map->key_id() >= min;
  map->key_id() < max;
  map->key_id() <= max;

  min < map->key_id();
  min <= map->key_id();
  max > map->key_id();
  max >= map->key_id();

  (map->key_id() > min) && (map->key_id() < max);
  (map->key_id() > min) && (map->key_id() <= max);
  (map->key_id() >= min) && (map->key_id() < max);
  (map->key_id() >= min) && (map->key_id() <= max);

  (map->key_id() < max) && (map->key_id() > min);
  (map->key_id() <= max) && (map->key_id() > min);
  (map->key_id() < max) && (map->key_id() >= min);
  (map->key_id() <= max) && (map->key_id() >= min);
}

template <typename T>
void test_map_key(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  const T min = generate_random_key<T>();
  const T max = generate_random_key<T>();

  map->key() > min;
  map->key() >= min;
  map->key() < max;
  map->key() <= max;

  min < map->key();
  min <= map->key();
  max > map->key();
  max >= map->key();

  (map->key() > min) && (map->key() < max);
  (map->key() > min) && (map->key() <= max);
  (map->key() >= min) && (map->key() < max);
  (map->key() >= min) && (map->key() <= max);

  (map->key() < max) && (map->key() > min);
  (map->key() <= max) && (map->key() > min);
  (map->key() < max) && (map->key() >= min);
  (map->key() <= max) && (map->key() >= min);
}

template <typename T>
void test_map_create_key_id_range_cursor(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  constexpr std::int64_t min = 10;
  constexpr std::int64_t max = 100;
  std::unique_ptr<grnxx::MapCursor<T>> cursor;

  cursor.reset(map->create_cursor(map->key_id() > min));
  assert(cursor);
  cursor.reset(map->create_cursor(map->key_id() >= min));
  assert(cursor);
  cursor.reset(map->create_cursor(map->key_id() < max));
  assert(cursor);
  cursor.reset(map->create_cursor(map->key_id() <= max));
  assert(cursor);

  cursor.reset(map->create_cursor(min < map->key_id()));
  assert(cursor);
  cursor.reset(map->create_cursor(min <= map->key_id()));
  assert(cursor);
  cursor.reset(map->create_cursor(max > map->key_id()));
  assert(cursor);
  cursor.reset(map->create_cursor(max >= map->key_id()));
  assert(cursor);

  cursor.reset(map->create_cursor(
      (map->key_id() > min) && (map->key_id() < max)));
  assert(cursor);
  cursor.reset(map->create_cursor(
      (map->key_id() > min) && (map->key_id() <= max)));
  assert(cursor);
  cursor.reset(map->create_cursor(
      (map->key_id() >= min) && (map->key_id() < max)));
  assert(cursor);
  cursor.reset(map->create_cursor(
      (map->key_id() >= min) && (map->key_id() <= max)));
  assert(cursor);

  cursor.reset(map->create_cursor(
      (map->key_id() < max) && (map->key_id() > min)));
  assert(cursor);
  cursor.reset(map->create_cursor(
      (map->key_id() <= max) && (map->key_id() > min)));
  assert(cursor);
  cursor.reset(map->create_cursor(
      (map->key_id() < max) && (map->key_id() >= min)));
  assert(cursor);
  cursor.reset(map->create_cursor(
      (map->key_id() <= max) && (map->key_id() >= min)));
  assert(cursor);
}

template <typename T>
void test_map_create_key_range_cursor(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  const T min = generate_random_key<T>();
  const T max = generate_random_key<T>();
  std::unique_ptr<grnxx::MapCursor<T>> cursor;

  cursor.reset(map->create_cursor(map->key() > min));
  assert(cursor);
  cursor.reset(map->create_cursor(map->key() >= min));
  assert(cursor);
  cursor.reset(map->create_cursor(map->key() < max));
  assert(cursor);
  cursor.reset(map->create_cursor(map->key() <= max));
  assert(cursor);

  cursor.reset(map->create_cursor(min < map->key()));
  assert(cursor);
  cursor.reset(map->create_cursor(min <= map->key()));
  assert(cursor);
  cursor.reset(map->create_cursor(max > map->key()));
  assert(cursor);
  cursor.reset(map->create_cursor(max >= map->key()));
  assert(cursor);

  cursor.reset(map->create_cursor(
      (map->key() > min) && (map->key() < max)));
  assert(cursor);
  cursor.reset(map->create_cursor(
      (map->key() > min) && (map->key() <= max)));
  assert(cursor);
  cursor.reset(map->create_cursor(
      (map->key() >= min) && (map->key() < max)));
  assert(cursor);
  cursor.reset(map->create_cursor(
      (map->key() >= min) && (map->key() <= max)));
  assert(cursor);

  cursor.reset(map->create_cursor(
      (map->key() < max) && (map->key() > min)));
  assert(cursor);
  cursor.reset(map->create_cursor(
      (map->key() <= max) && (map->key() > min)));
  assert(cursor);
  cursor.reset(map->create_cursor(
      (map->key() < max) && (map->key() >= min)));
  assert(cursor);
  cursor.reset(map->create_cursor(
      (map->key() <= max) && (map->key() >= min)));
  assert(cursor);
}

template <>
void test_map_create_key_range_cursor<grnxx::GeoPoint>(grnxx::MapType) {
  // This operation is not supported for grnxx::GeoPoint.
}

template <typename T>
void test_map_create_scanner(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map(
      grnxx::Map<T>::create(map_type, storage.get(),
                            grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
}

template <typename T>
void test_map(grnxx::MapType map_type) {
  test_map_create<T>(map_type);
  test_map_open<T>(map_type);
  test_map_unlink<T>(map_type);
  test_map_storage_node_id<T>(map_type);
  test_map_type<T>(map_type);
  test_map_min_key_id<T>(map_type);
  test_map_max_key_id<T>(map_type);
  test_map_next_key_id<T>(map_type);
  test_map_num_keys<T>(map_type);
  test_map_get<T>(map_type);
  test_map_get_next<T>(map_type);
  test_map_unset<T>(map_type);
  test_map_reset<T>(map_type);
  test_map_find<T>(map_type);
  test_map_add<T>(map_type);
  test_map_remove<T>(map_type);
  test_map_replace<T>(map_type);
  test_map_find_longest_prefix_match<T>(map_type);
  test_map_truncate<T>(map_type);
  test_map_all_keys<T>(map_type);
  test_map_key_id<T>(map_type);
  test_map_key<T>(map_type);
  test_map_create_key_id_range_cursor<T>(map_type);
  test_map_create_key_range_cursor<T>(map_type);
  test_map_create_scanner<T>(map_type);
}

template <typename T>
void test_map(T) {
  GRNXX_NOTICE() << __PRETTY_FUNCTION__;
  test_map<T>(grnxx::MAP_ARRAY);
}

template <typename T, typename... U>
void test_map(T, U... args) {
  test_map(T());
  test_map(args...);
}

void test_bytes_store() {
  test_bytes_store_create();
  test_bytes_store_open();
  test_bytes_store_unlink();
  test_bytes_store_storage_node_id();
  test_bytes_store_get();
  test_bytes_store_unset();
  test_bytes_store_add();
  test_bytes_store_sweep();
}

void test_map() {
  // TODO: Add grnxx::Bytes.
  test_map(std::int8_t(),
           std::uint8_t(),
           std::int16_t(),
           std::uint16_t(),
           std::int32_t(),
           std::uint32_t(),
           std::int64_t(),
           std::uint64_t(),
           double(),
           grnxx::GeoPoint());
}

}  // namespace

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  // Increment the reference count for grnxx::PeriodicClock.
  grnxx::PeriodicClock clock;

  test_bytes_store();
  test_map();

  return 0;
}
