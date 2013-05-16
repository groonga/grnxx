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
#include <cassert>
#include <memory>
#include <type_traits>

#include "grnxx/bytes.hpp"
#include "grnxx/geo_point.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/map.hpp"
#include "grnxx/storage.hpp"
#include "grnxx/time/periodic_clock.hpp"

namespace {

template <typename T>
void test_map_create(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map;

  map.reset(grnxx::Map<T>::create(map_type, storage.get(),
                                  grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
}

template <typename T>
void test_map_open(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map;

  map.reset(grnxx::Map<T>::create(map_type, storage.get(),
                                  grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  const uint32_t storage_node_id = map->storage_node_id();
  map.reset(grnxx::Map<T>::open(storage.get(), storage_node_id));
  assert(map);
}

template <typename T>
void test_map_unlink(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map;

  map.reset(grnxx::Map<T>::create(map_type, storage.get(),
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
  std::unique_ptr<grnxx::Map<T>> map;

  map.reset(grnxx::Map<T>::create(map_type, storage.get(),
                                  grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  assert(map->type() == map_type);
}

template <typename T>
void test_map_storage_node_id(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map;

  map.reset(grnxx::Map<T>::create(map_type, storage.get(),
                                  grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  grnxx::StorageNode storage_node = storage->open_node(map->storage_node_id());
  assert(storage_node);
  assert(storage_node.status() == grnxx::STORAGE_NODE_ACTIVE);
}

template <typename T>
void test_map_min_key_id(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map;

  map.reset(grnxx::Map<T>::create(map_type, storage.get(),
                                  grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  assert(map->min_key_id() == grnxx::MAP_MIN_KEY_ID);
  // TODO: Test after add and remove.
}

template <typename T>
void test_map_max_key_id(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map;

  map.reset(grnxx::Map<T>::create(map_type, storage.get(),
                                  grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  assert(map->max_key_id() == (grnxx::MAP_MIN_KEY_ID - 1));
  // TODO: Test after add and remove.
}

template <typename T>
void test_map_next_key_id(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map;

  map.reset(grnxx::Map<T>::create(map_type, storage.get(),
                                  grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  assert(map->next_key_id() == grnxx::MAP_MIN_KEY_ID);
  // TODO: Test after add and remove.
}

template <typename T>
void test_map_num_keys(grnxx::MapType map_type) {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  std::unique_ptr<grnxx::Map<T>> map;

  map.reset(grnxx::Map<T>::create(map_type, storage.get(),
                                  grnxx::STORAGE_ROOT_NODE_ID));
  assert(map);
  assert(map->num_keys() == 0);
  // TODO: Test after add and remove.
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

}  // namespace

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  // Increment the reference count for grnxx::PeriodicClock.
  grnxx::PeriodicClock clock;

  test_map(grnxx::int8_t(), grnxx::uint8_t(),
           grnxx::int16_t(), grnxx::uint16_t(),
           grnxx::int32_t(), grnxx::uint32_t(),
           grnxx::int64_t(), grnxx::uint64_t(),
           grnxx::GeoPoint());

  return 0;
}
