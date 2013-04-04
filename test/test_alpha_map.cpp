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

    compare_maps(map, hash_map);
  }
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

  return 0;
}
