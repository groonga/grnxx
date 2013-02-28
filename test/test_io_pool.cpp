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
#include <random>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "io/pool.hpp"
#include "logger.hpp"
#include "stopwatch.hpp"

void test_constructor() {
  grnxx::io::Pool::unlink_if_exists("temp.grn");

  grnxx::io::Pool pool;
  assert(!pool);

  pool.open(grnxx::io::POOL_CREATE, "temp.grn");
  assert(pool);
  assert(pool.flags() & grnxx::io::POOL_CREATE);

  pool.open(grnxx::io::PoolFlags::none(), "temp.grn");
  assert(pool);
  assert(pool.flags() & grnxx::io::POOL_OPEN);

  pool.open(grnxx::io::POOL_ANONYMOUS, "temp.grn");
  assert(pool);
  assert(pool.flags() & grnxx::io::POOL_ANONYMOUS);

  pool.open(grnxx::io::POOL_TEMPORARY, "temp.grn");
  assert(pool);
  assert(pool.flags() & grnxx::io::POOL_TEMPORARY);

  grnxx::io::Pool::unlink_if_exists("temp.grn");
}

void test_compare() {
  grnxx::io::Pool pool;
  assert(pool == pool);

  grnxx::io::Pool pool2(grnxx::io::POOL_TEMPORARY, "temp.grn");
  assert(pool != pool2);
  assert(pool2 == pool2);

  grnxx::io::Pool pool3(grnxx::io::POOL_TEMPORARY, "temp.grn");
  assert(pool != pool3);
  assert(pool2 != pool3);
  assert(pool3 == pool3);
}

void test_copy() {
  grnxx::io::Pool pool(grnxx::io::POOL_TEMPORARY, "temp.grn");

  grnxx::io::Pool pool2(pool);
  assert(pool == pool2);

  grnxx::io::Pool pool3;
  pool3 = pool;
  assert(pool == pool3);
}

void test_move() {
  grnxx::io::Pool pool(grnxx::io::POOL_TEMPORARY, "temp.grn");
  grnxx::io::Pool pool_copy(pool);

  grnxx::io::Pool pool2(std::move(pool));
  assert(pool2 == pool_copy);

  grnxx::io::Pool pool3;
  pool3 = std::move(pool2);
  assert(pool3 == pool_copy);
}

void test_swap() {
  grnxx::io::Pool pool(grnxx::io::POOL_TEMPORARY, "temp.grn");
  grnxx::io::Pool pool2(grnxx::io::POOL_TEMPORARY, "temp.grn");

  grnxx::io::Pool pool_copy(pool);
  grnxx::io::Pool pool2_copy(pool2);

  pool.swap(pool2);
  assert(pool == pool2_copy);
  assert(pool2 == pool_copy);

  swap(pool, pool2);
  assert(pool == pool_copy);
  assert(pool2 == pool2_copy);
}

void test_exists() {
  grnxx::io::Pool::unlink_if_exists("temp.grn");

  assert(!grnxx::io::Pool::exists("temp.grn"));

  grnxx::io::Pool(grnxx::io::POOL_CREATE, "temp.grn");

  assert(grnxx::io::Pool::exists("temp.grn"));

  grnxx::io::Pool::unlink("temp.grn");
}

void test_unlink() {
  grnxx::io::Pool::unlink_if_exists("temp.grn");

  grnxx::io::Pool(grnxx::io::POOL_CREATE, "temp.grn");

  grnxx::io::Pool::unlink("temp.grn");
}

void test_unlink_if_exists() {
  grnxx::io::Pool::unlink_if_exists("temp.grn");

  grnxx::io::Pool(grnxx::io::POOL_CREATE, "temp.grn");

  assert(grnxx::io::Pool::unlink_if_exists("temp.grn"));
}

void test_write_to() {
  grnxx::io::Pool pool(grnxx::io::POOL_TEMPORARY, "temp.grn");

  GRNXX_NOTICE() << "pool = " << pool;
}

void test_create_block() {
  grnxx::io::Pool pool(grnxx::io::POOL_ANONYMOUS, "temp.grn");

  // Create a minimum-size block.
  const grnxx::io::BlockInfo *block_info = pool.create_block(0);
  assert(block_info);
  assert(block_info->id() == 0);
  assert(block_info->status() == grnxx::io::BLOCK_ACTIVE);
  assert(block_info->chunk_id() == 0);
  assert(block_info->offset() == 0);
  assert(block_info->size() == grnxx::io::BLOCK_UNIT_SIZE);

  pool.open(grnxx::io::POOL_TEMPORARY, "temp.grn");

  // Create a maximum-size block.
  block_info = pool.create_block(pool.options().max_block_chunk_size());
  assert(block_info);
  assert(block_info->id() == 0);
  assert(block_info->status() == grnxx::io::BLOCK_ACTIVE);
  assert(block_info->chunk_id() == 0);
  assert(block_info->offset() == 0);
  assert(block_info->size() == pool.options().max_block_chunk_size());

  const int NUM_BLOCKS = 1 << 16;

  std::mt19937 random;

  pool.open(grnxx::io::POOL_TEMPORARY, "temp.grn");

  // Create small blocks.
  const std::uint64_t SMALL_MAX_SIZE = std::uint64_t(1) << 16;
  for (int i = 0; i < NUM_BLOCKS; ++i) {
    pool.create_block(random() % SMALL_MAX_SIZE);
  }

  // Create medium blocks.
  const std::uint64_t MEDIUM_MAX_SIZE = std::uint64_t(1) << 22;
  for (int i = 0; i < NUM_BLOCKS; ++i) {
    pool.create_block(random() % MEDIUM_MAX_SIZE);
  }

  // Create large blocks.
  const std::uint64_t LARGE_MAX_SIZE = std::uint64_t(1) << 28;
  for (int i = 0; i < NUM_BLOCKS; ++i) {
    pool.create_block(random() % LARGE_MAX_SIZE);
  }
}

void test_get_block_info() {
  grnxx::io::Pool pool(grnxx::io::POOL_ANONYMOUS, "temp.grn");

  const grnxx::io::BlockInfo *block_info;

  block_info = pool.create_block(std::uint64_t(1) << 10);
  assert(block_info == pool.get_block_info(block_info->id()));

  block_info = pool.create_block(std::uint64_t(1) << 20);
  assert(block_info == pool.get_block_info(block_info->id()));

  block_info = pool.create_block(std::uint64_t(1) << 30);
  assert(block_info == pool.get_block_info(block_info->id()));

  block_info = pool.create_block(std::uint64_t(1) << 40);
  assert(block_info == pool.get_block_info(block_info->id()));
}

void test_get_block_address() {
  grnxx::io::Pool pool(grnxx::io::POOL_ANONYMOUS, "temp.grn");

  const int NUM_BLOCKS = 1 << 10;
  const std::uint32_t MAX_SIZE = 1 << 16;

  std::mt19937 random;
  std::unordered_map<const grnxx::io::BlockInfo *, int> map;

  for (int i = 0; i < NUM_BLOCKS; ++i) {
    // Create a block and fill it with a random alphabet.
    const grnxx::io::BlockInfo *block_info =
        pool.create_block(random() % MAX_SIZE);
    void *block_address = pool.get_block_address(*block_info);

    const int label = 'A' + (random() % 26);
    std::memset(block_address, label, block_info->size());
    map[block_info] = label;
  }

  for (auto it = map.begin(); it != map.end(); ++it) {
    // Check the blocks are not broken.
    const char *block_address = static_cast<char *>(
        pool.get_block_address(it->first->id()));
    for (std::uint64_t j = 0; j < it->first->size(); ++j) {
      assert(block_address[j] == it->second);
    }
  }
}

void test_free_block() {
  grnxx::io::Pool pool(grnxx::io::POOL_ANONYMOUS, "temp.grn");

  const grnxx::io::BlockInfo *block_info = pool.create_block(0);
  pool.free_block(block_info->id());
  assert(block_info->status() == grnxx::io::BLOCK_FROZEN);

  block_info = pool.create_block(1 << 20);
  pool.free_block(*block_info);
  assert(block_info->status() == grnxx::io::BLOCK_FROZEN);

  const int NUM_BLOCKS = 1 << 16;

  std::mt19937 random;
  std::vector<const grnxx::io::BlockInfo *> block_infos;

  pool.open(grnxx::io::POOL_TEMPORARY, "temp.grn");

  // Create small blocks.
  const std::uint64_t SMALL_MAX_SIZE = std::uint64_t(1) << 16;
  for (int i = 0; i < NUM_BLOCKS; ++i) {
    block_infos.push_back(pool.create_block(random() % SMALL_MAX_SIZE));
  }

  // Create medium blocks.
  const std::uint64_t MEDIUM_MAX_SIZE = std::uint64_t(1) << 22;
  for (int i = 0; i < NUM_BLOCKS; ++i) {
    block_infos.push_back(pool.create_block(random() % MEDIUM_MAX_SIZE));
  }

  // Create large blocks.
  const std::uint64_t LARGE_MAX_SIZE = std::uint64_t(1) << 28;
  for (int i = 0; i < NUM_BLOCKS; ++i) {
    block_infos.push_back(pool.create_block(random() % LARGE_MAX_SIZE));
  }

  for (std::size_t i = 0; i < block_infos.size(); ++i) {
    assert(block_infos[i]->status() == grnxx::io::BLOCK_ACTIVE);
    pool.free_block(*block_infos[i]);
    assert(block_infos[i]->status() == grnxx::io::BLOCK_FROZEN);
  }
}

void test_unfreeze_block() {
  // Enable immediate reuse of freed blocks.
  grnxx::io::PoolOptions options;
  options.set_frozen_duration(grnxx::Duration(0));

  grnxx::io::Pool pool(grnxx::io::POOL_TEMPORARY, "temp.grn", options);
  assert(pool.options().frozen_duration() == grnxx::Duration(0));

  const grnxx::io::BlockInfo *block_info = pool.create_block(0);
  pool.free_block(*block_info);

  // The freed ID is not available yet.
  block_info = pool.create_block(0);
  assert(block_info->id() != 0);

  const int OPERATION_COUNT = 1 << 16;

  std::mt19937 random;
  std::unordered_set<const grnxx::io::BlockInfo *> block_infos;

  const std::uint64_t MAX_SIZE = std::uint64_t(1) << 32;
  for (int i = 0; i < OPERATION_COUNT; ++i) {
    if (!block_infos.empty() && ((random() % 2) == 0)) {
      pool.free_block(**block_infos.begin());
      block_infos.erase(block_infos.begin());
    } else {
      block_infos.insert(pool.create_block(random() % MAX_SIZE));
    }
  }

  // The total_size may be greater than 100TB if the block reuse does not work.
  GRNXX_NOTICE() << "total_size = " << pool.header().total_size();
  assert(pool.header().total_size() < (std::uint64_t(1) << 42));
}

void test_random_queries() {
  // Enable immediate reuse of freed blocks.
  grnxx::io::PoolOptions options;
  options.set_frozen_duration(grnxx::Duration(0));

  grnxx::io::Pool pool(grnxx::io::POOL_ANONYMOUS, "temp.grn", options);

  const int OPERATION_COUNT = 1 << 18;

  std::mt19937 random;
  std::unordered_set<std::uint32_t> id_set;

  // Create and free blocks in random.
  for (int i = 0; i < OPERATION_COUNT; ++i) {
    const std::uint32_t value = random() & 256;
    if (value < 16) {
      // Free a block.
      if (!id_set.empty()) {
        pool.free_block(*id_set.begin());
        id_set.erase(id_set.begin());
      }
    } else {
      std::uint64_t size;
      if (value < 32) {
        // Create a small block.
        const std::uint64_t SMALL_MAX_SIZE = std::uint64_t(1) << 16;
        size = random() % SMALL_MAX_SIZE;
      } else if (value < 248) {
        // Create a medium block.
        const std::uint64_t MEDIUM_MAX_SIZE = std::uint64_t(1) << 22;
        size = random() % MEDIUM_MAX_SIZE;
      } else {
        // Create a large block.
        const std::uint64_t LARGE_MAX_SIZE = std::uint64_t(1) << 28;
        size = random() % LARGE_MAX_SIZE;
      }
      const grnxx::io::BlockInfo *block_info = pool.create_block(size);
      id_set.insert(block_info->id());
    }
  }
}

void benchmark() {
  const int OPERATION_COUNT = 1 << 16;

  std::vector<const grnxx::io::BlockInfo *> block_infos;
  block_infos.resize(OPERATION_COUNT);

  grnxx::io::Pool pool(grnxx::io::POOL_TEMPORARY, "temp.grn");

  // Measure the speed of create_block().
  grnxx::Stopwatch stopwatch(true);
  for (int i = 0; i < OPERATION_COUNT; ++i) {
    block_infos[i] = pool.create_block(0);
  }
  grnxx::Duration elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "create_block: elapsed [ns] = "
                 << (1000.0 * elapsed.count() / OPERATION_COUNT);

  // Measure the speed of get_block_info().
  stopwatch.reset();
  for (int i = 0; i < OPERATION_COUNT; ++i) {
    pool.get_block_info(block_infos[i]->id());
  }
  elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "get_block_info: elapsed [ns] = "
                 << (1000.0 * elapsed.count() / OPERATION_COUNT);

  // Measure the speed of get_block_address().
  stopwatch.reset();
  for (int i = 0; i < OPERATION_COUNT; ++i) {
    pool.get_block_address(*block_infos[i]);
  }
  elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "get_block_address_by_info (1st): elapsed [ns] = "
                 << (1000.0 * elapsed.count() / OPERATION_COUNT);

  // Measure the speed of get_block_address() again.
  stopwatch.reset();
  for (int i = 0; i < OPERATION_COUNT; ++i) {
    pool.get_block_address(*block_infos[i]);
  }
  elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "get_block_address_by_info (2nd): elapsed [ns] = "
                 << (1000.0 * elapsed.count() / OPERATION_COUNT);

  // Measure the speed of get_block_address() again and again.
  stopwatch.reset();
  for (int i = 0; i < OPERATION_COUNT; ++i) {
    pool.get_block_address(block_infos[i]->id());
  }
  elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "get_block_address_by_id: elapsed [ns] = "
                 << (1000.0 * elapsed.count() / OPERATION_COUNT);

  // Measure the speed of free_block().
  stopwatch.reset();
  for (int i = 0; i < OPERATION_COUNT; ++i) {
    pool.free_block(block_infos[i]->id());
  }
  elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "free_block: elapsed [ns] = "
                 << (1000.0 * elapsed.count() / OPERATION_COUNT);
}

int main() {
  // Enables logging to the standard output.
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_constructor();
  test_compare();
  test_copy();
  test_move();
  test_swap();
  test_exists();
  test_unlink();
  test_unlink_if_exists();
  test_write_to();
  test_create_block();
  test_get_block_info();
  test_get_block_address();
  test_free_block();
  test_unfreeze_block();
  test_random_queries();
  benchmark();

  return 0;
}
