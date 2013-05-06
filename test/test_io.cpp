/*
  Copyright (C) 2012  Brazil, Inc.

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
#include <map>
#include <random>
#include <unordered_map>
#include <unordered_set>

#include "grnxx/io/file.hpp"
#include "grnxx/io/file_info.hpp"
#include "grnxx/io/path.hpp"
#include "grnxx/io/pool.hpp"
#include "grnxx/io/view.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/time/stopwatch.hpp"

namespace {

void test_file_create() {
  const char FILE_PATH[] = "temp.grn";

  grnxx::io::File::unlink_if_exists(FILE_PATH);

  assert(!grnxx::io::File::exists(FILE_PATH));
  assert(!grnxx::io::File::unlink_if_exists(FILE_PATH));

  std::unique_ptr<grnxx::io::File> file(
      grnxx::io::File::open(grnxx::io::FILE_CREATE, FILE_PATH));

  assert(file->path() == FILE_PATH);
  assert(file->tell() == 0);
  assert(file->size() == 0);

  file.reset();

  assert(grnxx::io::File::exists(FILE_PATH));
  grnxx::io::File::unlink(FILE_PATH);

  assert(!grnxx::io::File::exists(FILE_PATH));
  assert(!grnxx::io::File::unlink_if_exists(FILE_PATH));
}

void test_file_open() {
  const char FILE_PATH[] = "temp.grn";

  grnxx::io::File::unlink_if_exists(FILE_PATH);
  std::unique_ptr<grnxx::io::File> file(
      grnxx::io::File::open(grnxx::io::FILE_CREATE, FILE_PATH));
  file.reset();

  file.reset(grnxx::io::File::open(grnxx::io::FILE_OPEN, FILE_PATH));
  file.reset();

  grnxx::io::File::unlink(FILE_PATH);
}

void test_file_create_or_open() {
  const char FILE_PATH[] = "temp.grn";

  grnxx::io::File::unlink_if_exists(FILE_PATH);

  std::unique_ptr<grnxx::io::File> file(
      grnxx::io::File::open(grnxx::io::FILE_CREATE_OR_OPEN, FILE_PATH));
  file.reset();

  file.reset(grnxx::io::File::open(grnxx::io::FILE_CREATE_OR_OPEN, FILE_PATH));
  file.reset();

  grnxx::io::File::unlink(FILE_PATH);
}

void test_file_write() {
  const char FILE_PATH[] = "temp.grn";

  grnxx::io::File::unlink_if_exists(FILE_PATH);
  std::unique_ptr<grnxx::io::File> file(
      grnxx::io::File::open(grnxx::io::FILE_CREATE, FILE_PATH));

  assert(file->write("0123456789", 10) == 10);
  assert(file->tell() == 10);
  assert(file->size() == 10);

  file.reset();
  grnxx::io::File::unlink(FILE_PATH);
}

void test_file_resize() {
  const char FILE_PATH[] = "temp.grn";
  const std::uint64_t FILE_SIZE = 1 << 20;

  grnxx::io::File::unlink_if_exists(FILE_PATH);
  std::unique_ptr<grnxx::io::File> file(
      grnxx::io::File::open(grnxx::io::FILE_CREATE, FILE_PATH));

  file->resize(FILE_SIZE);
  assert(file->tell() == 0);
  assert(file->size() == FILE_SIZE);

  file->resize(0);
  assert(file->tell() == 0);
  assert(file->size() == 0);

  file.reset();
  grnxx::io::File::unlink(FILE_PATH);
}

void test_file_seek() {
  const char FILE_PATH[] = "temp.grn";
  const std::uint64_t FILE_SIZE = 1 << 20;

  grnxx::io::File::unlink_if_exists(FILE_PATH);
  std::unique_ptr<grnxx::io::File> file(
      grnxx::io::File::open(grnxx::io::FILE_CREATE, FILE_PATH));

  file->resize(FILE_SIZE);

  assert(file->seek(0) == 0);
  assert(file->tell() == 0);

  assert(file->seek(FILE_SIZE / 2) == (FILE_SIZE / 2));
  assert(file->tell() == (FILE_SIZE / 2));

  assert(file->seek(FILE_SIZE / 4, SEEK_CUR) ==
         ((FILE_SIZE / 2) + (FILE_SIZE / 4)));
  assert(file->tell() == ((FILE_SIZE / 2) + (FILE_SIZE / 4)));

  assert(file->seek(-(FILE_SIZE / 2), SEEK_END) == (FILE_SIZE / 2));
  assert(file->tell() == (FILE_SIZE / 2));

  file.reset();
  grnxx::io::File::unlink(FILE_PATH);
}

void test_file_read() {
  const char FILE_PATH[] = "temp.grn";

  grnxx::io::File::unlink_if_exists(FILE_PATH);
  std::unique_ptr<grnxx::io::File> file(
      grnxx::io::File::open(grnxx::io::FILE_CREATE, FILE_PATH));

  file->write("0123456789", 10);
  file->seek(0);

  char buf[256];
  assert(file->read(buf, sizeof(buf)) == 10);
  assert(std::memcmp(buf, "0123456789", 10) == 0);
  assert(file->tell() == 10);

  file->seek(3);

  assert(file->read(buf, 5) == 5);
  assert(file->tell() == 8);
  assert(std::memcmp(buf, "34567", 5) == 0);

  file.reset();
  grnxx::io::File::unlink(FILE_PATH);
}

void test_file_temporary() {
  const char FILE_PATH[] = "temp.grn";

  std::unique_ptr<grnxx::io::File> file(
      grnxx::io::File::open(grnxx::io::FILE_TEMPORARY, FILE_PATH));

  assert(file->write("0123456789", 10) == 10);
  assert(file->seek(0) == 0);

  char buf[256];
  assert(file->read(buf, sizeof(buf)) == 10);
  assert(std::memcmp(buf, "0123456789", 10) == 0);

  grnxx::String path = file->path();

  file.reset();
  assert(!grnxx::io::File::exists(path.c_str()));
}

void test_file_unlink_at_close() {
  const char FILE_PATH[] = "temp.grn";

  std::unique_ptr<grnxx::io::File> file(
      grnxx::io::File::open(grnxx::io::FILE_CREATE, FILE_PATH));

  file->set_unlink_at_close(true);

  assert(file->unlink_at_close());

  file.reset();
  assert(!grnxx::io::File::exists(FILE_PATH));
}

void test_file_lock() {
  const char FILE_PATH[] = "temp.grn";

  grnxx::io::File::unlink_if_exists(FILE_PATH);
  std::unique_ptr<grnxx::io::File> file_1(
      grnxx::io::File::open(grnxx::io::FILE_CREATE, FILE_PATH));

  assert(!file_1->unlock());
  assert(file_1->try_lock(grnxx::io::FILE_LOCK_EXCLUSIVE));
  assert(!file_1->try_lock(grnxx::io::FILE_LOCK_SHARED));
  assert(file_1->unlock());

  assert(file_1->try_lock(grnxx::io::FILE_LOCK_SHARED));
  assert(file_1->unlock());
  assert(!file_1->unlock());

  std::unique_ptr<grnxx::io::File> file_2(
      grnxx::io::File::open(grnxx::io::FILE_OPEN, FILE_PATH));

  assert(file_1->try_lock(grnxx::io::FILE_LOCK_EXCLUSIVE));
  assert(!file_2->try_lock(grnxx::io::FILE_LOCK_SHARED));
  assert(!file_2->try_lock(grnxx::io::FILE_LOCK_EXCLUSIVE));
  assert(!file_2->unlock());
  assert(file_1->unlock());

  assert(file_1->try_lock(grnxx::io::FILE_LOCK_SHARED));
  assert(!file_2->try_lock(grnxx::io::FILE_LOCK_EXCLUSIVE));
  assert(file_2->try_lock(grnxx::io::FILE_LOCK_SHARED));
  assert(file_1->unlock());
  assert(!file_1->try_lock(grnxx::io::FILE_LOCK_EXCLUSIVE));
  assert(file_2->unlock());

  file_1.reset();
  file_2.reset();
  grnxx::io::File::unlink(FILE_PATH);
}

void test_file_info_non_existent_file() {
  const char FILE_PATH[] = "temp.grn";

  grnxx::io::File::unlink_if_exists(FILE_PATH);

  std::unique_ptr<grnxx::io::FileInfo> file_info(
      grnxx::io::FileInfo::stat(FILE_PATH));
  assert(!file_info);
}

void test_file_info_existent_file() {
  const char FILE_PATH[] = "temp.grn";
  const std::uint64_t FILE_SIZE = 12345;

  grnxx::io::File::unlink_if_exists(FILE_PATH);
  std::unique_ptr<grnxx::io::File> file(
      grnxx::io::File::open(grnxx::io::FILE_CREATE, FILE_PATH));
  file->resize(FILE_SIZE);

  std::unique_ptr<grnxx::io::FileInfo> file_info(
      grnxx::io::FileInfo::stat(FILE_PATH));
  assert(file_info);

  GRNXX_NOTICE() << "file_info (regular) = " << *file_info;

  assert(file_info->is_file());
  assert(!file_info->is_directory());
  assert(file_info->size() == FILE_SIZE);

  file_info.reset(grnxx::io::FileInfo::stat(file.get()));
  assert(file_info);

  GRNXX_NOTICE() << "file_info (regular) = " << *file_info;

  file.reset();
  grnxx::io::File::unlink(FILE_PATH);
}

void test_file_info_non_existent_directory() {
  const char DIRECTORY_PATH[] = "no_such_directory/";

  std::unique_ptr<grnxx::io::FileInfo> file_info(
    grnxx::io::FileInfo::stat(DIRECTORY_PATH));
  assert(!file_info);
}

void test_file_info_existent_directory() {
  const char DIRECTORY_PATH[] = "./";

  std::unique_ptr<grnxx::io::FileInfo> file_info(
    grnxx::io::FileInfo::stat(DIRECTORY_PATH));
  assert(file_info);

  GRNXX_NOTICE() << "file_info (directory) = " << *file_info;

  assert(!file_info->is_file());
  assert(file_info->is_directory());
}

void test_pool_constructor() {
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

void test_pool_compare() {
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

void test_pool_copy() {
  grnxx::io::Pool pool(grnxx::io::POOL_TEMPORARY, "temp.grn");

  grnxx::io::Pool pool2(pool);
  assert(pool == pool2);

  grnxx::io::Pool pool3;
  pool3 = pool;
  assert(pool == pool3);
}

void test_pool_move() {
  grnxx::io::Pool pool(grnxx::io::POOL_TEMPORARY, "temp.grn");
  grnxx::io::Pool pool_copy(pool);

  grnxx::io::Pool pool2(std::move(pool));
  assert(pool2 == pool_copy);

  grnxx::io::Pool pool3;
  pool3 = std::move(pool2);
  assert(pool3 == pool_copy);
}

void test_pool_swap() {
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

void test_pool_exists() {
  grnxx::io::Pool::unlink_if_exists("temp.grn");

  assert(!grnxx::io::Pool::exists("temp.grn"));

  grnxx::io::Pool(grnxx::io::POOL_CREATE, "temp.grn");

  assert(grnxx::io::Pool::exists("temp.grn"));

  grnxx::io::Pool::unlink("temp.grn");
}

void test_pool_unlink() {
  grnxx::io::Pool::unlink_if_exists("temp.grn");

  grnxx::io::Pool(grnxx::io::POOL_CREATE, "temp.grn");

  grnxx::io::Pool::unlink("temp.grn");
}

void test_pool_unlink_if_exists() {
  grnxx::io::Pool::unlink_if_exists("temp.grn");

  grnxx::io::Pool(grnxx::io::POOL_CREATE, "temp.grn");

  assert(grnxx::io::Pool::unlink_if_exists("temp.grn"));
}

void test_pool_write_to() {
  grnxx::io::Pool pool(grnxx::io::POOL_TEMPORARY, "temp.grn");

  GRNXX_NOTICE() << "pool = " << pool;
}

void test_pool_create_block() {
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

void test_pool_get_block_info() {
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

void test_pool_get_block_address() {
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

void test_pool_free_block() {
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

void test_pool_unfreeze_block() {
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

void test_pool_random_queries() {
  // Enable immediate reuse of freed blocks.
  grnxx::io::PoolOptions options;
  options.set_frozen_duration(grnxx::Duration(0));

  grnxx::io::Pool pool(grnxx::io::POOL_ANONYMOUS, "temp.grn", options);

  const int OPERATION_COUNT = 1 << 18;

  std::mt19937 random;
  std::unordered_set<std::uint32_t> id_set;

  // Create and free blocks in random.
  for (int i = 0; i < OPERATION_COUNT; ++i) {
    const std::uint32_t value = random() & 255;
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

void test_pool_benchmark() {
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

void test_view_anonymous_mmap() {
  const std::uint64_t MMAP_SIZE = 1 << 20;

  // Create an anonymous memory mapping.
  std::unique_ptr<grnxx::io::View> view(
      grnxx::io::View::open(grnxx::io::ViewFlags::none(), MMAP_SIZE));
  assert(view);

  GRNXX_NOTICE() << "view = " << *view;

  // Check members of the view.
  assert(view->flags() == (grnxx::io::VIEW_ANONYMOUS |
                          grnxx::io::VIEW_PRIVATE));
  assert(view->address() != nullptr);
  assert(view->size() == MMAP_SIZE);

  // Fill the mapping with 0.
  std::memset(view->address(), 0, view->size());
}

void test_view_file_backed_mmap() {
  const char FILE_PATH[] = "temp.grn";
  const std::uint64_t FILE_SIZE = 1 << 24;
  const std::uint64_t MMAP_SIZE = 1 << 20;

  // Create a file of "FILE_SIZE" bytes.
  std::unique_ptr<grnxx::io::File> file(
      grnxx::io::File::open(grnxx::io::FILE_TEMPORARY, FILE_PATH));
  file->resize(FILE_SIZE);
  assert(file->size() == FILE_SIZE);

  // Create a memory mapping on "file".
  std::unique_ptr<grnxx::io::View> view(
      grnxx::io::View::open(grnxx::io::VIEW_SHARED, file.get()));
  assert(view);

  GRNXX_NOTICE() << "view = " << *view;

  assert(view->flags() == grnxx::io::VIEW_SHARED);
  assert(view->address() != nullptr);
  assert(view->size() == FILE_SIZE);

  std::memset(view->address(), 'x', view->size());

  // Recreate a memory mapping on "file".
  view.reset();
  view.reset(grnxx::io::View::open(grnxx::io::VIEW_PRIVATE, file.get()));
  assert(view);

  GRNXX_NOTICE() << "view = " << *view;

  assert(view->flags() == grnxx::io::VIEW_PRIVATE);
  assert(view->address() != nullptr);
  assert(view->size() == FILE_SIZE);

  for (std::uint64_t i = 0; i < FILE_SIZE; ++i) {
    assert(static_cast<const char *>(view->address())[i] == 'x');
  }
  std::memset(view->address(), 'z', view->size());

  // Create a memory mapping on a part of "file".
  view.reset();
  view.reset(grnxx::io::View::open(grnxx::io::VIEW_SHARED |
                                   grnxx::io::VIEW_PRIVATE,
                                   file.get(), FILE_SIZE / 2, MMAP_SIZE));
  assert(view);

  GRNXX_NOTICE() << "view = " << *view;

  assert(view->flags() == grnxx::io::VIEW_SHARED);
  assert(view->address() != nullptr);
  assert(view->size() == MMAP_SIZE);

  for (std::uint64_t i = 0; i < MMAP_SIZE; ++i) {
    assert(static_cast<const char *>(view->address())[i] == 'x');
  }
}

void test_file() {
  test_file_create();
  test_file_open();
  test_file_create_or_open();
  test_file_write();
  test_file_resize();
  test_file_seek();
  test_file_read();
  test_file_temporary();
  test_file_unlink_at_close();
  test_file_lock();
}

void test_file_info() {
  test_file_info_non_existent_file();
  test_file_info_existent_file();
  test_file_info_non_existent_directory();
  test_file_info_existent_directory();
}

void test_path() {
  grnxx::String full_path = grnxx::io::Path::full_path(nullptr);
  GRNXX_NOTICE() << "full_path = " << full_path;

  full_path = grnxx::io::Path::full_path("temp.grn");
  GRNXX_NOTICE() << "full_path = " << full_path;

  assert(grnxx::io::Path::full_path("/") == "/");
  assert(grnxx::io::Path::full_path("/.") == "/");
  assert(grnxx::io::Path::full_path("/..") == "/");

  assert(grnxx::io::Path::full_path("/usr/local/lib") == "/usr/local/lib");
  assert(grnxx::io::Path::full_path("/usr/local/lib/") == "/usr/local/lib/");
  assert(grnxx::io::Path::full_path("/usr/local/lib/.") == "/usr/local/lib");
  assert(grnxx::io::Path::full_path("/usr/local/lib/./") == "/usr/local/lib/");

  assert(grnxx::io::Path::full_path("/usr/local/lib/..") == "/usr/local");
  assert(grnxx::io::Path::full_path("/usr/local/lib/../") == "/usr/local/");

  grnxx::String unique_path = grnxx::io::Path::unique_path("temp.grn");
  GRNXX_NOTICE() << "unique_path = " << unique_path;

  unique_path = grnxx::io::Path::unique_path(nullptr);
  GRNXX_NOTICE() << "unique_path = " << unique_path;
}

void test_pool() {
  test_pool_constructor();
  test_pool_compare();
  test_pool_copy();
  test_pool_move();
  test_pool_swap();
  test_pool_exists();
  test_pool_unlink();
  test_pool_unlink_if_exists();
  test_pool_write_to();
  test_pool_create_block();
  test_pool_get_block_info();
  test_pool_get_block_address();
  test_pool_free_block();
  test_pool_unfreeze_block();
  test_pool_random_queries();
  test_pool_benchmark();
}

void test_view() {
  test_view_anonymous_mmap();
  test_view_file_backed_mmap();
}

}  // namespace

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_file();
  test_file_info();
  test_path();
  test_pool();
  test_view();

  return 0;
}
