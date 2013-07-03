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
#include <cstring>
#include <memory>
#include <random>
#include <unordered_set>
#include <vector>

#include "grnxx/logger.hpp"
#include "grnxx/storage.hpp"
#include "grnxx/storage/file.hpp"
#include "grnxx/storage/path.hpp"
#include "grnxx/storage/chunk.hpp"
#include "grnxx/periodic_clock.hpp"
#include "grnxx/types.hpp"

namespace {

void test_full_path(const char *path, const char *answer) {
  std::unique_ptr<char[]> full_path(grnxx::storage::Path::full_path(path));
  assert(std::strcmp(full_path.get(), answer) == 0);
}

void test_full_path() {
  std::unique_ptr<char[]> full_path(grnxx::storage::Path::full_path(nullptr));
  GRNXX_NOTICE() << "full_path = " << full_path.get();

  full_path.reset(grnxx::storage::Path::full_path("temp.grn"));
  GRNXX_NOTICE() << "full_path = " << full_path.get();

  test_full_path("/", "/");
  test_full_path("/.", "/");
  test_full_path("/..", "/");

  test_full_path("/usr/local/lib", "/usr/local/lib");
  test_full_path("/usr/local/lib/", "/usr/local/lib/");
  test_full_path("/usr/local/lib/.", "/usr/local/lib");
  test_full_path("/usr/local/lib/./", "/usr/local/lib/");
  test_full_path("/usr/local/lib/..", "/usr/local");
  test_full_path("/usr/local/lib/../", "/usr/local/");
}

void test_unique_path() {
  std::unique_ptr<char[]> unique_path(
      grnxx::storage::Path::unique_path(nullptr));
  GRNXX_NOTICE() << "unique_path = " << unique_path.get();

  unique_path.reset(grnxx::storage::Path::unique_path("temp.grn"));
  GRNXX_NOTICE() << "unique_path = " << unique_path.get();
}

void test_file_create() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::storage::File::unlink(FILE_PATH);
  std::unique_ptr<grnxx::storage::File> file;

  file.reset(grnxx::storage::File::create(FILE_PATH));

  file.reset(grnxx::storage::File::create(FILE_PATH,
                                          grnxx::storage::FILE_TEMPORARY));
  file.reset(grnxx::storage::File::create(FILE_PATH,
                                          grnxx::storage::FILE_TEMPORARY));

  file.reset(grnxx::storage::File::create(nullptr));
  file.reset(grnxx::storage::File::create(nullptr));

  grnxx::storage::File::unlink(FILE_PATH);
}

void test_file_open() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::storage::File::unlink(FILE_PATH);
  std::unique_ptr<grnxx::storage::File> file;

  file.reset(grnxx::storage::File::create(FILE_PATH));
  file.reset(grnxx::storage::File::open(FILE_PATH));

  file.reset();
  grnxx::storage::File::unlink(FILE_PATH);
}

void test_file_open_or_create() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::storage::File::unlink(FILE_PATH);
  std::unique_ptr<grnxx::storage::File> file;

  file.reset(grnxx::storage::File::open_or_create(FILE_PATH));
  file.reset(grnxx::storage::File::open_or_create(FILE_PATH));

  file.reset();
  grnxx::storage::File::unlink(FILE_PATH);
}

void test_file_exists_and_unlink() {
  const char FILE_PATH[] = "temp.grn";
  std::unique_ptr<grnxx::storage::File>(
      grnxx::storage::File::open_or_create(FILE_PATH));

  assert(grnxx::storage::File::exists(FILE_PATH));
  assert(grnxx::storage::File::unlink(FILE_PATH));
  assert(!grnxx::storage::File::exists(FILE_PATH));
  assert(!grnxx::storage::File::unlink(FILE_PATH));
}

void test_file_lock_and_unlock() {
  const char FILE_PATH[] = "temp.grn";
  std::unique_ptr<grnxx::storage::File> file_1;
  file_1.reset(grnxx::storage::File::open_or_create(FILE_PATH));

  assert(file_1->lock(grnxx::storage::FILE_LOCK_SHARED));
  file_1->unlock();

  assert(file_1->lock(grnxx::storage::FILE_LOCK_EXCLUSIVE));
  file_1->unlock();

  std::unique_ptr<grnxx::storage::File> file_2;
  file_2.reset(grnxx::storage::File::open(FILE_PATH));

  assert(file_1->lock(grnxx::storage::FILE_LOCK_SHARED));
  assert(file_2->lock(grnxx::storage::FILE_LOCK_SHARED |
                      grnxx::storage::FILE_LOCK_NONBLOCKING));
  file_2->unlock();
  assert(!file_2->lock(grnxx::storage::FILE_LOCK_EXCLUSIVE |
                       grnxx::storage::FILE_LOCK_NONBLOCKING));
  file_1->unlock();

  assert(file_1->lock(grnxx::storage::FILE_LOCK_EXCLUSIVE));
  assert(!file_2->lock(grnxx::storage::FILE_LOCK_SHARED |
                       grnxx::storage::FILE_LOCK_NONBLOCKING));
  assert(!file_2->lock(grnxx::storage::FILE_LOCK_EXCLUSIVE |
                       grnxx::storage::FILE_LOCK_NONBLOCKING));
  file_1->unlock();

  file_1.reset();
  file_2.reset();
  grnxx::storage::File::unlink(FILE_PATH);
}

void test_file_sync() {
  std::unique_ptr<grnxx::storage::File> file(
      grnxx::storage::File::create(nullptr));

  file->sync();
}

void test_file_resize_and_size() {
  std::unique_ptr<grnxx::storage::File> file(
      grnxx::storage::File::create(nullptr));

  assert(file->get_size() == 0);
  file->resize(65536);
  assert(file->get_size() == 65536);
  file->resize(1024);
  assert(file->get_size() == 1024);
}

void test_file_path() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::storage::File::unlink(FILE_PATH);
  std::unique_ptr<grnxx::storage::File> file;

  file.reset(grnxx::storage::File::create(FILE_PATH));
  assert(std::strcmp(file->path(), FILE_PATH) == 0);

  file.reset(grnxx::storage::File::create(FILE_PATH,
                                          grnxx::storage::FILE_TEMPORARY));
  assert(std::strcmp(file->path(), FILE_PATH) != 0);

  assert(grnxx::storage::File::unlink(FILE_PATH));
}

void test_file_flags() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::storage::File::unlink(FILE_PATH);
  std::unique_ptr<grnxx::storage::File> file;

  file.reset(grnxx::storage::File::create(FILE_PATH));
  assert(file->flags() == grnxx::storage::FILE_DEFAULT);

  file.reset(grnxx::storage::File::open(FILE_PATH,
                                        grnxx::storage::FILE_READ_ONLY));
  assert(file->flags() == grnxx::storage::FILE_READ_ONLY);

  file.reset(grnxx::storage::File::create(FILE_PATH,
                                          grnxx::storage::FILE_TEMPORARY));
  assert(file->flags() == grnxx::storage::FILE_TEMPORARY);

  assert(grnxx::storage::File::unlink(FILE_PATH));
}

void test_file_handle() {
  std::unique_ptr<grnxx::storage::File> file(
      grnxx::storage::File::create(nullptr));

  assert(file->handle());
}

void test_chunk_create() {
  constexpr std::uint64_t FILE_SIZE = 1 << 20;
  std::unique_ptr<grnxx::storage::File> file;
  std::unique_ptr<grnxx::storage::Chunk> chunk;

  file.reset(grnxx::storage::File::create(nullptr));
  file->resize(FILE_SIZE);

  chunk.reset(grnxx::storage::Chunk::create(file.get()));
  chunk.reset(grnxx::storage::Chunk::create(file.get(), 0));
  chunk.reset(grnxx::storage::Chunk::create(file.get(), 0, 0));
  chunk.reset(grnxx::storage::Chunk::create(file.get(), 0, FILE_SIZE));
  chunk.reset(grnxx::storage::Chunk::create(file.get(), 0, 10));

  chunk.reset(grnxx::storage::Chunk::create(nullptr, 0, 1 << 20));
}

void test_chunk_sync() {
  constexpr std::uint64_t FILE_SIZE = 1 << 20;
  std::unique_ptr<grnxx::storage::File> file;
  std::unique_ptr<grnxx::storage::Chunk> chunk;

  file.reset(grnxx::storage::File::create(nullptr));
  file->resize(FILE_SIZE);

  chunk.reset(grnxx::storage::Chunk::create(file.get()));
  chunk->sync();
  chunk->sync(0);
  chunk->sync(0, 0);
  chunk->sync(0, FILE_SIZE);
}

void test_chunk_flags() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::storage::File::unlink(FILE_PATH);
  std::unique_ptr<grnxx::storage::File> file;
  std::unique_ptr<grnxx::storage::Chunk> chunk;

  file.reset(grnxx::storage::File::create(FILE_PATH));
  file->resize(1 << 20);

  chunk.reset(grnxx::storage::Chunk::create(file.get()));
  assert(chunk->flags() == grnxx::storage::CHUNK_DEFAULT);

  file.reset(grnxx::storage::File::open(FILE_PATH,
                                        grnxx::storage::FILE_READ_ONLY));

  chunk.reset(grnxx::storage::Chunk::create(file.get()));
  assert(chunk->flags() == grnxx::storage::CHUNK_READ_ONLY);

  file.reset();
  assert(grnxx::storage::File::unlink(FILE_PATH));
}

void test_chunk_address() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::storage::File::unlink(FILE_PATH);
  std::unique_ptr<grnxx::storage::File> file;
  std::unique_ptr<grnxx::storage::Chunk> chunk;

  file.reset(grnxx::storage::File::create(nullptr));
  file->resize(10);

  chunk.reset(grnxx::storage::Chunk::create(file.get()));
  std::memcpy(chunk->address(), "0123456789", 10);
  chunk.reset(grnxx::storage::Chunk::create(file.get()));
  assert(std::memcmp(chunk->address(), "0123456789", 10) == 0);

  file.reset(grnxx::storage::File::create(FILE_PATH));
  file->resize(1 << 16);

  chunk.reset(grnxx::storage::Chunk::create(file.get()));
  for (std::uint64_t i = 0; i < (1 << 16); ++i) {
    static_cast<std::uint8_t *>(chunk->address())[i] = static_cast<uint8_t>(i);
  }
  chunk.reset();
  file.reset();

  file.reset(grnxx::storage::File::open(FILE_PATH));
  chunk.reset(grnxx::storage::Chunk::create(file.get()));
  for (std::uint64_t i = 0; i < (1 << 16); ++i) {
    assert(static_cast<std::uint8_t *>(chunk->address())[i] ==
           static_cast<uint8_t>(i));
  }

  file.reset();
  assert(grnxx::storage::File::unlink(FILE_PATH));
}

void test_chunk_size() {
  constexpr std::uint64_t FILE_SIZE = 1 << 20;
  std::unique_ptr<grnxx::storage::File> file;
  std::unique_ptr<grnxx::storage::Chunk> chunk;

  file.reset(grnxx::storage::File::create(nullptr));
  file->resize(FILE_SIZE);

  chunk.reset(grnxx::storage::Chunk::create(file.get()));
  assert(chunk->size() == FILE_SIZE);
  chunk.reset(grnxx::storage::Chunk::create(file.get(), FILE_SIZE / 2));
  assert(chunk->size() == (FILE_SIZE / 2));
  chunk.reset(grnxx::storage::Chunk::create(file.get(), 0, FILE_SIZE / 2));
  assert(chunk->size() == (FILE_SIZE / 2));

  chunk.reset(grnxx::storage::Chunk::create(nullptr, 0, 1 << 20));
  assert(chunk->size() == (1 << 20));
}

void test_storage_create() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::Storage::unlink(FILE_PATH);
  std::unique_ptr<grnxx::Storage> storage;

  storage.reset(grnxx::Storage::create(FILE_PATH));
  storage.reset(grnxx::Storage::create(FILE_PATH, grnxx::STORAGE_TEMPORARY));

  storage.reset(grnxx::Storage::create(nullptr));
  storage.reset(grnxx::Storage::create(nullptr, grnxx::STORAGE_TEMPORARY));

  storage.reset();
  assert(grnxx::Storage::unlink(FILE_PATH));
}

void test_storage_open() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::Storage::unlink(FILE_PATH);
  std::unique_ptr<grnxx::Storage> storage;

  storage.reset(grnxx::Storage::create(FILE_PATH));
  storage.reset(grnxx::Storage::open(FILE_PATH));

  storage.reset();
  assert(grnxx::Storage::unlink(FILE_PATH));
}

void test_storage_open_or_create() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::Storage::unlink(FILE_PATH);
  std::unique_ptr<grnxx::Storage> storage;

  storage.reset(grnxx::Storage::open_or_create(FILE_PATH));
  storage.reset(grnxx::Storage::open_or_create(FILE_PATH));

  storage.reset();
  grnxx::Storage::unlink(FILE_PATH);
}

void test_storage_exists_and_unlink() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::Storage::unlink(FILE_PATH);
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(FILE_PATH));
  storage.reset();

  assert(grnxx::Storage::exists(FILE_PATH));
  assert(grnxx::Storage::unlink(FILE_PATH));
  assert(!grnxx::Storage::unlink(FILE_PATH));
  assert(!grnxx::Storage::exists(FILE_PATH));
}

void test_storage_create_node() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::Storage::unlink(FILE_PATH);
  std::unique_ptr<grnxx::Storage> storage;
  grnxx::StorageNode node;

  storage.reset(grnxx::Storage::create(FILE_PATH));
  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 20);
  assert(node.status() == grnxx::STORAGE_NODE_ACTIVE);
  assert(node.size() == (1 << 20));
  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 24);
  assert(node.status() == grnxx::STORAGE_NODE_ACTIVE);
  assert(node.size() == (1 << 24));
//  node = storage->create_node(-1, 1 << 16);
//  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, -1);

  storage.reset(grnxx::Storage::create(FILE_PATH, grnxx::STORAGE_TEMPORARY));
  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 20);
  assert(node.status() == grnxx::STORAGE_NODE_ACTIVE);
  assert(node.size() == (1 << 20));
  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 24);
  assert(node.status() == grnxx::STORAGE_NODE_ACTIVE);
  assert(node.size() == (1 << 24));
//  node = storage->create_node(-1, 1 << 16);
//  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, -1);

  storage.reset(grnxx::Storage::create(nullptr));
  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 20);
  assert(node.status() == grnxx::STORAGE_NODE_ACTIVE);
  assert(node.size() == (1 << 20));
  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 24);
  assert(node.status() == grnxx::STORAGE_NODE_ACTIVE);
  assert(node.size() == (1 << 24));
//  node = storage->create_node(-1, 1 << 16);
//  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, -1);

  assert(grnxx::Storage::unlink(FILE_PATH));
}

void test_storage_open_node() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::Storage::unlink(FILE_PATH);
  std::unique_ptr<grnxx::Storage> storage;
  grnxx::StorageNode node;
  std::uint32_t node_id_1, node_id_2;

  grnxx::StorageOptions options;
  options.root_size = 1 << 16;
  storage.reset(grnxx::Storage::create(FILE_PATH, grnxx::STORAGE_DEFAULT,
                                       options));
  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 20);
  node_id_1 = node.id();
  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 24);
  node_id_2 = node.id();

  storage.reset(grnxx::Storage::open(FILE_PATH));
  node = storage->open_node(grnxx::STORAGE_ROOT_NODE_ID);
  assert(node.status() == grnxx::STORAGE_NODE_ACTIVE);
  assert(node.size() == options.root_size);
  node = storage->open_node(node_id_1);
  assert(node.status() == grnxx::STORAGE_NODE_ACTIVE);
  assert(node.size() == (1 << 20));
  node = storage->open_node(node_id_2);
  assert(node.status() == grnxx::STORAGE_NODE_ACTIVE);
  assert(node.size() == (1 << 24));

  storage.reset();
  assert(grnxx::Storage::unlink(FILE_PATH));
}

void test_storage_unlink_node() {
  std::unique_ptr<grnxx::Storage> storage;
  grnxx::StorageNode node_1, node_2;

  storage.reset(grnxx::Storage::create(nullptr));
  node_1 = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 20);
  assert(node_1);
  node_2 = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 24);
  assert(node_2);

  assert(storage->unlink_node(node_1.id()));
  assert(node_1.status() == grnxx::STORAGE_NODE_UNLINKED);
  assert(storage->unlink_node(node_2.id()));
  assert(node_2.status() == grnxx::STORAGE_NODE_UNLINKED);
//  assert(!storage->unlink_node(grnxx::STORAGE_ROOT_NODE_ID));
}

void test_storage_sweep() {
  std::unique_ptr<grnxx::Storage> storage;
  grnxx::StorageNode node;

  storage.reset(grnxx::Storage::create(nullptr));
  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 18);
  assert(storage->create_node(node.id(), 1 << 18));
  assert(storage->create_node(node.id(), 1 << 18));
  uint64_t total_size = storage->total_size();
  for (int i = 0; i < 100; ++i) {
    assert(storage->unlink_node(node.id()));
    storage->sweep(grnxx::Duration(0));
    node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 18);
    assert(storage->create_node(node.id(), 1 << 18));
    assert(storage->create_node(node.id(), 1 << 18));
    assert(storage->total_size() == total_size);
  }

  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 0);
  assert(storage->create_node(node.id(), 0));
  assert(storage->create_node(node.id(), 0));
  total_size = storage->total_size();
  for (int i = 0; i < 100; ++i) {
    assert(storage->unlink_node(node.id()));
    storage->sweep(grnxx::Duration(0));
    node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 0);
    assert(storage->create_node(node.id(), 0));
    assert(storage->create_node(node.id(), 0));
    assert(storage->total_size() == total_size);
  }
}

void test_storage_path() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::Storage::unlink(FILE_PATH);
  std::unique_ptr<grnxx::Storage> storage;

  storage.reset(grnxx::Storage::create(FILE_PATH));
  assert(std::strcmp(storage->path(), FILE_PATH) == 0);

  storage.reset(grnxx::Storage::create(FILE_PATH, grnxx::STORAGE_TEMPORARY));
  assert(std::strcmp(storage->path(), FILE_PATH) == 0);

  assert(grnxx::Storage::unlink(FILE_PATH));
}

void test_storage_flags() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::Storage::unlink(FILE_PATH);
  std::unique_ptr<grnxx::Storage> storage;

  storage.reset(grnxx::Storage::create(FILE_PATH));
  assert(storage->flags() == grnxx::STORAGE_DEFAULT);

  storage.reset(grnxx::Storage::open(FILE_PATH, grnxx::STORAGE_READ_ONLY));
  assert(storage->flags() == grnxx::STORAGE_READ_ONLY);

  storage.reset(grnxx::Storage::create(FILE_PATH, grnxx::STORAGE_TEMPORARY));
  assert(storage->flags() == grnxx::STORAGE_TEMPORARY);

  assert(grnxx::Storage::unlink(FILE_PATH));
}

void test_storage_max_file_size() {
  grnxx::StorageOptions options;
  options.max_file_size = 1ULL << 36;
  std::unique_ptr<grnxx::Storage> storage(
      grnxx::Storage::create(nullptr, grnxx::STORAGE_DEFAULT, options));
  assert(storage->max_file_size() == options.max_file_size);
}

void test_storage_max_num_files() {
  grnxx::StorageOptions options;
  options.max_num_files = 100;
  std::unique_ptr<grnxx::Storage> storage(
      grnxx::Storage::create(nullptr, grnxx::STORAGE_DEFAULT, options));
  assert(storage->max_num_files() == options.max_num_files);
}

void test_storage_num_nodes() {
  grnxx::StorageNode node;
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  assert(storage->num_nodes() == 1);

  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 24);
  assert(storage->num_nodes() == 2);
  assert(storage->unlink_node(node.id()));
  assert(storage->num_nodes() == 2);
  storage->sweep(grnxx::Duration(0));
  assert(storage->num_nodes() == 1);
  for (int i = 0; i < 16; ++i) {
    assert(storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 24));
    assert(storage->num_nodes() == static_cast<std::uint32_t>(i + 2));
  }
}

void test_storage_num_chunks() {
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  assert(storage->num_chunks() == 1);

  for (int i = 0; i < 16; ++i) {
    assert(storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 24));
    assert(storage->num_chunks() == static_cast<std::uint16_t>(i + 2));
  }
}

void test_storage_body_usage() {
  uint64_t prev_body_usage = 0;
  grnxx::StorageNode node;
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));

  assert(storage->body_usage() > prev_body_usage);
  prev_body_usage = storage->body_usage();
  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 24);
  assert(storage->body_usage() == (prev_body_usage + node.size()));
  assert(storage->unlink_node(node.id()));
  storage->sweep(grnxx::Duration(0));
  assert(storage->body_usage() == prev_body_usage);
  for (int i = 0; i < 16; ++i) {
    assert(storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 24));
    assert(storage->body_usage() > prev_body_usage);
    prev_body_usage = storage->body_usage();
  }
}

void test_storage_body_size() {
  uint64_t prev_body_size = 0;
  grnxx::StorageNode node;
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));

  assert(storage->body_size() > prev_body_size);
  prev_body_size = storage->body_size();
  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 23);
  assert(storage->body_size() > prev_body_size);
  prev_body_size = storage->body_size();
  assert(storage->unlink_node(node.id()));
  storage->sweep(grnxx::Duration(0));
  assert(storage->body_size() == prev_body_size);
  for (int i = 0; i < 16; ++i) {
    assert(storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 24));
    assert(storage->body_size() > prev_body_size);
    prev_body_size = storage->body_size();
  }
}

void test_storage_total_size() {
  uint64_t prev_total_size = 0;
  std::unique_ptr<grnxx::Storage> storage(grnxx::Storage::create(nullptr));
  assert(storage->total_size() > prev_total_size);
  prev_total_size = storage->total_size();
  for (int i = 0; i < 16; ++i) {
    assert(storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 24));
    assert(storage->total_size() > prev_total_size);
    prev_total_size = storage->total_size();
  }
}

using IDVector = std::vector<std::uint32_t>;
using IDSet = std::unordered_set<std::uint32_t>;

void test_storage_random_queries() {
  std::mt19937 mersenne_twister;
  std::unique_ptr<grnxx::Storage> storage(
      grnxx::Storage::create(nullptr, grnxx::STORAGE_TEMPORARY));

  IDSet id_set;
  for (int i = 0; i < (1 << 16); ++i) {
    const std::uint32_t value = mersenne_twister() % 256;
    if (value < 1) {
      storage->sweep(grnxx::Duration(0));
    } else if (value < 64) {
      if (!id_set.empty()) {
        // Unlink a node.
        assert(storage->unlink_node(*id_set.begin()));
        id_set.erase(id_set.begin());
      }
    } else {
      std::uint64_t size;
      if (value < 96) {
        // Create a small node.
        const std::uint64_t SMALL_MAX_SIZE = std::uint64_t(1) << 11;
        size = mersenne_twister() % SMALL_MAX_SIZE;
      } else if (value < 248) {
        // Create a regular node.
        const std::uint64_t MEDIUM_MAX_SIZE = std::uint64_t(1) << 21;
        size = mersenne_twister() % MEDIUM_MAX_SIZE;
      } else {
        // Create a large node.
        const std::uint64_t LARGE_MAX_SIZE = std::uint64_t(1) << 28;
        size = mersenne_twister() % LARGE_MAX_SIZE;
      }
      const grnxx::StorageNode node =
          storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, size);
      id_set.insert(node.id());
    }
  }
  GRNXX_NOTICE() << ", num_nodes = " << storage->num_nodes()
                 << ", num_chunks = " << storage->num_chunks()
                 << ", body_usage = " << storage->body_usage()
                 << ", body_size = " << storage->body_size()
                 << ", total_size = " << storage->total_size();
}

void test_storage_random_queries2() {
  constexpr int LOOP_COUNT = 10;
  constexpr std::uint32_t NODE_COUNT = 1000;

  std::mt19937 mersenne_twister;
  std::unique_ptr<grnxx::Storage> storage(
      grnxx::Storage::create(nullptr, grnxx::STORAGE_TEMPORARY));
  const std::uint32_t root_size = storage->body_usage();

  for (int i = 0; i < LOOP_COUNT; ++i) {
    assert(storage->body_usage() == root_size);
    assert(storage->body_size() >= root_size);
    assert(storage->num_nodes() == 1);
    assert(storage->total_size() > storage->body_size());

    const std::uint32_t node_size =
        mersenne_twister() % (64 << (mersenne_twister() % 20));
    IDSet id_set;
    IDVector id_vector;
    IDVector root_child_id_vector;

    id_set.insert(grnxx::STORAGE_ROOT_NODE_ID);
    id_vector.push_back(grnxx::STORAGE_ROOT_NODE_ID);
    std::uint32_t max_node_id = grnxx::STORAGE_ROOT_NODE_ID;

    // Create "NODE_COUNT" nodes.
    for (std::uint32_t j = 0; j < NODE_COUNT; ++j) {
      const std::uint32_t parent_node_id =
          id_vector[mersenne_twister() % id_vector.size()];
      grnxx::StorageNode node =
          storage->create_node(parent_node_id, node_size);
      assert(id_set.insert(node.id()).second);
      id_vector.push_back(node.id());
      if (parent_node_id == grnxx::STORAGE_ROOT_NODE_ID) {
        root_child_id_vector.push_back(node.id());
      }
      if (node.id() > max_node_id) {
        max_node_id = node.id();
      }
    }
//    GRNXX_NOTICE() << "node_size = " << node_size
//                   << ", max_node_id = " << max_node_id
//                   << ", num_nodes = " << storage->num_nodes()
//                   << ", num_chunks = " << storage->num_chunks()
//                   << ", body_usage = " << storage->body_usage()
//                   << ", body_size = " << storage->body_size()
//                   << ", total_size = " << storage->total_size();

    // Unlink children of the root node.
    for (std::uint32_t root_child_id : root_child_id_vector) {
      assert(storage->unlink_node(root_child_id));
    }
    storage->sweep(grnxx::Duration(0));
  }
  GRNXX_NOTICE() << ", num_nodes = " << storage->num_nodes()
                 << ", num_chunks = " << storage->num_chunks()
                 << ", body_usage = " << storage->body_usage()
                 << ", body_size = " << storage->body_size()
                 << ", total_size = " << storage->total_size();
}
void test_path() {
  test_full_path();
  test_unique_path();
}

void test_file() {
  test_file_create();
  test_file_open();
  test_file_open_or_create();
  test_file_exists_and_unlink();
  test_file_lock_and_unlock();
  test_file_sync();
  test_file_resize_and_size();
  test_file_path();
  test_file_flags();
  test_file_handle();
}

void test_chunk() {
  test_chunk_create();
  test_chunk_sync();
  test_chunk_flags();
  test_chunk_address();
  test_chunk_size();
}

void test_storage() {
  test_storage_create();
  test_storage_open();
  test_storage_open_or_create();
  test_storage_exists_and_unlink();
  test_storage_create_node();
  test_storage_open_node();
  test_storage_unlink_node();
  test_storage_sweep();
  test_storage_path();
  test_storage_flags();
  test_storage_max_file_size();
  test_storage_max_num_files();
  test_storage_num_nodes();
  test_storage_num_chunks();
  test_storage_body_usage();
  test_storage_body_size();
  test_storage_total_size();
  test_storage_random_queries();
  test_storage_random_queries2();
}

}  // namespace

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  // FIXME: Increment the reference count for grnxx::PeriodicClock.
  grnxx::PeriodicClock clock;

  test_path();
  test_file();
  test_chunk();
  test_storage();

  return 0;
}
