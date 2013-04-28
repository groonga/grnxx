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
#include <sstream>

#include "grnxx/storage.hpp"
#include "grnxx/storage/file.hpp"
#include "grnxx/storage/path.hpp"
#include "grnxx/storage/chunk.hpp"
#include "grnxx/logger.hpp"

namespace {

void test_full_path(const char *path, const char *answer) {
  std::unique_ptr<char[]> full_path(grnxx::storage::Path::full_path(path));
  assert(full_path);
  assert(std::strcmp(full_path.get(), answer) == 0);
}

void test_full_path() {
  std::unique_ptr<char[]> full_path(grnxx::storage::Path::full_path(nullptr));
  assert(full_path);
  GRNXX_NOTICE() << "full_path = " << full_path.get();

  full_path.reset(grnxx::storage::Path::full_path("temp.grn"));
  assert(full_path);
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
  assert(unique_path);
  GRNXX_NOTICE() << "unique_path = " << unique_path.get();

  unique_path.reset(grnxx::storage::Path::unique_path("temp.grn"));
  GRNXX_NOTICE() << "unique_path = " << unique_path.get();
}

void test_file_create() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::storage::File::unlink(FILE_PATH);
  std::unique_ptr<grnxx::storage::File> file;

  file.reset(grnxx::storage::File::create(FILE_PATH));
  assert(file);
  file.reset(grnxx::storage::File::create(FILE_PATH));
  assert(!file);

  file.reset(grnxx::storage::File::create(FILE_PATH,
                                          grnxx::storage::FILE_TEMPORARY));
  assert(file);
  file.reset(grnxx::storage::File::create(FILE_PATH,
                                          grnxx::storage::FILE_TEMPORARY));
  assert(file);

  file.reset(grnxx::storage::File::create(nullptr));
  assert(file);
  file.reset(grnxx::storage::File::create(nullptr));
  assert(file);

  grnxx::storage::File::unlink(FILE_PATH);
}

void test_file_open() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::storage::File::unlink(FILE_PATH);
  std::unique_ptr<grnxx::storage::File> file;

  file.reset(grnxx::storage::File::open(FILE_PATH));
  assert(!file);

  file.reset(grnxx::storage::File::create(FILE_PATH));
  file.reset(grnxx::storage::File::open(FILE_PATH));
  assert(file);

  file.reset();
  grnxx::storage::File::unlink(FILE_PATH);
}

void test_file_open_or_create() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::storage::File::unlink(FILE_PATH);
  std::unique_ptr<grnxx::storage::File> file;

  file.reset(grnxx::storage::File::open_or_create(FILE_PATH));
  assert(file);
  file.reset(grnxx::storage::File::open_or_create(FILE_PATH));
  assert(file);

  file.reset();
  grnxx::storage::File::unlink(FILE_PATH);
}

void test_file_exists_and_unlink() {
  const char FILE_PATH[] = "temp.grn";
  std::unique_ptr<grnxx::storage::File>(
      grnxx::storage::File::open_or_create(FILE_PATH));

  assert(grnxx::storage::File::exists(FILE_PATH));
  assert(grnxx::storage::File::unlink(FILE_PATH));
  assert(!grnxx::storage::File::unlink(FILE_PATH));
  assert(!grnxx::storage::File::exists(FILE_PATH));
}

void test_file_lock_and_unlock() {
  const char FILE_PATH[] = "temp.grn";
  std::unique_ptr<grnxx::storage::File> file_1;
  file_1.reset(grnxx::storage::File::open_or_create(FILE_PATH));
  assert(file_1);

  assert(file_1->lock(grnxx::storage::FILE_LOCK_SHARED));
  assert(!file_1->lock(grnxx::storage::FILE_LOCK_SHARED));
  assert(file_1->unlock());
  assert(!file_1->unlock());

  assert(file_1->lock(grnxx::storage::FILE_LOCK_EXCLUSIVE));
  assert(!file_1->lock(grnxx::storage::FILE_LOCK_EXCLUSIVE));
  assert(file_1->unlock());
  assert(!file_1->unlock());

  std::unique_ptr<grnxx::storage::File> file_2;
  file_2.reset(grnxx::storage::File::open(FILE_PATH));
  assert(file_2);

  assert(file_1->lock(grnxx::storage::FILE_LOCK_SHARED));
  assert(file_2->lock(grnxx::storage::FILE_LOCK_SHARED |
                      grnxx::storage::FILE_LOCK_NONBLOCKING));
  assert(file_2->unlock());
  assert(!file_2->lock(grnxx::storage::FILE_LOCK_EXCLUSIVE |
                       grnxx::storage::FILE_LOCK_NONBLOCKING));
  assert(file_1->unlock());

  assert(file_1->lock(grnxx::storage::FILE_LOCK_EXCLUSIVE));
  assert(!file_2->lock(grnxx::storage::FILE_LOCK_SHARED |
                       grnxx::storage::FILE_LOCK_NONBLOCKING));
  assert(!file_2->lock(grnxx::storage::FILE_LOCK_EXCLUSIVE |
                       grnxx::storage::FILE_LOCK_NONBLOCKING));
  assert(file_1->unlock());

  file_1.reset();
  file_2.reset();
  grnxx::storage::File::unlink(FILE_PATH);
}

void test_file_sync() {
  std::unique_ptr<grnxx::storage::File> file(
      grnxx::storage::File::create(nullptr));
  assert(file);

  assert(file->sync());
}

void test_file_resize_and_size() {
  std::unique_ptr<grnxx::storage::File> file(
      grnxx::storage::File::create(nullptr));
  assert(file);

  assert(file->size() == 0);
  assert(file->resize(65536));
  assert(file->size() == 65536);
  assert(file->resize(1024));
  assert(file->size() == 1024);
  assert(!file->resize(-1));
}

void test_file_path() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::storage::File::unlink(FILE_PATH);
  std::unique_ptr<grnxx::storage::File> file;

  file.reset(grnxx::storage::File::create(FILE_PATH));
  assert(file);
  assert(std::strcmp(file->path(), FILE_PATH) == 0);

  file.reset(grnxx::storage::File::create(FILE_PATH,
                                          grnxx::storage::FILE_TEMPORARY));
  assert(file);
  assert(std::strcmp(file->path(), FILE_PATH) != 0);

  assert(grnxx::storage::File::unlink(FILE_PATH));
}

void test_file_flags() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::storage::File::unlink(FILE_PATH);
  std::unique_ptr<grnxx::storage::File> file;

  file.reset(grnxx::storage::File::create(FILE_PATH));
  assert(file);
  assert(file->flags() == grnxx::storage::FILE_DEFAULT);

  file.reset(grnxx::storage::File::open(FILE_PATH,
                                        grnxx::storage::FILE_READ_ONLY));
  assert(file);
  assert(file->flags() == grnxx::storage::FILE_READ_ONLY);

  file.reset(grnxx::storage::File::create(FILE_PATH,
                                          grnxx::storage::FILE_TEMPORARY));
  assert(file);
  assert(file->flags() == grnxx::storage::FILE_TEMPORARY);

  assert(grnxx::storage::File::unlink(FILE_PATH));
}

void test_file_handle() {
  std::unique_ptr<grnxx::storage::File> file(
      grnxx::storage::File::create(nullptr));
  assert(file);

  assert(file->handle());
}

void test_chunk_create() {
  std::unique_ptr<grnxx::storage::File> file;
  std::unique_ptr<grnxx::storage::Chunk> chunk;

  file.reset(grnxx::storage::File::create(nullptr));
  assert(file);
  chunk.reset(grnxx::storage::Chunk::create(file.get()));
  assert(!chunk);

  assert(file->resize(1 << 20));

  chunk.reset(grnxx::storage::Chunk::create(file.get()));
  assert(chunk);
  chunk.reset(grnxx::storage::Chunk::create(file.get(), 0));
  assert(chunk);
  chunk.reset(grnxx::storage::Chunk::create(file.get(), 0, -1));
  assert(chunk);
  chunk.reset(grnxx::storage::Chunk::create(file.get(), 0, file->size()));
  assert(chunk);
  chunk.reset(grnxx::storage::Chunk::create(file.get(), 0, 10));
  assert(chunk);

  chunk.reset(grnxx::storage::Chunk::create(file.get(), -1));
  assert(!chunk);
  chunk.reset(grnxx::storage::Chunk::create(file.get(), file->size() + 1));
  assert(!chunk);
  chunk.reset(grnxx::storage::Chunk::create(file.get(), 0, 0));
  assert(!chunk);
  chunk.reset(grnxx::storage::Chunk::create(file.get(), 0, file->size() + 1));
  assert(!chunk);
  chunk.reset(grnxx::storage::Chunk::create(file.get(), file->size() / 2,
                                            file->size()));
  assert(!chunk);

  chunk.reset(grnxx::storage::Chunk::create(nullptr, 0, 1 << 20));
  assert(chunk);

  chunk.reset(grnxx::storage::Chunk::create(nullptr, 0, 0));
  assert(!chunk);
  chunk.reset(grnxx::storage::Chunk::create(nullptr, 0, -1));
  assert(!chunk);
}

void test_chunk_sync() {
  std::unique_ptr<grnxx::storage::File> file;
  std::unique_ptr<grnxx::storage::Chunk> chunk;

  file.reset(grnxx::storage::File::create(nullptr));
  assert(file);
  assert(file->resize(1 << 20));

  chunk.reset(grnxx::storage::Chunk::create(file.get()));
  assert(chunk);
  assert(chunk->sync());
  assert(chunk->sync(0));
  assert(chunk->sync(0, -1));
  assert(chunk->sync(0, 0));
  assert(chunk->sync(0, file->size()));

  assert(!chunk->sync(-1));
  assert(!chunk->sync(file->size() + 1));
  assert(!chunk->sync(0, file->size() + 1));
  assert(!chunk->sync(file->size() / 2, file->size()));

  chunk.reset(grnxx::storage::Chunk::create(nullptr, 0, 1 << 20));
  assert(chunk);
  assert(!chunk->sync());
}

void test_chunk_flags() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::storage::File::unlink(FILE_PATH);
  std::unique_ptr<grnxx::storage::File> file;
  std::unique_ptr<grnxx::storage::Chunk> chunk;

  file.reset(grnxx::storage::File::create(FILE_PATH));
  assert(file);
  assert(file->resize(1 << 20));

  chunk.reset(grnxx::storage::Chunk::create(file.get()));
  assert(chunk);
  assert(chunk->flags() == grnxx::storage::CHUNK_DEFAULT);

  file.reset(grnxx::storage::File::open(FILE_PATH,
                                        grnxx::storage::FILE_READ_ONLY));
  assert(file);

  chunk.reset(grnxx::storage::Chunk::create(file.get()));
  assert(chunk);
  assert(chunk->flags() == grnxx::storage::CHUNK_READ_ONLY);

  file.reset();
  assert(grnxx::storage::File::unlink(FILE_PATH));
}

void test_chunk_address() {
  std::unique_ptr<grnxx::storage::File> file;
  std::unique_ptr<grnxx::storage::Chunk> chunk;

  file.reset(grnxx::storage::File::create(nullptr));
  assert(file);
  assert(file->resize(10));

  chunk.reset(grnxx::storage::Chunk::create(file.get()));
  assert(chunk);
  std::memcpy(chunk->address(), "0123456789", 10);
  chunk.reset(grnxx::storage::Chunk::create(file.get()));
  assert(chunk);
  assert(std::memcmp(chunk->address(), "0123456789", 10) == 0);
}

void test_chunk_size() {
  std::unique_ptr<grnxx::storage::File> file;
  std::unique_ptr<grnxx::storage::Chunk> chunk;

  file.reset(grnxx::storage::File::create(nullptr));
  assert(file);
  assert(file->resize(1 << 20));

  chunk.reset(grnxx::storage::Chunk::create(file.get()));
  assert(chunk);
  assert(chunk->size() == file->size());
  chunk.reset(grnxx::storage::Chunk::create(file.get(), file->size() / 2));
  assert(chunk);
  assert(chunk->size() == (file->size() / 2));
  chunk.reset(grnxx::storage::Chunk::create(file.get(), 0, file->size() / 2));
  assert(chunk);
  assert(chunk->size() == (file->size() / 2));

  chunk.reset(grnxx::storage::Chunk::create(nullptr, 0, 1 << 20));
  assert(chunk);
  assert(chunk->size() == (1 << 20));
}

void test_storage_create() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::Storage::unlink(FILE_PATH);
  std::unique_ptr<grnxx::Storage> storage;

  storage.reset(grnxx::Storage::create(FILE_PATH));
  assert(storage);
  storage.reset(grnxx::Storage::create(FILE_PATH, grnxx::STORAGE_TEMPORARY));
  assert(storage);

  storage.reset(grnxx::Storage::create(nullptr));
  assert(storage);
  storage.reset(grnxx::Storage::create(nullptr, grnxx::STORAGE_TEMPORARY));
  assert(storage);

  storage.reset();
  assert(grnxx::Storage::unlink(FILE_PATH));
}

void test_storage_open() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::Storage::unlink(FILE_PATH);
  std::unique_ptr<grnxx::Storage> storage;

  storage.reset(grnxx::Storage::create(FILE_PATH));
  assert(storage);

  storage.reset(grnxx::Storage::open(FILE_PATH));
  assert(storage);

  storage.reset();
  assert(grnxx::Storage::unlink(FILE_PATH));
}

void test_storage_open_or_create() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::Storage::unlink(FILE_PATH);
  std::unique_ptr<grnxx::Storage> storage;

  storage.reset(grnxx::Storage::open_or_create(FILE_PATH));
  assert(storage);
  storage.reset(grnxx::Storage::open_or_create(FILE_PATH));
  assert(storage);

  storage.reset();
  grnxx::Storage::unlink(FILE_PATH);
}

void test_storage_exists_and_unlink() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::Storage::unlink(FILE_PATH);
  std::unique_ptr<grnxx::Storage>(grnxx::Storage::create(FILE_PATH));

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
  assert(node.is_valid());
  assert(node.status() == grnxx::STORAGE_NODE_ACTIVE);
  assert(node.size() == (1 << 20));
  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 24);
  assert(node.is_valid());
  assert(node.status() == grnxx::STORAGE_NODE_ACTIVE);
  assert(node.size() == (1 << 24));
  node = storage->create_node(-1, 1 << 16);
  assert(!node.is_valid());
  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, -1);
  assert(!node.is_valid());

  storage.reset(grnxx::Storage::create(FILE_PATH, grnxx::STORAGE_TEMPORARY));
  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 20);
  assert(node.is_valid());
  assert(node.status() == grnxx::STORAGE_NODE_ACTIVE);
  assert(node.size() == (1 << 20));
  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 24);
  assert(node.is_valid());
  assert(node.status() == grnxx::STORAGE_NODE_ACTIVE);
  assert(node.size() == (1 << 24));
  node = storage->create_node(-1, 1 << 16);
  assert(!node.is_valid());
  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, -1);
  assert(!node.is_valid());

  storage.reset(grnxx::Storage::create(nullptr));
  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 20);
  assert(node.is_valid());
  assert(node.status() == grnxx::STORAGE_NODE_ACTIVE);
  assert(node.size() == (1 << 20));
  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, 1 << 24);
  assert(node.is_valid());
  assert(node.status() == grnxx::STORAGE_NODE_ACTIVE);
  assert(node.size() == (1 << 24));
  node = storage->create_node(-1, 1 << 16);
  assert(!node.is_valid());
  node = storage->create_node(grnxx::STORAGE_ROOT_NODE_ID, -1);
  assert(!node.is_valid());

  assert(grnxx::Storage::unlink(FILE_PATH));
}

void test_storage_open_node() {
  // TODO
}

void test_storage_unlink_node() {
  // TODO
}

void test_storage_sweep() {
  // TODO
}

void test_storage_path() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::Storage::unlink(FILE_PATH);
  std::unique_ptr<grnxx::Storage> storage;

  storage.reset(grnxx::Storage::create(FILE_PATH));
  assert(storage);
  assert(std::strcmp(storage->path(), FILE_PATH) == 0);

  storage.reset(grnxx::Storage::create(FILE_PATH, grnxx::STORAGE_TEMPORARY));
  assert(storage);
  assert(std::strcmp(storage->path(), FILE_PATH) == 0);

  assert(grnxx::Storage::unlink(FILE_PATH));
}

void test_storage_flags() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::Storage::unlink(FILE_PATH);
  std::unique_ptr<grnxx::Storage> storage;

  storage.reset(grnxx::Storage::create(FILE_PATH));
  assert(storage);
  assert(storage->flags() == grnxx::STORAGE_DEFAULT);

  storage.reset(grnxx::Storage::open(FILE_PATH, grnxx::STORAGE_READ_ONLY));
  assert(storage);
  assert(storage->flags() == grnxx::STORAGE_READ_ONLY);

  storage.reset(grnxx::Storage::create(FILE_PATH, grnxx::STORAGE_TEMPORARY));
  assert(storage);
  assert(storage->flags() == grnxx::STORAGE_TEMPORARY);

  assert(grnxx::Storage::unlink(FILE_PATH));
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
}

}  // namespace

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_path();
  test_file();
  test_chunk();
  test_storage();

  return 0;
}
