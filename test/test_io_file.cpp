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

#include "io/file.hpp"
#include "logger.hpp"

void test_create() {
  const char FILE_PATH[] = "temp.grn";

  grnxx::io::File::unlink_if_exists(FILE_PATH);

  assert(!grnxx::io::File::exists(FILE_PATH));
  assert(!grnxx::io::File::unlink_if_exists(FILE_PATH));

  grnxx::io::File file(grnxx::io::FILE_CREATE, FILE_PATH);

  assert(file.path() == FILE_PATH);
  assert(file.tell() == 0);
  assert(file.size() == 0);

  file.close();

  assert(grnxx::io::File::exists(FILE_PATH));
  grnxx::io::File::unlink(FILE_PATH);

  assert(!grnxx::io::File::exists(FILE_PATH));
  assert(!grnxx::io::File::unlink_if_exists(FILE_PATH));
}

void test_open() {
  const char FILE_PATH[] = "temp.grn";

  grnxx::io::File::unlink_if_exists(FILE_PATH);
  grnxx::io::File(grnxx::io::FILE_CREATE, FILE_PATH);

  grnxx::io::File file(grnxx::io::FILE_OPEN, FILE_PATH);

  file.close();
  grnxx::io::File::unlink(FILE_PATH);
}

void test_create_or_open() {
  const char FILE_PATH[] = "temp.grn";

  grnxx::io::File::unlink_if_exists(FILE_PATH);

  grnxx::io::File file(grnxx::io::FILE_CREATE_OR_OPEN, FILE_PATH);

  file.close();
  file.open(grnxx::io::FILE_CREATE_OR_OPEN, FILE_PATH);

  file.close();
  grnxx::io::File::unlink(FILE_PATH);
}

void test_write() {
  const char FILE_PATH[] = "temp.grn";

  grnxx::io::File::unlink_if_exists(FILE_PATH);
  grnxx::io::File file(grnxx::io::FILE_CREATE, FILE_PATH);

  assert(file.write("0123456789", 10) == 10);
  assert(file.tell() == 10);
  assert(file.size() == 10);

  file.close();
  grnxx::io::File::unlink(FILE_PATH);
}

void test_resize() {
  const char FILE_PATH[] = "temp.grn";
  const std::uint64_t FILE_SIZE = 1 << 20;

  grnxx::io::File::unlink_if_exists(FILE_PATH);
  grnxx::io::File file(grnxx::io::FILE_CREATE, FILE_PATH);

  file.resize(FILE_SIZE);
  assert(file.tell() == FILE_SIZE);
  assert(file.size() == FILE_SIZE);

  file.resize(0);
  assert(file.tell() == 0);
  assert(file.size() == 0);

  file.close();
  grnxx::io::File::unlink(FILE_PATH);
}

void test_seek() {
  const char FILE_PATH[] = "temp.grn";
  const std::uint64_t FILE_SIZE = 1 << 20;

  grnxx::io::File::unlink_if_exists(FILE_PATH);
  grnxx::io::File file(grnxx::io::FILE_CREATE, FILE_PATH);

  file.resize(FILE_SIZE);

  assert(file.seek(0) == 0);
  assert(file.tell() == 0);

  assert(file.seek(FILE_SIZE / 2) == (FILE_SIZE / 2));
  assert(file.tell() == (FILE_SIZE / 2));

  assert(file.seek(FILE_SIZE / 4, SEEK_CUR) ==
         ((FILE_SIZE / 2) + (FILE_SIZE / 4)));
  assert(file.tell() == ((FILE_SIZE / 2) + (FILE_SIZE / 4)));

  assert(file.seek(-(FILE_SIZE / 2), SEEK_END) == (FILE_SIZE / 2));
  assert(file.tell() == (FILE_SIZE / 2));

  file.close();
  grnxx::io::File::unlink(FILE_PATH);
}

void test_read() {
  const char FILE_PATH[] = "temp.grn";

  grnxx::io::File::unlink_if_exists(FILE_PATH);
  grnxx::io::File file(grnxx::io::FILE_CREATE, FILE_PATH);

  file.write("0123456789", 10);
  file.seek(0);

  char buf[256];
  assert(file.read(buf, sizeof(buf)) == 10);
  assert(std::memcmp(buf, "0123456789", 10) == 0);
  assert(file.tell() == 10);

  file.seek(3);

  assert(file.read(buf, 5) == 5);
  assert(file.tell() == 8);
  assert(std::memcmp(buf, "34567", 5) == 0);

  file.close();
  grnxx::io::File::unlink(FILE_PATH);
}

void test_temporary() {
  const char FILE_PATH[] = "temp.grn";

  grnxx::io::File file(grnxx::io::FILE_TEMPORARY, FILE_PATH);

  assert(file.write("0123456789", 10) == 10);
  assert(file.seek(0) == 0);

  char buf[256];
  assert(file.read(buf, sizeof(buf)) == 10);
  assert(std::memcmp(buf, "0123456789", 10) == 0);

  grnxx::String path = file.path();

  file.close();

  assert(!grnxx::io::File::exists(path.c_str()));
}

void test_unlink_at_close() {
  const char FILE_PATH[] = "temp.grn";

  grnxx::io::File file(grnxx::io::FILE_CREATE, FILE_PATH);

  file.set_unlink_at_close(true);

  assert(file.unlink_at_close());

  file.close();

  assert(!grnxx::io::File::exists(FILE_PATH));
}

void test_lock() {
  const char FILE_PATH[] = "temp.grn";

  grnxx::io::File::unlink_if_exists(FILE_PATH);
  grnxx::io::File file_1(grnxx::io::FILE_CREATE, FILE_PATH);

  assert(!file_1.unlock());
  assert(file_1.try_lock(grnxx::io::FILE_LOCK_EXCLUSIVE));
  assert(!file_1.try_lock(grnxx::io::FILE_LOCK_SHARED));
  assert(file_1.unlock());

  assert(file_1.try_lock(grnxx::io::FILE_LOCK_SHARED));
  assert(file_1.unlock());
  assert(!file_1.unlock());

  grnxx::io::File file_2(grnxx::io::FILE_OPEN, FILE_PATH);

  assert(file_1.try_lock(grnxx::io::FILE_LOCK_EXCLUSIVE));
  assert(!file_2.try_lock(grnxx::io::FILE_LOCK_SHARED));
  assert(!file_2.try_lock(grnxx::io::FILE_LOCK_EXCLUSIVE));
  assert(!file_2.unlock());
  assert(file_1.unlock());

  assert(file_1.try_lock(grnxx::io::FILE_LOCK_SHARED));
  assert(!file_2.try_lock(grnxx::io::FILE_LOCK_EXCLUSIVE));
  assert(file_2.try_lock(grnxx::io::FILE_LOCK_SHARED));
  assert(file_1.unlock());
  assert(!file_1.try_lock(grnxx::io::FILE_LOCK_EXCLUSIVE));
  assert(file_2.unlock());

  file_1 = grnxx::io::File();
  file_2 = grnxx::io::File();
  grnxx::io::File::unlink(FILE_PATH);
}

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_create();
  test_open();
  test_create_or_open();
  test_write();
  test_resize();
  test_seek();
  test_read();
  test_temporary();
  test_unlink_at_close();
  test_lock();

  return 0;
}
