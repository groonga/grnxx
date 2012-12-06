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
#include "io/file_info.hpp"
#include "logger.hpp"

void test_non_existent_file() {
  const char FILE_PATH[] = "temp.grn";

  grnxx::io::File::unlink_if_exists(FILE_PATH);

  grnxx::io::FileInfo file_info(FILE_PATH);

  GRNXX_NOTICE() << "file_info (invalid) = " << file_info;

  assert(!file_info);
  assert(!file_info.is_file());
  assert(!file_info.is_directory());
}

void test_existent_file() {
  const char FILE_PATH[] = "temp.grn";
  const std::uint64_t FILE_SIZE = 12345;

  grnxx::io::File::unlink_if_exists(FILE_PATH);
  grnxx::io::File file(grnxx::io::FILE_CREATE, FILE_PATH);
  file.resize(FILE_SIZE);

  grnxx::io::FileInfo file_info(FILE_PATH);

  GRNXX_NOTICE() << "file_info (regular) = " << file_info;

  assert(file_info);
  assert(file_info.is_file());
  assert(!file_info.is_directory());
  assert(file_info.size() == FILE_SIZE);

  assert(grnxx::io::FileInfo(file));

  file.close();
  grnxx::io::File::unlink(FILE_PATH);
}

void test_non_existent_directory() {
  const char DIRECTORY_PATH[] = "no_such_directory/";

  grnxx::io::FileInfo file_info(DIRECTORY_PATH);

  assert(!file_info);
  assert(!file_info.is_file());
  assert(!file_info.is_directory());
}

void test_existent_directory() {
  const char DIRECTORY_PATH[] = "./";

  grnxx::io::FileInfo file_info(DIRECTORY_PATH);

  GRNXX_NOTICE() << "file_info (directory) = " << file_info;

  assert(file_info);
  assert(!file_info.is_file());
  assert(file_info.is_directory());
}

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_non_existent_file();
  test_existent_file();
  test_non_existent_directory();
  test_existent_directory();

  const char FILE_PATH[] = "temp.grn";

  grnxx::io::File::unlink_if_exists(FILE_PATH);

  grnxx::io::FileInfo file_info(FILE_PATH);

  GRNXX_NOTICE() << "file_info = " << file_info;

  assert(!file_info);

  grnxx::io::File file(grnxx::io::FILE_TEMPORARY, FILE_PATH);
  file.resize(12345);

  file_info = grnxx::io::FileInfo(file);

  file_info = grnxx::io::FileInfo(file);

  GRNXX_NOTICE() << "file_info = " << file_info;

  assert(file_info);
  assert(file_info.is_file());
  assert(!file_info.is_directory());
  assert(file_info.size() == 12345);

  file.close();

  assert(!grnxx::io::File::exists("temp.dat"));

  return 0;
}
