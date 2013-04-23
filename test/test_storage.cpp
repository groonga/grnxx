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

#include "grnxx/storage/file.hpp"
#include "grnxx/storage/path.hpp"
#include "grnxx/storage/view.hpp"
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

void test_view_create() {
  std::unique_ptr<grnxx::storage::File> file;
  std::unique_ptr<grnxx::storage::View> view;

  file.reset(grnxx::storage::File::create(nullptr));
  assert(file);
  view.reset(grnxx::storage::View::create(file.get()));
  assert(!view);

  assert(file->resize(1 << 20));

  view.reset(grnxx::storage::View::create(file.get()));
  assert(view);
  view.reset(grnxx::storage::View::create(file.get(), 0));
  assert(view);
  view.reset(grnxx::storage::View::create(file.get(), 0, -1));
  assert(view);
  view.reset(grnxx::storage::View::create(file.get(), 0, file->size()));
  assert(view);
  view.reset(grnxx::storage::View::create(file.get(), 0, 10));
  assert(view);

  view.reset(grnxx::storage::View::create(file.get(), -1));
  assert(!view);
  view.reset(grnxx::storage::View::create(file.get(), file->size() + 1));
  assert(!view);
  view.reset(grnxx::storage::View::create(file.get(), 0, 0));
  assert(!view);
  view.reset(grnxx::storage::View::create(file.get(), 0, file->size() + 1));
  assert(!view);
  view.reset(grnxx::storage::View::create(file.get(), file->size() / 2,
                                          file->size()));
  assert(!view);

  view.reset(grnxx::storage::View::create(nullptr, 0, 1 << 20));
  assert(view);

  view.reset(grnxx::storage::View::create(nullptr, 0, 0));
  assert(!view);
  view.reset(grnxx::storage::View::create(nullptr, 0, -1));
  assert(!view);
}

void test_view_sync() {
  std::unique_ptr<grnxx::storage::File> file;
  std::unique_ptr<grnxx::storage::View> view;

  file.reset(grnxx::storage::File::create(nullptr));
  assert(file);
  assert(file->resize(1 << 20));

  view.reset(grnxx::storage::View::create(file.get()));
  assert(view);
  assert(view->sync());
  assert(view->sync(0));
  assert(view->sync(0, -1));
  assert(view->sync(0, 0));
  assert(view->sync(0, file->size()));

  assert(!view->sync(-1));
  assert(!view->sync(file->size() + 1));
  assert(!view->sync(0, file->size() + 1));
  assert(!view->sync(file->size() / 2, file->size()));

  view.reset(grnxx::storage::View::create(nullptr, 0, 1 << 20));
  assert(view);
  assert(!view->sync());
}

void test_view_flags() {
  const char FILE_PATH[] = "temp.grn";
  grnxx::storage::File::unlink(FILE_PATH);
  std::unique_ptr<grnxx::storage::File> file;
  std::unique_ptr<grnxx::storage::View> view;

  file.reset(grnxx::storage::File::create(FILE_PATH));
  assert(file);
  assert(file->resize(1 << 20));

  view.reset(grnxx::storage::View::create(file.get()));
  assert(view);
  assert(view->flags() == grnxx::storage::VIEW_DEFAULT);

  file.reset(grnxx::storage::File::open(FILE_PATH,
                                        grnxx::storage::FILE_READ_ONLY));
  assert(file);

  view.reset(grnxx::storage::View::create(file.get()));
  assert(view);
  assert(view->flags() == grnxx::storage::VIEW_READ_ONLY);

  file.reset();
  assert(grnxx::storage::File::unlink(FILE_PATH));
}

void test_view_address() {
  std::unique_ptr<grnxx::storage::File> file;
  std::unique_ptr<grnxx::storage::View> view;

  file.reset(grnxx::storage::File::create(nullptr));
  assert(file);
  assert(file->resize(10));

  view.reset(grnxx::storage::View::create(file.get()));
  assert(view);
  std::memcpy(view->address(), "0123456789", 10);
  view.reset(grnxx::storage::View::create(file.get()));
  assert(view);
  assert(std::memcmp(view->address(), "0123456789", 10) == 0);
}

void test_view_size() {
  std::unique_ptr<grnxx::storage::File> file;
  std::unique_ptr<grnxx::storage::View> view;

  file.reset(grnxx::storage::File::create(nullptr));
  assert(file);
  assert(file->resize(1 << 20));

  view.reset(grnxx::storage::View::create(file.get()));
  assert(view);
  assert(view->size() == file->size());
  view.reset(grnxx::storage::View::create(file.get(), file->size() / 2));
  assert(view);
  assert(view->size() == (file->size() / 2));
  view.reset(grnxx::storage::View::create(file.get(), 0, file->size() / 2));
  assert(view);
  assert(view->size() == (file->size() / 2));

  view.reset(grnxx::storage::View::create(nullptr, 0, 1 << 20));
  assert(view);
  assert(view->size() == (1 << 20));
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

void test_view() {
  test_view_create();
  test_view_sync();
  test_view_flags();
  test_view_address();
  test_view_size();
}

}  // namespace

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_path();
  test_file();
  test_view();

  return 0;
}
