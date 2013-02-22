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

#include "logger.hpp"
#include "io/file.hpp"
#include "io/view.hpp"

void test_anonymous_mmap() {
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

void test_file_backed_mmap() {
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

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_anonymous_mmap();
  test_file_backed_mmap();

  return 0;
}
