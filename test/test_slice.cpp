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

#include "logger.hpp"
#include "slice.hpp"

void test_constructors() {
  grnxx::Slice slice;

  assert(!slice);
  assert(slice.size() == 0);

  const char *empty_str = "";
  slice = grnxx::Slice(empty_str);

  assert(!slice);
  assert(slice.address() == empty_str);
  assert(slice.size() == 0);

  const char *digits = "0123456789";
  slice = grnxx::Slice(digits);

  assert(slice);
  assert(slice.address() == digits);
  assert(slice.size() == 10);

  slice = grnxx::Slice(digits + 3, 5);

  assert(slice);
  assert(slice.address() == (digits + 3));
  assert(slice.size() == 5);
}

void test_prefix() {
  grnxx::Slice slice("0123456789");
  grnxx::Slice prefix = slice.prefix(0);

  assert(!prefix);
  assert(prefix.ptr() == slice.ptr());
  assert(prefix.size() == 0);

  prefix = slice.prefix(5);

  assert(prefix);
  assert(prefix.ptr() == slice.ptr());
  assert(prefix.size() == 5);
}

void test_suffix() {
  grnxx::Slice slice("0123456789");
  grnxx::Slice suffix = slice.suffix(0);

  assert(!suffix);
  assert(suffix.ptr() == (slice.ptr() + 10));
  assert(suffix.size() == 0);

  suffix = slice.suffix(5);

  assert(suffix);
  assert(suffix.ptr() == (slice.ptr() + 5));
  assert(suffix.size() == 5);
}

void test_subslice() {
  grnxx::Slice slice("0123456789");
  grnxx::Slice subslice = slice.subslice(5, 0);

  assert(!subslice);
  assert(subslice.ptr() == (slice.ptr() + 5));
  assert(subslice.size() == 0);

  subslice = slice.subslice(3, 5);

  assert(subslice);
  assert(subslice.ptr() == (slice.ptr() + 3));
  assert(subslice.size() == 5);
}

void test_remove_prefix() {
  grnxx::Slice slice("0123456789");
  grnxx::Slice suffix = slice;

  suffix.remove_prefix(0);
  assert(suffix == slice);

  suffix.remove_prefix(3);
  assert(suffix == slice.suffix(7));

  suffix.remove_prefix(5);
  assert(suffix == slice.suffix(2));

  suffix.remove_prefix(2);
  assert(suffix == slice.suffix(0));
}

void test_remove_suffix() {
  grnxx::Slice slice("0123456789");
  grnxx::Slice prefix = slice;

  prefix.remove_suffix(0);
  assert(prefix == slice);

  prefix.remove_suffix(3);
  assert(prefix == slice.prefix(7));

  prefix.remove_suffix(5);
  assert(prefix == slice.prefix(2));

  prefix.remove_suffix(2);
  assert(prefix == slice.prefix(0));
}

void test_compare() {
  grnxx::Slice abc("abc");
  grnxx::Slice abcde("abcde");
  grnxx::Slice cde("cde");

  assert(abc.compare(abc) == 0);
  assert(abc.compare(abcde) < 0);
  assert(abc.compare(cde) < 0);

  assert(abcde.compare(abc) > 0);
  assert(abcde.compare(abcde) == 0);
  assert(abcde.compare(cde) < 0);

  assert(cde.compare(abc) > 0);
  assert(cde.compare(abcde) > 0);
  assert(cde.compare(cde) == 0);
}

void test_starts_with() {
  grnxx::Slice slice("cde");

  assert(slice.starts_with(""));
  assert(slice.starts_with("c"));
  assert(slice.starts_with("cd"));
  assert(slice.starts_with("cde"));
  assert(!slice.starts_with("cdef"));
  assert(!slice.starts_with("abc"));
}

void test_ends_with() {
  grnxx::Slice slice("cde");

  assert(slice.ends_with(""));
  assert(slice.ends_with("e"));
  assert(slice.ends_with("de"));
  assert(slice.ends_with("cde"));
  assert(!slice.ends_with("bcde"));
  assert(!slice.ends_with("abc"));
}

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_constructors();
  test_prefix();
  test_suffix();
  test_subslice();
  test_remove_prefix();
  test_remove_suffix();
  test_compare();
  test_starts_with();
  test_ends_with();

  return 0;
}
