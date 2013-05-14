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

#include "grnxx/bytes.hpp"
#include "grnxx/logger.hpp"

void test_constructors() {
  grnxx::Bytes bytes = nullptr;

  assert(!bytes);
  assert(bytes.size() == 0);

  const char *empty_str = "";
  bytes = grnxx::Bytes(empty_str);

  assert(!bytes);
  assert(bytes.address() == empty_str);
  assert(bytes.size() == 0);

  const char *digits = "0123456789";
  bytes = grnxx::Bytes(digits);

  assert(bytes);
  assert(bytes.address() == digits);
  assert(bytes.size() == 10);

  bytes = grnxx::Bytes(digits + 3, 5);

  assert(bytes);
  assert(bytes.address() == (digits + 3));
  assert(bytes.size() == 5);
}

void test_extract() {
  grnxx::Bytes bytes("0123456789");
  grnxx::Bytes part = bytes.extract(5, 0);

  assert(!part);
  assert(part.ptr() == (bytes.ptr() + 5));
  assert(part.size() == 0);

  part = bytes.extract(3, 5);

  assert(part);
  assert(part.ptr() == (bytes.ptr() + 3));
  assert(part.size() == 5);
}

void test_trim() {
  grnxx::Bytes bytes("0123456789");
  grnxx::Bytes part = bytes.extract(5, 0);

  assert(!part);
  assert(part.ptr() == (bytes.ptr() + 5));
  assert(part.size() == 0);

  part = bytes.extract(3, 5);

  assert(part);
  assert(part.ptr() == (bytes.ptr() + 3));
  assert(part.size() == 5);
}

void test_prefix() {
  grnxx::Bytes bytes("0123456789");
  grnxx::Bytes prefix = bytes.prefix(0);

  assert(!prefix);
  assert(prefix.ptr() == bytes.ptr());
  assert(prefix.size() == 0);

  prefix = bytes.prefix(5);

  assert(prefix);
  assert(prefix.ptr() == bytes.ptr());
  assert(prefix.size() == 5);
}

void test_suffix() {
  grnxx::Bytes bytes("0123456789");
  grnxx::Bytes suffix = bytes.suffix(0);

  assert(!suffix);
  assert(suffix.ptr() == (bytes.ptr() + 10));
  assert(suffix.size() == 0);

  suffix = bytes.suffix(5);

  assert(suffix);
  assert(suffix.ptr() == (bytes.ptr() + 5));
  assert(suffix.size() == 5);
}

void test_except_prefix() {
  grnxx::Bytes bytes("0123456789");

  assert(bytes.except_prefix(0) == bytes);
  assert(bytes.except_prefix(3) == bytes.suffix(7));
  assert(bytes.except_prefix(3).except_prefix(5) == bytes.suffix(2));
  assert(!bytes.except_prefix(3).except_prefix(5).except_prefix(2));
}

void test_except_suffix() {
  grnxx::Bytes bytes("0123456789");

  assert(bytes.except_suffix(0) == bytes);
  assert(bytes.except_suffix(3) == bytes.prefix(7));
  assert(!bytes.except_suffix(3).except_suffix(5).except_suffix(2));
}

void test_compare() {
  grnxx::Bytes abc("abc");
  grnxx::Bytes abcde("abcde");
  grnxx::Bytes cde("cde");

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
  grnxx::Bytes bytes("cde");

  assert(bytes.starts_with(""));
  assert(bytes.starts_with("c"));
  assert(bytes.starts_with("cd"));
  assert(bytes.starts_with("cde"));
  assert(!bytes.starts_with("cdef"));
  assert(!bytes.starts_with("abc"));
}

void test_equal_to() {
  grnxx::Bytes abc("abc");
  grnxx::Bytes abc2("abc");
  grnxx::Bytes abcde("abcde");
  grnxx::Bytes cde("cde");

  assert(abc == abc);
  assert(!(abc == abcde));
  assert(!(abcde == abc));
  assert(!(abc == cde));
}

void test_not_equal_to() {
  grnxx::Bytes abc("abc");
  grnxx::Bytes abcde("abcde");
  grnxx::Bytes cde("cde");

  assert(!(abc != abc));
  assert(abc != abcde);
  assert(abcde != abc);
  assert(abc != cde);
}

void test_less() {
  grnxx::Bytes abc("abc");
  grnxx::Bytes abcde("abcde");
  grnxx::Bytes cde("cde");

  assert(!(abc < abc));
  assert(abc < abcde);
  assert(!(abcde < abc));
  assert(abc < cde);
  assert(!(cde < abc));
}

void test_greater() {
  grnxx::Bytes abc("abc");
  grnxx::Bytes abcde("abcde");
  grnxx::Bytes cde("cde");

  assert(!(abc > abc));
  assert(!(abc > abcde));
  assert(abcde > abc);
  assert(!(abc > cde));
  assert(cde > abc);
}

void test_less_equal() {
  grnxx::Bytes abc("abc");
  grnxx::Bytes abcde("abcde");
  grnxx::Bytes cde("cde");

  assert(abc <= abc);
  assert(abc <= abcde);
  assert(!(abcde <= abc));
  assert(abc <= cde);
  assert(!(cde <= abc));
}

void test_greater_equal() {
  grnxx::Bytes abc("abc");
  grnxx::Bytes abcde("abcde");
  grnxx::Bytes cde("cde");

  assert(abc >= abc);
  assert(!(abc >= abcde));
  assert(abcde >= abc);
  assert(!(abc >= cde));
  assert(cde >= abc);
}

void test_ends_with() {
  grnxx::Bytes bytes("cde");

  assert(bytes.ends_with(""));
  assert(bytes.ends_with("e"));
  assert(bytes.ends_with("de"));
  assert(bytes.ends_with("cde"));
  assert(!bytes.ends_with("bcde"));
  assert(!bytes.ends_with("abc"));
}

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_constructors();
  test_extract();
  test_trim();
  test_prefix();
  test_suffix();
  test_except_prefix();
  test_except_suffix();
  test_compare();
  test_equal_to();
  test_not_equal_to();
  test_less();
  test_greater();
  test_less_equal();
  test_greater_equal();
  test_starts_with();
  test_ends_with();

  return 0;
}
