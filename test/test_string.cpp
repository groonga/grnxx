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

#include "logger.hpp"
#include "string.hpp"
#include "time/stopwatch.hpp"

void test_constructors() {
  assert(!grnxx::String());

  assert(!grnxx::String(nullptr));
  assert(!grnxx::String(""));
  assert(grnxx::String("ABC"));

  assert(!grnxx::String(nullptr, 0));
  assert(!grnxx::String("ABC", 0));
  assert(grnxx::String("ABC", 3));

  assert(grnxx::String() == "");

  assert(grnxx::String(nullptr) == "");
  assert(grnxx::String("") == "");
  assert(grnxx::String("ABC") == "ABC");

  assert(grnxx::String(nullptr, 0) == "");
  assert(grnxx::String("ABC", 0) == "");
  assert(grnxx::String("ABC", 1) == "A");
  assert(grnxx::String("ABC", 2) == "AB");
  assert(grnxx::String("ABC", 3) == "ABC");
}

void test_assignment_operators() {
  grnxx::String str;

  str = nullptr;
  assert(!str);
  assert(str == "");

  str = "";
  assert(!str);
  assert(str == "");

  str = "123";
  assert(str);
  assert(str == "123");

  grnxx::String str2;

  str2 = str;
  assert(str2 == "123");
  assert(str2 == str);
}

void test_comparison_operators() {
  assert(grnxx::String("") == grnxx::String(""));
  assert(grnxx::String("") != grnxx::String("X"));

  assert(grnxx::String("ABC") != grnxx::String(""));
  assert(grnxx::String("ABC") != grnxx::String("A"));
  assert(grnxx::String("ABC") != grnxx::String("AB"));
  assert(grnxx::String("ABC") == grnxx::String("ABC"));
  assert(grnxx::String("ABC") != grnxx::String("ABCD"));
}

void test_contains() {
  grnxx::String str = "BCD";

  assert(!str.contains('A'));
  assert(str.contains('B'));
  assert(str.contains('C'));
  assert(str.contains('D'));
  assert(!str.contains('E'));

  char buf[] = { 'X' ,'\0', 'Y' };
  str = grnxx::String(buf, sizeof(buf));

  assert(str.contains('X'));
  assert(str.contains('\0'));
  assert(str.contains('Y'));
}

void test_starts_with() {
  grnxx::String str = "This is a pen.";

  assert(str.starts_with(""));
  assert(str.starts_with("T"));
  assert(str.starts_with("This is"));
  assert(str.starts_with("This is a pen."));

  assert(!str.starts_with("XYZ"));
  assert(!str.starts_with("This is a pen.+XYZ"));
}

void test_ends_with() {
  grnxx::String str = "This is a pen.";

  assert(str.ends_with(""));
  assert(str.ends_with("."));
  assert(str.ends_with("a pen."));
  assert(str.ends_with("This is a pen."));

  assert(!str.ends_with("XYZ"));
  assert(!str.ends_with("XYZ+This is a pen."));
}

void test_swap() {
  grnxx::String str = "ABC";
  grnxx::String str2 = "XYZ";

  str.swap(str2);

  assert(str == "XYZ");
  assert(str2 == "ABC");

  using std::swap;
  swap(str, str2);

  assert(str == "ABC");
  assert(str2 == "XYZ");
}

void benchmark() {
  const int LOOP_COUNT = 1 << 16;

  grnxx::String str, str2;

  grnxx::Stopwatch stopwatch(true);
  for (int i = 0; i < LOOP_COUNT; ++i) {
    str = grnxx::String("This is an apple.");
  }
  grnxx::Duration elapsed = stopwatch.elapsed();
  GRNXX_NOTICE() << "string creation: elapsed [ns] = "
                 << (1000.0 * elapsed.count() / LOOP_COUNT);

  stopwatch.reset();
  for (int i = 0; i < LOOP_COUNT; ++i) {
    str2 = str;
  }
  elapsed = stopwatch.elapsed();

  GRNXX_NOTICE() << "string copy: elapsed [ns] = "
                 << (1000.0 * elapsed.count() / LOOP_COUNT);
}

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_constructors();
  test_assignment_operators();
  test_comparison_operators();
  test_contains();
  test_starts_with();
  test_ends_with();
  test_swap();

  benchmark();

  return 0;
}
