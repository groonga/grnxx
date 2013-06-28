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
#include <type_traits>

#include "grnxx/logger.hpp"
#include "grnxx/traits.hpp"

namespace {

// This type should use pass by reference.
struct Point {
  int x, y;
};

// This type has starts_with().
struct Something {
  bool starts_with(const Something &) const;
};

// An operator<() is declared for Something.
//bool operator<(const Something &, const Something &);

// This type's starts_with() is invalid because it does not take Something2.
// This type has operator<().
struct Something2 {
  bool starts_with(const Point &) const;
  bool operator<(const Something2 &) const;
};

void test_type() {
  assert((std::is_same<int, typename grnxx::Traits<int>::Type>::value));
  assert((std::is_same<Point, typename grnxx::Traits<Point>::Type>::value));
}

void test_argument_type() {
  assert((std::is_same<int,
                       typename grnxx::Traits<int>::ArgumentType>::value));
  assert((std::is_same<const Point &,
                       typename grnxx::Traits<Point>::ArgumentType>::value));
}

//void test_has_less() {
//  assert(grnxx::Traits<int>::has_less());
//  assert(!grnxx::Traits<Point>::has_less());
//  assert(grnxx::Traits<Something>::has_less());
//  assert(grnxx::Traits<Something2>::has_less());
//}

//void test_has_starts_with() {
//  assert(!grnxx::Traits<int>::has_starts_with());
//  assert(!grnxx::Traits<Point>::has_starts_with());
//  assert(grnxx::Traits<Something>::has_starts_with());
//  assert(!grnxx::Traits<Something2>::has_starts_with());
//}

}  // namespace

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_type();
  test_argument_type();
//  test_has_less();
//  test_has_starts_with();

  return 0;
}
