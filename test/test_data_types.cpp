/*
  Copyright (C) 2012-2014  Brazil, Inc.

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
#include <iostream>

#include "grnxx/data_types.hpp"

void test_bool() {
  grnxx::Bool true_object(true);
  grnxx::Bool false_object(false);
  grnxx::Bool na_object((grnxx::NA()));

  assert(true_object.type() == grnxx::BOOL_DATA);
  assert(false_object.type() == grnxx::BOOL_DATA);
  assert(na_object.type() == grnxx::BOOL_DATA);

  assert(true_object.value() == grnxx::Bool::true_value());
  assert(false_object.value() == grnxx::Bool::false_value());
  assert(na_object.value() == grnxx::Bool::na_value());

  assert(true_object.is_true());
  assert(!true_object.is_false());
  assert(!true_object.is_na());

  assert(!false_object.is_true());
  assert(false_object.is_false());
  assert(!false_object.is_na());

  assert(!na_object.is_true());
  assert(!na_object.is_false());
  assert(na_object.is_na());

  assert(static_cast<bool>(true_object));
  assert(!static_cast<bool>(false_object));
  assert(!static_cast<bool>(na_object));

  assert(!true_object == false_object);
  assert(!false_object == true_object);
  assert((!na_object).is_na());

  assert(~true_object == false_object);
  assert(~false_object == true_object);
  assert((~na_object).is_na());

  assert((true_object & true_object) == true_object);
  assert((true_object & false_object) == false_object);
  assert((true_object & na_object).is_na());
  assert((false_object & true_object) == false_object);
  assert((false_object & false_object) == false_object);
  assert((false_object & na_object) == false_object);
  assert((na_object & true_object).is_na());
  assert((na_object & false_object) == false_object);
  assert((na_object & na_object).is_na());

  assert((true_object | true_object) == true_object);
  assert((true_object | false_object) == true_object);
  assert((true_object | na_object) == true_object);
  assert((false_object | true_object) == true_object);
  assert((false_object | false_object) == false_object);
  assert((false_object | na_object).is_na());
  assert((na_object | true_object) == true_object);
  assert((na_object | false_object).is_na());
  assert((na_object | na_object).is_na());

  assert((true_object ^ true_object) == false_object);
  assert((true_object ^ false_object) == true_object);
  assert((true_object ^ na_object).is_na());
  assert((false_object ^ true_object) == true_object);
  assert((false_object ^ false_object) == false_object);
  assert((false_object ^ na_object).is_na());
  assert((na_object ^ true_object).is_na());
  assert((na_object ^ false_object).is_na());
  assert((na_object ^ na_object).is_na());

  assert((true_object == true_object) == true_object);
  assert((true_object == false_object) == false_object);
  assert((true_object == na_object).is_na());
  assert((false_object == true_object) == false_object);
  assert((false_object == false_object) == true_object);
  assert((false_object == na_object).is_na());
  assert((na_object == true_object).is_na());
  assert((na_object == false_object).is_na());
  assert((na_object == na_object).is_na());

  assert((true_object != true_object) == false_object);
  assert((true_object != false_object) == true_object);
  assert((true_object != na_object).is_na());
  assert((false_object != true_object) == true_object);
  assert((false_object != false_object) == false_object);
  assert((false_object != na_object).is_na());
  assert((na_object != true_object).is_na());
  assert((na_object != false_object).is_na());
  assert((na_object != na_object).is_na());

  assert(grnxx::Bool::na().is_na());
}

void test_int() {
  // TODO
}

void test_float() {
  // TODO
}

void test_geo_point() {
  // TODO
}

int main() {
  test_bool();
  test_int();
  test_float();
  test_geo_point();
  return 0;
}
