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
#include <random>
#include <vector>

#include "grnxx/array.hpp"
#include "grnxx/data_types.hpp"

std::mt19937_64 mersenne_twister;

void test_bool() {
  grnxx::Array<grnxx::Bool> array;
  assert(array.size() == 0);
  assert(array.capacity() == 0);

  array.push_back(grnxx::Bool(true));
  assert(array.size() == 1);
  assert(array.capacity() == 1);
  assert(array[0]);

  array.push_back(grnxx::Bool(false));
  assert(array.size() == 2);
  assert(array.capacity() == 2);
  assert(array[0]);
  assert(!array[1]);

  array.push_back(grnxx::Bool(true));
  assert(array.size() == 3);
  assert(array.capacity() == 4);
  assert(array[0]);
  assert(!array[1]);
  assert(array[2]);

  array.resize(200, grnxx::Bool(true));
  assert(array.size() == 200);
  assert(array.capacity() == 200);
  assert(array[0]);
  assert(!array[1]);
  assert(array[2]);
  for (size_t i = 3; i < 200; ++i) {
    assert(array[i]);
  }

  constexpr size_t ARRAY_SIZE = 1 << 20;

  array.resize(ARRAY_SIZE, grnxx::Bool(false));
  assert(array.size() == ARRAY_SIZE);
  assert(array.capacity() == ARRAY_SIZE);
  assert(array[0]);
  assert(!array[1]);
  assert(array[2]);
  for (size_t i = 3; i < 200; ++i) {
    assert(array[i]);
  }
  for (size_t i = 200; i < ARRAY_SIZE; ++i) {
    assert(!array[i]);
  }

  std::vector<grnxx::Bool> values(ARRAY_SIZE);
  for (size_t i = 0; i < ARRAY_SIZE; ++i) {
    values[i] = grnxx::Bool((mersenne_twister() & 1) != 0);
    array.set(i, values[i]);
    assert(array.get(i) == values[i]);
  }
  for (size_t i = 0; i < ARRAY_SIZE; ++i) {
    assert(array.get(i) == values[i]);
  }

  grnxx::Array<grnxx::Bool> array2;
  array2 = std::move(array);
  assert(array.size() == 0);
  assert(array.capacity() == 0);
  assert(array2.size() == ARRAY_SIZE);
  assert(array2.capacity() == ARRAY_SIZE);
  for (size_t i = 0; i < ARRAY_SIZE; ++i) {
    assert(array2.get(i) == values[i]);
  }
}

void test_int() {
  grnxx::Array<grnxx::Int> array;
  assert(array.size() == 0);
  assert(array.capacity() == 0);

  array.push_back(grnxx::Int(123));
  assert(array.size() == 1);
  assert(array.capacity() == 1);
  assert(array[0] == grnxx::Int(123));

  array.push_back(grnxx::Int(456));
  assert(array.size() == 2);
  assert(array.capacity() == 2);
  assert(array[0] == grnxx::Int(123));
  assert(array[1] == grnxx::Int(456));

  array.push_back(grnxx::Int(789));
  assert(array.size() == 3);
  assert(array.capacity() == 4);
  assert(array[0] == grnxx::Int(123));
  assert(array[1] == grnxx::Int(456));
  assert(array[2] == grnxx::Int(789));

  array.resize(200, grnxx::Int(12345));
  assert(array.size() == 200);
  assert(array.capacity() == 200);
  assert(array[0] == grnxx::Int(123));
  assert(array[1] == grnxx::Int(456));
  assert(array[2] == grnxx::Int(789));
  for (size_t i = 3; i < 200; ++i) {
    assert(array[i] == grnxx::Int(12345));
  }

  constexpr size_t ARRAY_SIZE = 1 << 20;

  array.resize(ARRAY_SIZE, grnxx::Int(0));
  assert(array.size() == ARRAY_SIZE);
  assert(array.capacity() == ARRAY_SIZE);
  assert(array[0] == grnxx::Int(123));
  assert(array[1] == grnxx::Int(456));
  assert(array[2] == grnxx::Int(789));
  for (size_t i = 3; i < 200; ++i) {
    assert(array[i] == grnxx::Int(12345));
  }
  for (size_t i = 200; i < ARRAY_SIZE; ++i) {
    assert(array[i] == grnxx::Int(0));
  }

  std::vector<grnxx::Int> values(ARRAY_SIZE);
  for (size_t i = 0; i < ARRAY_SIZE; ++i) {
    grnxx::Int value = grnxx::Int(mersenne_twister());
    while (value.is_na()) {
      value = grnxx::Int(mersenne_twister());
    }
    values[i] = grnxx::Int(value);
    array.set(i, values[i]);
    assert(array.get(i) == values[i]);
  }
  for (size_t i = 0; i < ARRAY_SIZE; ++i) {
    assert(array[i] == values[i]);
  }

  grnxx::Array<grnxx::Int> array2;
  array2 = std::move(array);
  assert(array.size() == 0);
  assert(array.capacity() == 0);
  assert(array2.size() == ARRAY_SIZE);
  assert(array2.capacity() == ARRAY_SIZE);
  for (size_t i = 0; i < ARRAY_SIZE; ++i) {
    assert(array2[i] == values[i]);
  }
}

int main() {
  test_bool();
  test_int();
  return 0;
}
