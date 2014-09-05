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
#include "grnxx/error.hpp"

std::mt19937_64 mersenne_twister;

void test_bool() {
  grnxx::Error error;

  grnxx::Array<grnxx::Bool> array;
  assert(array.size() == 0);
  assert(array.capacity() == 0);

  assert(array.push_back(&error, true));
  assert(array.size() == 1);
  assert(array.capacity() == 64);
  assert(array[0]);

  assert(array.push_back(&error, false));
  assert(array.size() == 2);
  assert(array.capacity() == 64);
  assert(array[0]);
  assert(!array[1]);

  assert(array.resize(&error, 200, true));
  assert(array.size() == 200);
  assert(array.capacity() == 256);
  assert(array[0]);
  assert(!array[1]);
  for (grnxx::Int i = 2; i < 200; ++i) {
    assert(array.get(i));
  }

  constexpr grnxx::Int NUM_ROWS = 1 << 20;

  assert(array.resize(&error, NUM_ROWS, false));
  assert(array.size() == NUM_ROWS);
  assert(array.capacity() == NUM_ROWS);
  assert(array[0]);
  assert(!array[1]);
  for (grnxx::Int i = 2; i < 200; ++i) {
    assert(array.get(i));
  }
  for (grnxx::Int i = 200; i < NUM_ROWS; ++i) {
    assert(!array.get(i));
  }

  std::vector<bool> values(NUM_ROWS);
  for (grnxx::Int i = 0; i < NUM_ROWS; ++i) {
    values[i] = (mersenne_twister() & 1) != 0;
    array.set(i, values[i]);
    assert(array[i] == values[i]);
  }

  for (grnxx::Int i = 0; i < NUM_ROWS; ++i) {
    assert(array[i] == values[i]);
  }
}

int main() {
  test_bool();
  return 0;
}
