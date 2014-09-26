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
#include <algorithm>
#include <cassert>
#include <iostream>
#include <string>
#include <sstream>
#include <vector>

#include "grnxx/types.hpp"

bool string_starts_with(const std::string &lhs, const std::string &rhs) {
  if (lhs.size() < rhs.size()) {
    return false;
  }
  return lhs.compare(0, rhs.size(), rhs) == 0;
}

bool string_ends_with(const std::string &lhs, const std::string &rhs) {
  if (lhs.size() < rhs.size()) {
    return false;
  }
  return lhs.compare(lhs.size() - rhs.size(), rhs.size(), rhs) == 0;
}

void test_string_cref() {
  constexpr grnxx::Int NUM_STRINGS = 1000;

  std::vector<std::string> strings(NUM_STRINGS);
  std::vector<grnxx::StringCRef> refs(NUM_STRINGS);
  for (grnxx::Int i = 0; i < NUM_STRINGS; ++i) {
    std::stringstream stream;
    stream << i;
    strings[i] = stream.str();
    refs[i] = grnxx::StringCRef(strings[i].data(), strings[i].size());
  }

  for (grnxx::Int i = 0; i < NUM_STRINGS; ++i) {
    for (grnxx::Int j = 0; j < NUM_STRINGS; ++j) {
      assert((refs[i] == refs[j]) == (strings[i] == strings[j]));
      assert((refs[i] != refs[j]) == (strings[i] != strings[j]));
      assert((refs[i] < refs[j]) == (strings[i] < strings[j]));
      assert((refs[i] > refs[j]) == (strings[i] > strings[j]));
      assert((refs[i] <= refs[j]) == (strings[i] <= strings[j]));
      assert((refs[i] >= refs[j]) == (strings[i] >= strings[j]));

      assert((refs[i] == strings[j].c_str()) == (strings[i] == strings[j]));
      assert((refs[i] != strings[j].c_str()) == (strings[i] != strings[j]));
      assert((refs[i] < strings[j].c_str()) == (strings[i] < strings[j]));
      assert((refs[i] > strings[j].c_str()) == (strings[i] > strings[j]));
      assert((refs[i] <= strings[j].c_str()) == (strings[i] <= strings[j]));
      assert((refs[i] >= strings[j].c_str()) == (strings[i] >= strings[j]));

      assert((strings[i].c_str() == refs[j]) == (strings[i] == strings[j]));
      assert((strings[i].c_str() != refs[j]) == (strings[i] != strings[j]));
      assert((strings[i].c_str() < refs[j]) == (strings[i] < strings[j]));
      assert((strings[i].c_str() > refs[j]) == (strings[i] > strings[j]));
      assert((strings[i].c_str() <= refs[j]) == (strings[i] <= strings[j]));
      assert((strings[i].c_str() >= refs[j]) == (strings[i] >= strings[j]));

      assert(refs[i].starts_with(refs[j]) ==
             string_starts_with(strings[i], strings[j]));
      assert(refs[i].starts_with(strings[j].c_str()) ==
             string_starts_with(strings[i], strings[j]));
      assert(refs[i].ends_with(refs[j]) ==
             string_ends_with(strings[i], strings[j]));
      assert(refs[i].ends_with(strings[j].c_str()) ==
             string_ends_with(strings[i], strings[j]));
    }
  }
}

int main() {
  test_string_cref();
  return 0;
}
