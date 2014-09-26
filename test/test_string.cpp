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
    assert(refs[i].size() == static_cast<grnxx::Int>(strings[i].size()));
    for (grnxx::Int j = 0; j < refs[i].size(); ++j) {
      assert(refs[i][j] == strings[i][j]);
    }

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

void test_string() {
  constexpr grnxx::Int NUM_STRINGS = 1000;

  grnxx::Error error;

  std::vector<std::string> strings(NUM_STRINGS);
  std::vector<grnxx::StringCRef> refs(NUM_STRINGS);
  std::vector<grnxx::String> bodies(NUM_STRINGS);
  for (grnxx::Int i = 0; i < NUM_STRINGS; ++i) {
    std::stringstream stream;
    stream << i;
    strings[i] = stream.str();
    refs[i] = grnxx::StringCRef(strings[i].data(), strings[i].size());
    assert(bodies[i].assign(&error, refs[i]));
  }

  for (grnxx::Int i = 0; i < NUM_STRINGS; ++i) {
    assert(bodies[i].size() == static_cast<grnxx::Int>(strings[i].size()));
    for (grnxx::Int j = 0; j < bodies[i].size(); ++j) {
      assert(bodies[i][j] == strings[i][j]);
    }

    for (grnxx::Int j = 0; j < NUM_STRINGS; ++j) {
      assert((bodies[i] == bodies[j]) == (strings[i] == strings[j]));
      assert((bodies[i] != bodies[j]) == (strings[i] != strings[j]));
      assert((bodies[i] < bodies[j]) == (strings[i] < strings[j]));
      assert((bodies[i] > bodies[j]) == (strings[i] > strings[j]));
      assert((bodies[i] <= bodies[j]) == (strings[i] <= strings[j]));
      assert((bodies[i] >= bodies[j]) == (strings[i] >= strings[j]));

      assert((bodies[i] == refs[j]) == (strings[i] == strings[j]));
      assert((bodies[i] != refs[j]) == (strings[i] != strings[j]));
      assert((bodies[i] < refs[j]) == (strings[i] < strings[j]));
      assert((bodies[i] > refs[j]) == (strings[i] > strings[j]));
      assert((bodies[i] <= refs[j]) == (strings[i] <= strings[j]));
      assert((bodies[i] >= refs[j]) == (strings[i] >= strings[j]));

      assert((bodies[i] == strings[j].c_str()) == (strings[i] == strings[j]));
      assert((bodies[i] != strings[j].c_str()) == (strings[i] != strings[j]));
      assert((bodies[i] < strings[j].c_str()) == (strings[i] < strings[j]));
      assert((bodies[i] > strings[j].c_str()) == (strings[i] > strings[j]));
      assert((bodies[i] <= strings[j].c_str()) == (strings[i] <= strings[j]));
      assert((bodies[i] >= strings[j].c_str()) == (strings[i] >= strings[j]));

      assert((refs[i] == bodies[j]) == (strings[i] == strings[j]));
      assert((refs[i] != bodies[j]) == (strings[i] != strings[j]));
      assert((refs[i] < bodies[j]) == (strings[i] < strings[j]));
      assert((refs[i] > bodies[j]) == (strings[i] > strings[j]));
      assert((refs[i] <= bodies[j]) == (strings[i] <= strings[j]));
      assert((refs[i] >= bodies[j]) == (strings[i] >= strings[j]));

      assert((strings[i].c_str() == bodies[j]) == (strings[i] == strings[j]));
      assert((strings[i].c_str() != bodies[j]) == (strings[i] != strings[j]));
      assert((strings[i].c_str() < bodies[j]) == (strings[i] < strings[j]));
      assert((strings[i].c_str() > bodies[j]) == (strings[i] > strings[j]));
      assert((strings[i].c_str() <= bodies[j]) == (strings[i] <= strings[j]));
      assert((strings[i].c_str() >= bodies[j]) == (strings[i] >= strings[j]));

      assert(bodies[i].starts_with(bodies[j]) ==
             string_starts_with(strings[i], strings[j]));
      assert(bodies[i].starts_with(strings[j].c_str()) ==
             string_starts_with(strings[i], strings[j]));
      assert(bodies[i].ends_with(bodies[j]) ==
             string_ends_with(strings[i], strings[j]));
      assert(bodies[i].ends_with(strings[j].c_str()) ==
             string_ends_with(strings[i], strings[j]));
    }
  }

  for (grnxx::Int i = 0; i < NUM_STRINGS; ++i) {
    std::stringstream stream;
    stream << (i / 2.0);
    std::string extra_string = stream.str();
    strings[i].append(extra_string);
    assert(bodies[i].append(&error, extra_string.data(), extra_string.size()));
    assert(bodies[i] ==
           grnxx::StringCRef(strings[i].data(), strings[i].size()));
  }
}

int main() {
  test_string_cref();
  test_string();
  return 0;
}
