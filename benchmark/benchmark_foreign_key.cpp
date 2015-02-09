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
#include <ctime>
#include <iostream>
#include <random>
#include <set>

#include "grnxx/db.hpp"
#include "grnxx/pipeline.hpp"

namespace {

constexpr size_t VALUES_SIZE = 10000;
constexpr size_t REFS_SIZE = 1000000;
constexpr size_t LOOP = 5;

class Timer {
 public:
  Timer() : base_(now()) {}

  double elapsed() const {
    return now() - base_;
  }

  static double now() {
    struct timespec ts;
    ::clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + (ts.tv_nsec / 1000000000.0);
  }

 private:
  double base_;
};

grnxx::Array<grnxx::Text> values;
grnxx::Array<grnxx::String> bodies;
grnxx::Array<grnxx::Int> refs;

void generate_data() {
  std::mt19937_64 rng;

  std::set<grnxx::String> set;
  while (set.size() < VALUES_SIZE) {
    constexpr size_t MIN_SIZE = 16;
    constexpr size_t MAX_SIZE = 255;
    grnxx::String string(MIN_SIZE + (rng() % (MAX_SIZE - MIN_SIZE + 1)));
    for (size_t i = 0; i < string.size(); ++i) {
      string[i] = '0' + (rng() % 10);
    }
    if (set.insert(string).second) {
      values.push_back(grnxx::Text(string.data(), string.size()));
      bodies.push_back(std::move(string));
    }
  }

  refs.resize(REFS_SIZE);
  for (size_t i = 0; i < REFS_SIZE; ++i) {
    refs[i] = grnxx::Int(rng() % VALUES_SIZE);
  }
}

void benchmark_direct_build() try {
  std::cout << __PRETTY_FUNCTION__ << std::endl;

  double min_elapsed = std::numeric_limits<double>::max();
  for (size_t i = 0; i < LOOP; ++i) {
    Timer timer;

    auto db = grnxx::open_db("");
    auto table = db->create_table("Values");
    auto column = table->create_column("Value", GRNXX_TEXT);
    for (size_t i = 0; i < VALUES_SIZE; ++i) {
      grnxx::Int row_id = table->insert_row();
      column->set(row_id, values[i]);
    }

    table = db->create_table("Refs");
    grnxx::ColumnOptions options;
    options.reference_table_name = "Values";
    column = table->create_column("Ref", GRNXX_INT, options);
    for (size_t i = 0; i < REFS_SIZE; ++i) {
      grnxx::Int row_id = table->insert_row();
      column->set(row_id, refs[i]);
    }

    double elapsed = timer.elapsed();
    if (elapsed < min_elapsed) {
      min_elapsed = elapsed;
    }
  }
  std::cout << "min. elapsed [s] = " << min_elapsed << std::endl;
} catch (const char *message) {
  std::cout << "message = " << message << std::endl;
}

void benchmark_indirect_build() try {
  std::cout << __PRETTY_FUNCTION__ << std::endl;

  double min_elapsed = std::numeric_limits<double>::max();
  for (size_t i = 0; i < LOOP; ++i) {
    Timer timer;

    auto db = grnxx::open_db("");
    auto to_table = db->create_table("Values");
    auto column = to_table->create_column("Value", GRNXX_TEXT);
    column->create_index("Index", GRNXX_TREE_INDEX);
    to_table->set_key_column("Value");
    for (size_t j = 0; j < VALUES_SIZE; ++j) {
      to_table->insert_row(values[j]);
    }

    auto from_table = db->create_table("Refs");
    grnxx::ColumnOptions options;
    options.reference_table_name = "Values";
    column = from_table->create_column("Ref", GRNXX_INT, options);
    for (size_t j = 0; j < REFS_SIZE; ++j) {
      grnxx::Int row_id = from_table->insert_row();
      column->set(row_id, to_table->find_row(values[refs[j].raw()]));
    }

    double elapsed = timer.elapsed();
    if (elapsed < min_elapsed) {
      min_elapsed = elapsed;
    }
  }
  std::cout << "min. elapsed [s] = " << min_elapsed << std::endl;
} catch (const char *message) {
  std::cout << "message = " << message << std::endl;
}

void benchmark_sequential_build() try {
  std::cout << __PRETTY_FUNCTION__ << std::endl;

  double min_elapsed = std::numeric_limits<double>::max();
  for (size_t i = 0; i < LOOP; ++i) {
    Timer timer;

    auto db = grnxx::open_db("");
    auto to_table = db->create_table("Values");
    auto value_column = to_table->create_column("Value", GRNXX_TEXT);
    value_column->create_index("Index", GRNXX_TREE_INDEX);
    to_table->set_key_column("Value");
    auto from_table = db->create_table("Refs");
    grnxx::ColumnOptions options;
    options.reference_table_name = "Values";
    auto ref_column =
        from_table->create_column("Ref", GRNXX_INT, options);
    for (size_t j = 0; j < REFS_SIZE; ++j) {
      grnxx::Int row_id = from_table->insert_row();
      grnxx::Int ref = to_table->find_or_insert_row(values[refs[j].raw()]);
      ref_column->set(row_id, ref);
    }

    double elapsed = timer.elapsed();
    if (elapsed < min_elapsed) {
      min_elapsed = elapsed;
    }
  }
  std::cout << "min. elapsed [s] = " << min_elapsed << std::endl;
} catch (const char *message) {
  std::cout << "message = " << message << std::endl;
}

}  // namespace

int main() {
  generate_data();

  benchmark_direct_build();
  benchmark_indirect_build();
  benchmark_sequential_build();

  return 0;
}
