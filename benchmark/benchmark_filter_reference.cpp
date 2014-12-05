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

#include "grnxx/db.hpp"
#include "grnxx/pipeline.hpp"

namespace {

constexpr size_t TO_SIZE = 100000;
constexpr size_t FROM_SIZE = 10000000;
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

grnxx::Array<grnxx::Int> a;
grnxx::Array<grnxx::Int> b;
grnxx::Array<grnxx::Int> c;
grnxx::Array<grnxx::Int> ref;

void generate_data() {
  std::mt19937_64 rng;
  a.resize(TO_SIZE);
  b.resize(TO_SIZE);
  c.resize(TO_SIZE);
  for (size_t i = 0; i < TO_SIZE; ++i) {
    a[i] = grnxx::Int(rng() % 256);
    b[i] = grnxx::Int(rng() % 256);
    c[i] = grnxx::Int(rng() % 256);
  }
  ref.resize(FROM_SIZE);
  for (size_t i = 0; i < FROM_SIZE; ++i) {
    ref[i] = grnxx::Int(rng() % TO_SIZE);
  }
}

void benchmark_grnxx(grnxx::Table *table,
                     grnxx::OperatorType logical_operator_type,
                     const grnxx::Array<grnxx::String> &column_names,
                     grnxx::Int upper_limit) {

  std::cout << "ratio = " << (100 * upper_limit.raw() / 256) << '%';

  // Use subexpressions to get column values.
  double min_elapsed = std::numeric_limits<double>::max();
  for (size_t i = 0; i < LOOP; ++i) {
    Timer timer;

    auto pipeline_builder = grnxx::PipelineBuilder::create(table);
    auto cursor = table->create_cursor();
    pipeline_builder->push_cursor(std::move(cursor));
    auto expression_builder = grnxx::ExpressionBuilder::create(table);
    for (size_t j = 0; j < column_names.size(); ++j) {
      expression_builder->push_column("Ref");
      expression_builder->begin_subexpression();
      expression_builder->push_column(column_names[j]);
      expression_builder->end_subexpression();
      expression_builder->push_constant(upper_limit);
      expression_builder->push_operator(grnxx::LESS_OPERATOR);
    }
    for (size_t j = 1; j < column_names.size(); ++j) {
      expression_builder->push_operator(logical_operator_type);
    }
    auto expression = expression_builder->release();
    pipeline_builder->push_filter(std::move(expression));
    auto pipeline = pipeline_builder->release();
    grnxx::Array<grnxx::Record> records;
    pipeline->flush(&records);

    double elapsed = timer.elapsed();
    if (elapsed < min_elapsed) {
      min_elapsed = elapsed;
    }
  }
  std::cout << ", min. elapsed [s] = " << min_elapsed;

  // Use a subexpression to get evaluation results.
  min_elapsed = std::numeric_limits<double>::max();
  for (size_t i = 0; i < LOOP; ++i) {
    Timer timer;

    auto pipeline_builder = grnxx::PipelineBuilder::create(table);
    auto cursor = table->create_cursor();
    pipeline_builder->push_cursor(std::move(cursor));
    auto expression_builder = grnxx::ExpressionBuilder::create(table);
    expression_builder->push_column("Ref");
    expression_builder->begin_subexpression();
    for (size_t j = 0; j < column_names.size(); ++j) {
      expression_builder->push_column(column_names[j]);
      expression_builder->push_constant(upper_limit);
      expression_builder->push_operator(grnxx::LESS_OPERATOR);
    }
    for (size_t j = 1; j < column_names.size(); ++j) {
      expression_builder->push_operator(logical_operator_type);
    }
    expression_builder->end_subexpression();
    auto expression = expression_builder->release();
    pipeline_builder->push_filter(std::move(expression));
    auto pipeline = pipeline_builder->release();
    grnxx::Array<grnxx::Record> records;
    pipeline->flush(&records);

    double elapsed = timer.elapsed();
    if (elapsed < min_elapsed) {
      min_elapsed = elapsed;
    }
  }
  std::cout << ", " << min_elapsed << std::endl;
}

void benchmark_grnxx(grnxx::Table *table,
                     grnxx::OperatorType logical_operator_type,
                     const char *column_names) {
  switch (logical_operator_type) {
    case grnxx::LOGICAL_AND_OPERATOR: {
      std::cout << "LOGICAL_AND: ";
      break;
    }
    case grnxx::LOGICAL_OR_OPERATOR: {
      std::cout << "LOGICAL_OR: ";
      break;
    }
    case grnxx::BITWISE_AND_OPERATOR: {
      std::cout << "BITWISE_AND: ";
      break;
    }
    case grnxx::BITWISE_OR_OPERATOR: {
      std::cout << "BITWISE_OR: ";
      break;
    }
    default: {
      break;
    }
  }
  std::cout << column_names << ':' << std::endl;

  // Parse "column_names" as comma-separated column names.
  grnxx::Array<grnxx::String> column_name_array;
  grnxx::String string(column_names);
  while (!string.is_empty()) {
    size_t delim_pos = 0;
    while (delim_pos < string.size()) {
      if (string[delim_pos] == ',') {
        break;
      }
      ++delim_pos;
    }
    column_name_array.push_back(string.substring(0, delim_pos));
    if (delim_pos == string.size()) {
      break;
    }
    string = string.substring(delim_pos + 1);
  }

  benchmark_grnxx(table, logical_operator_type, column_name_array,
                  grnxx::Int(16));
  benchmark_grnxx(table, logical_operator_type, column_name_array,
                  grnxx::Int(32));
  benchmark_grnxx(table, logical_operator_type, column_name_array,
                  grnxx::Int(64));
  benchmark_grnxx(table, logical_operator_type, column_name_array,
                  grnxx::Int(128));
  benchmark_grnxx(table, logical_operator_type, column_name_array,
                  grnxx::Int(192));
  benchmark_grnxx(table, logical_operator_type, column_name_array,
                  grnxx::Int(224));
  benchmark_grnxx(table, logical_operator_type, column_name_array,
                  grnxx::Int(240));
}

void benchmark_grnxx(grnxx::Table *table) {
  benchmark_grnxx(table, grnxx::LOGICAL_AND_OPERATOR, "A");
  benchmark_grnxx(table, grnxx::LOGICAL_AND_OPERATOR, "A,B");
  benchmark_grnxx(table, grnxx::LOGICAL_AND_OPERATOR, "A,B,C");
  benchmark_grnxx(table, grnxx::LOGICAL_OR_OPERATOR, "A,B");
  benchmark_grnxx(table, grnxx::LOGICAL_OR_OPERATOR, "A,B,C");

  benchmark_grnxx(table, grnxx::BITWISE_AND_OPERATOR, "A");
  benchmark_grnxx(table, grnxx::BITWISE_AND_OPERATOR, "A,B");
  benchmark_grnxx(table, grnxx::BITWISE_AND_OPERATOR, "A,B,C");
  benchmark_grnxx(table, grnxx::BITWISE_OR_OPERATOR, "A,B");
  benchmark_grnxx(table, grnxx::BITWISE_OR_OPERATOR, "A,B,C");
}

void benchmark_grnxx() {
  std::cout << __PRETTY_FUNCTION__ << std::endl;

  auto db = grnxx::open_db("");
  auto table = db->create_table("To");
  auto col_a = table->create_column("A", grnxx::INT_DATA);
  auto col_b = table->create_column("B", grnxx::INT_DATA);
  auto col_c = table->create_column("C", grnxx::INT_DATA);
  for (size_t i = 0; i < TO_SIZE; ++i) {
    grnxx::Int row_id = table->insert_row();
    col_a->set(row_id, a[i]);
    col_b->set(row_id, b[i]);
    col_c->set(row_id, c[i]);
  }
  table = db->create_table("From");
  grnxx::ColumnOptions options;
  options.reference_table_name = "To";
  auto col_ref = table->create_column("Ref", grnxx::INT_DATA, options);
  for (size_t i = 0; i < FROM_SIZE; ++i) {
    grnxx::Int row_id = table->insert_row();
    col_ref->set(row_id, ref[i]);
  }
  benchmark_grnxx(table);
}

template <typename T>
void benchmark_native(grnxx::Int upper_limit, T filter) {
  std::cout << "ratio = " << (100 * upper_limit.raw() / 256) << '%';
  double min_elapsed = std::numeric_limits<double>::max();
  for (size_t i = 0; i < LOOP; ++i) {
    Timer timer;

    // NOTE: This assumes that there are no invalid references.
    grnxx::Array<grnxx::Record> records;
    for (size_t j = 0; j < FROM_SIZE; ++j) {
      if (filter(upper_limit, j)) {
        records.push_back(grnxx::Record(grnxx::Int(j), grnxx::Float(0.0)));
      }
    }

    double elapsed = timer.elapsed();
    if (elapsed < min_elapsed) {
      min_elapsed = elapsed;
    }
  }
  std::cout << ", min. elapsed [s] = " << min_elapsed << std::endl;
}

struct FilterA {
  const char *name() const { return "LOGICAL_AND: A"; }
  bool operator()(grnxx::Int upper_limit, size_t i) const {
    return (a[ref[i].raw()] < upper_limit).is_true();
  }
};

struct AndFilterAB {
  const char *name() const { return "LOGICAL_AND: A,B"; }
  bool operator()(grnxx::Int upper_limit, size_t i) const {
    return (a[ref[i].raw()] < upper_limit).is_true() &&
           (b[ref[i].raw()] < upper_limit).is_true();
  }
};

struct AndFilterABC {
  const char *name() const { return "LOGICAL_AND: A,B,C"; }
  bool operator()(grnxx::Int upper_limit, size_t i) const {
    return (a[ref[i].raw()] < upper_limit).is_true() &&
           (b[ref[i].raw()] < upper_limit).is_true() &&
           (c[ref[i].raw()] < upper_limit).is_true();
  }
};

struct OrFilterAB {
  const char *name() const { return "LOGICAL_OR: A,B"; }
  bool operator()(grnxx::Int upper_limit, size_t i) const {
    return (a[ref[i].raw()] < upper_limit).is_true() ||
           (b[ref[i].raw()] < upper_limit).is_true();
  }
};

struct OrFilterABC {
  const char *name() const { return "LOGICAL_OR: A,B,C"; }
  bool operator()(grnxx::Int upper_limit, size_t i) const {
    return (a[ref[i].raw()] < upper_limit).is_true() ||
           (b[ref[i].raw()] < upper_limit).is_true() ||
           (c[ref[i].raw()] < upper_limit).is_true();
  }
};

template <typename T>
void benchmark_native(T filter) {
  std::cout << filter.name() << ':' << std::endl;

  benchmark_native(grnxx::Int(16), filter);
  benchmark_native(grnxx::Int(32), filter);
  benchmark_native(grnxx::Int(64), filter);
  benchmark_native(grnxx::Int(128), filter);
  benchmark_native(grnxx::Int(192), filter);
  benchmark_native(grnxx::Int(224), filter);
  benchmark_native(grnxx::Int(240), filter);
}

void benchmark_native() {
  std::cout << __PRETTY_FUNCTION__ << std::endl;

  benchmark_native(FilterA());
  benchmark_native(AndFilterAB());
  benchmark_native(AndFilterABC());
  benchmark_native(OrFilterAB());
  benchmark_native(OrFilterABC());
}

}  // namespace

int main() {
  generate_data();

  benchmark_grnxx();
  benchmark_native();

  return 0;
}
