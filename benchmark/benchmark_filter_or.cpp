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
#include <functional>
#include <iostream>
#include <random>

#include "grnxx/array.hpp"
#include "grnxx/data_types.hpp"
#include "grnxx/db.hpp"
#include "grnxx/pipeline.hpp"

namespace {

constexpr size_t SIZE = 10000000;
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

void generate_data() {
  std::mt19937_64 rng;
  a.resize(SIZE);
  b.resize(SIZE);
  c.resize(SIZE);
  for (size_t i = 0; i < SIZE; ++i) {
    a[i] = grnxx::Int(rng() % 256);
    b[i] = grnxx::Int(rng() % 256);
    c[i] = grnxx::Int(rng() % 256);
  }
}

void benchmark_grnxx_not_and(const grnxx::Table *table,
                             grnxx::Int upper_limit) {
  std::cout << "LOGICAL_NOT/AND: ";
  std::cout << "ratio = " << (100 * upper_limit.raw() / 256) << '%';
  double min_elapsed = std::numeric_limits<double>::max();
  for (size_t i = 0; i < LOOP; ++i) {
    Timer timer;

    auto pipeline_builder = grnxx::PipelineBuilder::create(table);
    auto cursor = table->create_cursor();
    pipeline_builder->push_cursor(std::move(cursor));
    auto expression_builder = grnxx::ExpressionBuilder::create(table);
    expression_builder->push_column("A");
    expression_builder->push_constant(upper_limit);
    expression_builder->push_operator(GRNXX_GREATER_EQUAL);
    expression_builder->push_column("B");
    expression_builder->push_constant(upper_limit);
    expression_builder->push_operator(GRNXX_GREATER_EQUAL);
    expression_builder->push_column("C");
    expression_builder->push_constant(upper_limit);
    expression_builder->push_operator(GRNXX_GREATER_EQUAL);
    expression_builder->push_operator(GRNXX_LOGICAL_AND);
    expression_builder->push_operator(GRNXX_LOGICAL_AND);
    expression_builder->push_operator(GRNXX_LOGICAL_NOT);
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
  std::cout << ", min. elapsed [s] = " << min_elapsed << std::endl;
}

void benchmark_grnxx_not_and(const grnxx::Table *table) {
  benchmark_grnxx_not_and(table, grnxx::Int(16));
  benchmark_grnxx_not_and(table, grnxx::Int(32));
  benchmark_grnxx_not_and(table, grnxx::Int(64));
  benchmark_grnxx_not_and(table, grnxx::Int(128));
  benchmark_grnxx_not_and(table, grnxx::Int(192));
  benchmark_grnxx_not_and(table, grnxx::Int(224));
  benchmark_grnxx_not_and(table, grnxx::Int(240));
}

void benchmark_grnxx(const grnxx::Table *table,
                     grnxx::OperatorType logical_operator_type,
                     grnxx::Int upper_limit) {
  switch (logical_operator_type) {
    case GRNXX_LOGICAL_OR: {
      std::cout << "LOGICAL_OR: ";
      break;
    }
    case GRNXX_BITWISE_OR: {
      std::cout << "BITWISE_OR: ";
      break;
    }
    default: {
      break;
    }
  }
  std::cout << "ratio = " << (100 * upper_limit.raw() / 256) << '%';
  double min_elapsed = std::numeric_limits<double>::max();
  for (size_t i = 0; i < LOOP; ++i) {
    Timer timer;

    auto pipeline_builder = grnxx::PipelineBuilder::create(table);
    auto cursor = table->create_cursor();
    pipeline_builder->push_cursor(std::move(cursor));
    auto expression_builder = grnxx::ExpressionBuilder::create(table);
    expression_builder->push_column("A");
    expression_builder->push_constant(upper_limit);
    expression_builder->push_operator(GRNXX_LESS);
    expression_builder->push_column("B");
    expression_builder->push_constant(upper_limit);
    expression_builder->push_operator(GRNXX_LESS);
    expression_builder->push_column("C");
    expression_builder->push_constant(upper_limit);
    expression_builder->push_operator(GRNXX_LESS);
    expression_builder->push_operator(logical_operator_type);
    expression_builder->push_operator(logical_operator_type);
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
  std::cout << ", min. elapsed [s] = " << min_elapsed << std::endl;
}

void benchmark_grnxx(const grnxx::Table *table,
                     grnxx::OperatorType logical_operator_type) {
  benchmark_grnxx(table, logical_operator_type, grnxx::Int(16));
  benchmark_grnxx(table, logical_operator_type, grnxx::Int(32));
  benchmark_grnxx(table, logical_operator_type, grnxx::Int(64));
  benchmark_grnxx(table, logical_operator_type, grnxx::Int(128));
  benchmark_grnxx(table, logical_operator_type, grnxx::Int(192));
  benchmark_grnxx(table, logical_operator_type, grnxx::Int(224));
  benchmark_grnxx(table, logical_operator_type, grnxx::Int(240));
}

void benchmark_grnxx() {
  std::cout << __PRETTY_FUNCTION__ << std::endl;

  auto db = grnxx::open_db("");
  auto table = db->create_table("Table");
  auto col_a = table->create_column("A", GRNXX_INT);
  auto col_b = table->create_column("B", GRNXX_INT);
  auto col_c = table->create_column("C", GRNXX_INT);
  for (size_t i = 0; i < SIZE; ++i) {
    grnxx::Int row_id = table->insert_row();
    col_a->set(row_id, a[i]);
    col_b->set(row_id, b[i]);
    col_c->set(row_id, c[i]);
  }

  benchmark_grnxx(table, GRNXX_LOGICAL_OR);
  benchmark_grnxx(table, GRNXX_BITWISE_OR);
  benchmark_grnxx_not_and(table);
}

void benchmark_native_batch(grnxx::Int upper_limit) {
  std::cout << "LOGICAL_OR: ";
  std::cout << "ratio = " << (100 * upper_limit.raw() / 256) << '%';
  double min_elapsed = std::numeric_limits<double>::max();
  for (size_t i = 0; i < LOOP; ++i) {
    Timer timer;

    grnxx::Array<grnxx::Record> records;
    records.resize(SIZE);
    for (size_t j = 0; j < SIZE; ++j) {
      records[j].row_id = grnxx::Int(j);
      records[j].score = grnxx::Float(0.0);
    }
    size_t count = 0;
    for (size_t j = 0; j < SIZE; ++j) {
      if ((a[records[j].row_id.raw()] < upper_limit).is_true() ||
          (b[records[j].row_id.raw()] < upper_limit).is_true() ||
          (c[records[j].row_id.raw()] < upper_limit).is_true()) {
        records[count] = grnxx::Record(grnxx::Int(j), grnxx::Float(0.0));
        ++count;
      }
    }
    records.resize(count);

    double elapsed = timer.elapsed();
    if (elapsed < min_elapsed) {
      min_elapsed = elapsed;
    }
  }
  std::cout << ", min. elapsed [s] = " << min_elapsed << std::endl;
}

void benchmark_native_batch() {
  std::cout << __PRETTY_FUNCTION__ << std::endl;

  benchmark_native_batch(grnxx::Int(16));
  benchmark_native_batch(grnxx::Int(32));
  benchmark_native_batch(grnxx::Int(64));
  benchmark_native_batch(grnxx::Int(128));
  benchmark_native_batch(grnxx::Int(192));
  benchmark_native_batch(grnxx::Int(224));
  benchmark_native_batch(grnxx::Int(240));
}

void benchmark_native_sequential(grnxx::Int upper_limit) {
  std::cout << "LOGICAL_OR: ";
  std::cout << "ratio = " << (100 * upper_limit.raw() / 256) << '%';
  double min_elapsed = std::numeric_limits<double>::max();
  for (size_t i = 0; i < LOOP; ++i) {
    Timer timer;

    grnxx::Array<grnxx::Record> records;
    for (size_t j = 0; j < SIZE; ++j) {
      if ((a[j] < upper_limit).is_true() ||
          (b[j] < upper_limit).is_true() ||
          (c[j] < upper_limit).is_true()) {
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

void benchmark_native_sequential() {
  std::cout << __PRETTY_FUNCTION__ << std::endl;

  benchmark_native_sequential(grnxx::Int(16));
  benchmark_native_sequential(grnxx::Int(32));
  benchmark_native_sequential(grnxx::Int(64));
  benchmark_native_sequential(grnxx::Int(128));
  benchmark_native_sequential(grnxx::Int(192));
  benchmark_native_sequential(grnxx::Int(224));
  benchmark_native_sequential(grnxx::Int(240));
}

void benchmark_native() {
  benchmark_native_batch();
  benchmark_native_sequential();
}

}  // namespace

int main() {
  generate_data();

  benchmark_grnxx();
  benchmark_native();

  return 0;
}
