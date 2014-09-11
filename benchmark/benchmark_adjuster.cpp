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

#include "grnxx/column.hpp"
#include "grnxx/cursor.hpp"
#include "grnxx/db.hpp"
#include "grnxx/expression.hpp"
#include "grnxx/pipeline.hpp"
#include "grnxx/sorter.hpp"
#include "grnxx/table.hpp"

int NUM_ROWS  = 1 << 16;
int NUM_LOOPS = 5;

class Timer {
 public:
  Timer() : base_(now()) {}

  void reset() {
    base_ = now();
  }

  double elapsed() const {
    return now() - base_;
  }

 private:
  double base_;

  static double now() {
    struct timespec current_time;
    ::clock_gettime(CLOCK_MONOTONIC, &current_time);
    return current_time.tv_sec + (current_time.tv_nsec / 1000000000.0);
  }
};

struct {
  grnxx::unique_ptr<grnxx::DB> db;
  grnxx::Table *table;
  grnxx::Array<grnxx::Bool> bool_values;
  grnxx::Array<grnxx::Int> int_values;
  grnxx::Array<grnxx::Float> float_values;
} test;

void init_test() {
  grnxx::Error error;

  // Create a database with the default options.
  test.db = grnxx::open_db(&error, "");
  assert(test.db);

  // Create a table with the default options.
  test.table = test.db->create_table(&error, "Table");
  assert(test.table);

  // Create columns for Bool, Int, and Float values.
  grnxx::DataType data_type = grnxx::BOOL_DATA;
  auto bool_column = test.table->create_column(&error, "Bool", data_type);
  assert(bool_column);

  data_type = grnxx::INT_DATA;
  auto int_column = test.table->create_column(&error, "Int", data_type);
  assert(int_column);

  data_type = grnxx::FLOAT_DATA;
  auto float_column = test.table->create_column(&error, "Float", data_type);
  assert(float_column);

  // Generate random values.
  // Bool: true or false.
  // Int: [0, 100).
  // Float: [0.0, 1.0].
  std::mt19937_64 mersenne_twister;
  assert(test.bool_values.resize(&error, NUM_ROWS + 1));
  assert(test.int_values.resize(&error, NUM_ROWS + 1));
  assert(test.float_values.resize(&error, NUM_ROWS + 1));
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    test.bool_values.set(i, (mersenne_twister() & 1) != 0);
    test.int_values.set(i, mersenne_twister() % 100);
    constexpr auto MAX_VALUE = mersenne_twister.max();
    test.float_values.set(i, 1.0 * mersenne_twister() / MAX_VALUE);
  }

  // Store generated values into columns.
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    grnxx::Int row_id;
    assert(test.table->insert_row(&error, grnxx::NULL_ROW_ID,
                                  grnxx::Datum(), &row_id));
    assert(bool_column->set(&error, row_id, test.bool_values[i]));
    assert(int_column->set(&error, row_id, test.int_values[i]));
    assert(float_column->set(&error, row_id, test.float_values[i]));
  }
}

void test_adjust() {
  grnxx::Error error;

  // Create an object for building a pipeline.
  auto pipeline_builder = grnxx::PipelineBuilder::create(&error, test.table);
  assert(pipeline_builder);

  // Create a cursor which reads all the records.
  auto cursor = test.table->create_cursor(&error);
  assert(cursor);
  assert(pipeline_builder->push_cursor(&error, std::move(cursor)));

  // Create an object for building expressions.
  auto expression_builder =
      grnxx::ExpressionBuilder::create(&error, test.table);
  assert(expression_builder);

  // Create an adjuster (Float).
  assert(expression_builder->push_column(&error, "Float"));
  auto expression = expression_builder->release(&error);
  assert(expression);
  assert(pipeline_builder->push_adjuster(&error, std::move(expression)));

  // Complete a pipeline.
  auto pipeline = pipeline_builder->release(&error);
  assert(pipeline);

  // Read records through the pipeline.
  grnxx::Array<grnxx::Record> records;
  assert(pipeline->flush(&error, &records));
}

void test_adjust2() {
  grnxx::Error error;

  // Create an object for building a pipeline.
  auto pipeline_builder = grnxx::PipelineBuilder::create(&error, test.table);
  assert(pipeline_builder);

  // Create a cursor which reads all the records.
  auto cursor = test.table->create_cursor(&error);
  assert(cursor);
  assert(pipeline_builder->push_cursor(&error, std::move(cursor)));

  // Create an object for building expressions.
  auto expression_builder =
      grnxx::ExpressionBuilder::create(&error, test.table);
  assert(expression_builder);

  // Create an adjuster (Float).
  assert(expression_builder->push_column(&error, "Int"));
  assert(expression_builder->push_operator(&error, grnxx::TO_FLOAT_OPERATOR));
  assert(expression_builder->push_datum(&error, grnxx::Float(100.0)));
  assert(expression_builder->push_operator(&error, grnxx::DIVISION_OPERATOR));
  assert(expression_builder->push_column(&error, "Float"));
  assert(expression_builder->push_operator(&error, grnxx::PLUS_OPERATOR));
  auto expression = expression_builder->release(&error);
  assert(expression);
  assert(pipeline_builder->push_adjuster(&error, std::move(expression)));

  // Complete a pipeline.
  auto pipeline = pipeline_builder->release(&error);
  assert(pipeline);

  // Read records through the pipeline.
  grnxx::Array<grnxx::Record> records;
  assert(pipeline->flush(&error, &records));
}

void test_filter_and_adjust() {
  grnxx::Error error;

  // Create an object for building a pipeline.
  auto pipeline_builder = grnxx::PipelineBuilder::create(&error, test.table);
  assert(pipeline_builder);

  // Create a cursor which reads all the records.
  auto cursor = test.table->create_cursor(&error);
  assert(cursor);
  assert(pipeline_builder->push_cursor(&error, std::move(cursor)));

  // Create an object for building expressions.
  auto expression_builder =
      grnxx::ExpressionBuilder::create(&error, test.table);
  assert(expression_builder);

  // Create a filter (Bool).
  assert(expression_builder->push_column(&error, "Bool"));
  auto expression = expression_builder->release(&error);
  assert(expression);
  assert(pipeline_builder->push_filter(&error, std::move(expression)));

  // Create an adjuster (Float).
  assert(expression_builder->push_column(&error, "Float"));
  expression = expression_builder->release(&error);
  assert(expression);
  assert(pipeline_builder->push_adjuster(&error, std::move(expression)));

  // Complete a pipeline.
  auto pipeline = pipeline_builder->release(&error);
  assert(pipeline);

  // Read records through the pipeline.
  grnxx::Array<grnxx::Record> records;
  assert(pipeline->flush(&error, &records));
}

void run_loop(void (*test)()) {
  double total_elapsed = 0.0;
  for (int i = 0; i < NUM_LOOPS; ++i) {
    Timer timer;
    test();
    double elapsed = timer.elapsed();
    std::cout << "elapsed [s] = " << elapsed << std::endl;
    total_elapsed += elapsed;
  }
  std::cout << "total elapsed [s] = " << total_elapsed << std::endl;
}

int main(int argc, char *argv[]) {
  if (argc > 1) {
    NUM_ROWS = std::atoi(argv[1]);
  }
  if (argc > 2) {
    NUM_LOOPS = std::atoi(argv[2]);
  }
  init_test();
  run_loop(test_adjust);
  run_loop(test_adjust2);
  run_loop(test_filter_and_adjust);
  return 0;
}
