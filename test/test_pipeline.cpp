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

#include "grnxx/column.hpp"
#include "grnxx/cursor.hpp"
#include "grnxx/db.hpp"
#include "grnxx/expression.hpp"
#include "grnxx/pipeline.hpp"
#include "grnxx/sorter.hpp"
#include "grnxx/table.hpp"

struct {
  std::unique_ptr<grnxx::DB> db;
  grnxx::Table *table;
  grnxx::Array<grnxx::Bool> bool_values;
  grnxx::Array<grnxx::Int> int_values;
  grnxx::Array<grnxx::Float> float_values;
} test;

void init_test() {
  // Create a database with the default options.
  test.db = grnxx::open_db("");

  // Create a table with the default options.
  test.table = test.db->create_table("Table");

  // Create columns for various data types.
  auto bool_column = test.table->create_column("Bool", grnxx::BOOL_DATA);
  auto int_column = test.table->create_column("Int", grnxx::INT_DATA);
  auto float_column = test.table->create_column("Float", grnxx::FLOAT_DATA);

  // Generate random values.
  // Bool: true, false, or N/A.
  // Int: [0, 128) or N/A.
  // Float: [0.0, 1.0) or N/A.
  constexpr size_t NUM_ROWS = 1 << 16;
  std::mt19937_64 mersenne_twister;
  test.bool_values.resize(NUM_ROWS);
  test.int_values.resize(NUM_ROWS);
  test.float_values.resize(NUM_ROWS);
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    uint64_t source = mersenne_twister() % 3;
    switch (source) {
      case 0: {
        test.bool_values[i] = grnxx::Bool(false);
        break;
      }
      case 1: {
        test.bool_values[i] = grnxx::Bool(true);
        break;
      }
      case 2: {
        test.bool_values[i] = grnxx::Bool::na();
        break;
      }
    }

    source = mersenne_twister() % 129;
    if (source == 128) {
      test.int_values[i] = grnxx::Int::na();
    } else {
      test.int_values[i] = grnxx::Int(source);
    }

    source = mersenne_twister() % 129;
    if (source == 128) {
      test.float_values[i] = grnxx::Float::na();
    } else {
      test.float_values[i] = grnxx::Float(source / 128.0);
    }
  }

  // Store generated values into columns.
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = test.table->insert_row();
    bool_column->set(row_id, test.bool_values[i]);
    int_column->set(row_id, test.int_values[i]);
    float_column->set(row_id, test.float_values[i]);
  }
}

void test_cursor() {
  // Create an object for building a pipeline.
  auto pipeline_builder = grnxx::PipelineBuilder::create(test.table);

  // Create a cursor which reads all the records.
  auto cursor = test.table->create_cursor();
  pipeline_builder->push_cursor(std::move(cursor));

  // Complete a pipeline.
  auto pipeline = pipeline_builder->release();

  // Read records through the pipeline.
  grnxx::Array<grnxx::Record> records;
  pipeline->flush(&records);
  assert(records.size() == test.table->num_rows());
}

void test_filter() {
  // Create an object for building a pipeline.
  auto pipeline_builder = grnxx::PipelineBuilder::create(test.table);

  // Create a cursor which reads all the records.
  auto cursor = test.table->create_cursor();
  pipeline_builder->push_cursor(std::move(cursor));

  // Create an object for building expressions.
  auto expression_builder = grnxx::ExpressionBuilder::create(test.table);

  // Create a filter (Bool && (Int < 50) && (Float < 0.5)).
  expression_builder->push_column("Bool");
  expression_builder->push_column("Int");
  expression_builder->push_constant(grnxx::Int(50));
  expression_builder->push_operator(grnxx::LESS_OPERATOR);
  expression_builder->push_column("Float");
  expression_builder->push_constant(grnxx::Float(0.5));
  expression_builder->push_operator(grnxx::LESS_OPERATOR);
  expression_builder->push_operator(grnxx::LOGICAL_AND_OPERATOR);
  expression_builder->push_operator(grnxx::LOGICAL_AND_OPERATOR);
  auto expression = expression_builder->release();
  pipeline_builder->push_filter(std::move(expression));

  // Complete a pipeline.
  auto pipeline = pipeline_builder->release();

  // Read records through the pipeline.
  grnxx::Array<grnxx::Record> records;
  pipeline->flush(&records);

  size_t count = 0;
  for (size_t i = 0; i < test.bool_values.size(); ++i) {
    if (test.bool_values[i].is_true() &&
        (test.int_values[i] < grnxx::Int(50)).is_true() &&
        (test.float_values[i] < grnxx::Float(0.5)).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Create a cursor which reads all the records.
  cursor = test.table->create_cursor();
  pipeline_builder->push_cursor(std::move(cursor));

  // Create a filter (Bool && (Int < 50)).
  constexpr size_t FILTER_OFFSET = 1234;
  constexpr size_t FILTER_LIMIT  = 2345;
  expression_builder->push_column("Bool");
  expression_builder->push_column("Int");
  expression_builder->push_constant(grnxx::Int(50));
  expression_builder->push_operator(grnxx::LESS_OPERATOR);
  expression_builder->push_operator(grnxx::LOGICAL_AND_OPERATOR);
  expression = expression_builder->release();
  pipeline_builder->push_filter(std::move(expression),
                                FILTER_OFFSET, FILTER_LIMIT);

  // Complete a pipeline.
  pipeline = pipeline_builder->release();

  // Read records through the pipeline.
  records.clear();
  pipeline->flush(&records);
  assert(records.size() == FILTER_LIMIT);

  count = 0;
  for (size_t i = 0; i < test.bool_values.size(); ++i) {
    if (test.bool_values[i].is_true() &&
        (test.int_values[i] < grnxx::Int(50)).is_true()) {
      if ((count >= FILTER_OFFSET) &&
          (count < (FILTER_OFFSET + FILTER_LIMIT))) {
        assert(records[count - FILTER_OFFSET].row_id.match(grnxx::Int(i)));
        ++count;
      }
    }
  }
}

void test_adjuster() {
  // Create an object for building a pipeline.
  auto pipeline_builder = grnxx::PipelineBuilder::create(test.table);

  // Create a cursor which reads all the records.
  auto cursor = test.table->create_cursor();
  pipeline_builder->push_cursor(std::move(cursor));

  // Create an object for building expressions.
  auto expression_builder = grnxx::ExpressionBuilder::create(test.table);

  // Create a filter (Bool).
  expression_builder->push_column("Bool");
  auto expression = expression_builder->release();
  pipeline_builder->push_filter(std::move(expression));

  // Create an adjuster (Float * 100.0).
  expression_builder->push_column("Float");
  expression_builder->push_constant(grnxx::Float(100.0));
  expression_builder->push_operator(grnxx::MULTIPLICATION_OPERATOR);
  expression = expression_builder->release();
  pipeline_builder->push_adjuster(std::move(expression));

  // Complete a pipeline.
  auto pipeline = pipeline_builder->release();

  // Read records through the pipeline.
  grnxx::Array<grnxx::Record> records;
  pipeline->flush(&records);

  size_t count = 0;
  for (size_t i = 0; i < test.bool_values.size(); ++i) {
    if (test.bool_values[i].is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      assert(records[count].score.match(
          test.float_values[i] * grnxx::Float(100.0)));
      ++count;
    }
  }
  assert(records.size() == count);
}

void test_sorter() {
  // Create an object for building a pipeline.
  auto pipeline_builder = grnxx::PipelineBuilder::create(test.table);

  // Create a cursor which reads all the records.
  auto cursor = test.table->create_cursor();
  pipeline_builder->push_cursor(std::move(cursor));

  // Create an object for building expressions.
  auto expression_builder = grnxx::ExpressionBuilder::create(test.table);

  // Create a filter (Bool).
  expression_builder->push_column("Bool");
  auto expression = expression_builder->release();
  pipeline_builder->push_filter(std::move(expression));

  // Create an adjuster (Float).
  expression_builder->push_column("Float");
  expression = expression_builder->release();
  pipeline_builder->push_adjuster(std::move(expression));

  // Create a sorter (Int, _id).
  grnxx::Array<grnxx::SorterOrder> orders;
  orders.resize(2);
  expression_builder->push_column("Int");
  expression = expression_builder->release();
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  expression_builder->push_row_id();
  expression = expression_builder->release();
  orders[1].expression = std::move(expression);
  orders[1].type = grnxx::SORTER_REGULAR_ORDER;
  auto sorter = grnxx::Sorter::create(std::move(orders));
  pipeline_builder->push_sorter(std::move(sorter));

  // Complete a pipeline.
  auto pipeline = pipeline_builder->release();

  // Read records through the pipeline.
  grnxx::Array<grnxx::Record> records;
  pipeline->flush(&records);

  size_t count = 0;
  for (size_t i = 0; i < test.bool_values.size(); ++i) {
    if (test.bool_values[i].is_true()) {
      ++count;
    }
  }
  assert(records.size() == count);

  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.value();
    assert(test.bool_values[row_id].is_true());
    assert(records[i].score.match(test.float_values[row_id]));
  }

  for (size_t i = 1; i < records.size(); ++i) {
    size_t prev_row_id = records[i - 1].row_id.value();
    size_t this_row_id = records[i].row_id.value();
    grnxx::Int prev_value = test.int_values[prev_row_id];
    grnxx::Int this_value = test.int_values[this_row_id];
    if (prev_value.is_na()) {
      assert(this_value.is_na());
    } else {
      assert(this_value.is_na() || (prev_value <= this_value).is_true());
    }
    if (prev_value.match(this_value)) {
      assert(prev_row_id < this_row_id);
    }
  }
}

void test_merger() {
  // Create an object for building a pipeline.
  auto pipeline_builder = grnxx::PipelineBuilder::create(test.table);

  // Create a cursor which reads all the records.
  auto cursor = test.table->create_cursor();
  pipeline_builder->push_cursor(std::move(cursor));

  // Create an object for building expressions.
  auto expression_builder = grnxx::ExpressionBuilder::create(test.table);

  // Create a filter (Bool).
  expression_builder->push_column("Bool");
  auto expression = expression_builder->release();
  pipeline_builder->push_filter(std::move(expression));

  // Create an adjuster (Float).
  expression_builder->push_column("Float");
  expression = expression_builder->release();
  pipeline_builder->push_adjuster(std::move(expression));

  // Create a cursor which reads all the records.
  cursor = test.table->create_cursor();
  pipeline_builder->push_cursor(std::move(cursor));

  // Create a filter (Int < 50).
  expression_builder->push_column("Int");
  expression_builder->push_constant(grnxx::Int(50));
  expression_builder->push_operator(grnxx::LESS_OPERATOR);
  expression = expression_builder->release();
  pipeline_builder->push_filter(std::move(expression));

  // Create an adjuster (Float * 2.0).
  expression_builder->push_column("Float");
  expression_builder->push_constant(grnxx::Float(2.0));
  expression_builder->push_operator(grnxx::MULTIPLICATION_OPERATOR);
  expression = expression_builder->release();
  pipeline_builder->push_adjuster(std::move(expression));

  // Create a merger.
  grnxx::MergerOptions options;
  options.logical_operator_type = grnxx::MERGER_LOGICAL_AND;
  options.score_operator_type = grnxx::MERGER_SCORE_PLUS;
  pipeline_builder->push_merger(options);

  // Complete a pipeline.
  auto pipeline = pipeline_builder->release();

  // Read records through the pipeline.
  grnxx::Array<grnxx::Record> records;
  pipeline->flush(&records);

  size_t count = 0;
  for (size_t i = 0; i < test.bool_values.size(); ++i) {
    if (test.bool_values[i].is_true() &&
        (test.int_values[i] < grnxx::Int(50)).is_true()) {
      ++count;
    }
  }
  assert(records.size() == count);

  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.value();
    assert(test.bool_values[row_id].is_true() &&
           (test.int_values[row_id] < grnxx::Int(50)).is_true());
    assert(records[i].score.match(
        test.float_values[row_id] * grnxx::Float(3.0)));
  }
}

int main() {
  init_test();
  test_cursor();
  test_filter();
  test_adjuster();
  test_sorter();
  test_merger();
  return 0;
}
