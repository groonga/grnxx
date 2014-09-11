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
#include "grnxx/error.hpp"
#include "grnxx/expression.hpp"
#include "grnxx/pipeline.hpp"
#include "grnxx/sorter.hpp"
#include "grnxx/table.hpp"

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
  constexpr grnxx::Int NUM_ROWS = 1 << 16;
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

void test_cursor() {
  grnxx::Error error;

  // Create an object for building a pipeline.
  auto pipeline_builder = grnxx::PipelineBuilder::create(&error, test.table);
  assert(pipeline_builder);

  // Create a cursor which reads all the records.
  auto cursor = test.table->create_cursor(&error);
  assert(cursor);
  assert(pipeline_builder->push_cursor(&error, std::move(cursor)));

  // Complete a pipeline.
  auto pipeline = pipeline_builder->release(&error);
  assert(pipeline);

  // Read records through the pipeline.
  grnxx::Array<grnxx::Record> records;
  assert(pipeline->flush(&error, &records));
  assert(records.size() == test.table->num_rows());
}

void test_filter() {
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

  // Create a filter (Bool && (Int < 50) && (Float < 0.5)).
  assert(expression_builder->push_column(&error, "Bool"));
  assert(expression_builder->push_column(&error, "Int"));
  assert(expression_builder->push_datum(&error, grnxx::Int(50)));
  assert(expression_builder->push_operator(&error, grnxx::LESS_OPERATOR));
  assert(expression_builder->push_column(&error, "Float"));
  assert(expression_builder->push_datum(&error, grnxx::Float(0.5)));
  assert(expression_builder->push_operator(&error, grnxx::LESS_OPERATOR));
  assert(expression_builder->push_operator(&error,
                                           grnxx::LOGICAL_AND_OPERATOR));
  assert(expression_builder->push_operator(&error,
                                           grnxx::LOGICAL_AND_OPERATOR));
  auto expression = expression_builder->release(&error);
  assert(expression);
  assert(pipeline_builder->push_filter(&error, std::move(expression)));

  // Complete a pipeline.
  auto pipeline = pipeline_builder->release(&error);
  assert(pipeline);

  // Read records through the pipeline.
  grnxx::Array<grnxx::Record> records;
  assert(pipeline->flush(&error, &records));

  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i < test.bool_values.size(); ++i) {
    if (test.bool_values[i] &&
        (test.int_values[i] < 50) &&
        (test.float_values[i] < 0.5)) {
      assert(records.get_row_id(count) == i);
      ++count;
    }
  }
  assert(records.size() == count);

  // Create a cursor which reads all the records.
  cursor = test.table->create_cursor(&error);
  assert(cursor);
  assert(pipeline_builder->push_cursor(&error, std::move(cursor)));

  // Create a filter (Bool && (Int < 50)).
  constexpr grnxx::Int FILTER_OFFSET = 1234;
  constexpr grnxx::Int FILTER_LIMIT  = 2345;
  assert(expression_builder->push_column(&error, "Bool"));
  assert(expression_builder->push_column(&error, "Int"));
  assert(expression_builder->push_datum(&error, grnxx::Int(50)));
  assert(expression_builder->push_operator(&error, grnxx::LESS_OPERATOR));
  assert(expression_builder->push_operator(&error,
                                           grnxx::LOGICAL_AND_OPERATOR));
  expression = expression_builder->release(&error);
  assert(expression);
  assert(pipeline_builder->push_filter(&error, std::move(expression),
                                       FILTER_OFFSET, FILTER_LIMIT));

  // Complete a pipeline.
  pipeline = pipeline_builder->release(&error);
  assert(pipeline);

  // Read records through the pipeline.
  records.clear();
  assert(pipeline->flush(&error, &records));
  assert(records.size() == FILTER_LIMIT);

  count = 0;
  for (grnxx::Int i = 1; i < test.bool_values.size(); ++i) {
    if (test.bool_values[i] && (test.int_values[i] < 50)) {
      if ((count >= FILTER_OFFSET) &&
          (count < (FILTER_OFFSET + FILTER_LIMIT))) {
        assert(records.get_row_id(count - FILTER_OFFSET) == i);
        ++count;
      }
    }
  }
}

void test_adjuster() {
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

  // Create an adjuster (Float * 100.0).
  assert(expression_builder->push_column(&error, "Float"));
  assert(expression_builder->push_datum(&error, grnxx::Float(100.0)));
  assert(expression_builder->push_operator(&error,
                                           grnxx::MULTIPLICATION_OPERATOR));
  expression = expression_builder->release(&error);
  assert(expression);
  assert(pipeline_builder->push_adjuster(&error, std::move(expression)));

  // Complete a pipeline.
  auto pipeline = pipeline_builder->release(&error);
  assert(pipeline);

  // Read records through the pipeline.
  grnxx::Array<grnxx::Record> records;
  assert(pipeline->flush(&error, &records));

  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i < test.bool_values.size(); ++i) {
    if (test.bool_values[i]) {
      assert(records.get_row_id(count) == i);
      assert(records.get_score(count) == (test.float_values[i] * 100.0));
      ++count;
    }
  }
  assert(records.size() == count);
}

void test_sorter() {
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

  // Create a sorter (Int, _id).
  grnxx::Array<grnxx::SortOrder> orders;
  assert(orders.resize(&error, 2));
  assert(expression_builder->push_column(&error, "Int"));
  expression = expression_builder->release(&error);
  assert(expression);
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::REGULAR_ORDER;
  assert(expression_builder->push_column(&error, "_id"));
  expression = expression_builder->release(&error);
  assert(expression);
  orders[1].expression = std::move(expression);
  orders[1].type = grnxx::REGULAR_ORDER;
  auto sorter = grnxx::Sorter::create(&error, std::move(orders));
  assert(pipeline_builder->push_sorter(&error, std::move(sorter)));

  // Complete a pipeline.
  auto pipeline = pipeline_builder->release(&error);
  assert(pipeline);

  // Read records through the pipeline.
  grnxx::Array<grnxx::Record> records;
  assert(pipeline->flush(&error, &records));

  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i < test.bool_values.size(); ++i) {
    if (test.bool_values[i]) {
      ++count;
    }
  }
  assert(records.size() == count);

  for (grnxx::Int i = 0; i < records.size(); ++i) {
    grnxx::Int row_id = records.get_row_id(i);
    assert(test.bool_values[row_id]);
    assert(records.get_score(i) == test.float_values[row_id]);
  }

  for (grnxx::Int i = 1; i < records.size(); ++i) {
    grnxx::Int prev_row_id = records.get_row_id(i - 1);
    grnxx::Int this_row_id = records.get_row_id(i);
    grnxx::Int prev_value = test.int_values[prev_row_id];
    grnxx::Int this_value = test.int_values[this_row_id];
    assert(prev_value <= this_value);
    if (prev_value == this_value) {
      assert(prev_row_id < this_row_id);
    }
  }
}

int main() {
  init_test();
  test_cursor();
  test_filter();
  test_adjuster();
  test_sorter();
  return 0;
}
