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
#include "grnxx/table.hpp"
#include "grnxx/merger.hpp"

struct {
  grnxx::unique_ptr<grnxx::DB> db;
  grnxx::Table *table;
  grnxx::Array<grnxx::Bool> bool_values;
  grnxx::Array<grnxx::Bool> bool2_values;
  grnxx::Array<grnxx::Float> float_values;
  grnxx::Array<grnxx::Float> float2_values;
} test;

void init_test() {
  grnxx::Error error;

  // Create a database with the default options.
  test.db = grnxx::open_db(&error, "");
  assert(test.db);

  // Create a table with the default options.
  test.table = test.db->create_table(&error, "Table");
  assert(test.table);

  // Create columns for Bool and Float values.
  grnxx::DataType data_type = grnxx::BOOL_DATA;
  auto bool_column = test.table->create_column(&error, "Bool", data_type);
  assert(bool_column);
  auto bool2_column = test.table->create_column(&error, "Bool2", data_type);
  assert(bool2_column);

  data_type = grnxx::FLOAT_DATA;
  auto float_column = test.table->create_column(&error, "Float", data_type);
  assert(float_column);
  auto float2_column = test.table->create_column(&error, "Float2", data_type);
  assert(float2_column);

  // Generate random values.
  // Bool: true or false.
  constexpr grnxx::Int NUM_ROWS = 1 << 16;
  std::mt19937_64 mersenne_twister;
  assert(test.bool_values.resize(&error, NUM_ROWS + 1));
  assert(test.bool2_values.resize(&error, NUM_ROWS + 1));
  assert(test.float_values.resize(&error, NUM_ROWS + 1));
  assert(test.float2_values.resize(&error, NUM_ROWS + 1));
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    test.bool_values.set(i, (mersenne_twister() & 1) != 0);
    test.bool2_values.set(i, (mersenne_twister() & 1) != 0);
    grnxx::Float float_value =
        1.0 * mersenne_twister() / mersenne_twister.max();
    test.float_values.set(i, float_value);
    float_value = 1.0 * mersenne_twister() / mersenne_twister.max();
    test.float2_values.set(i, float_value);
  }

  // Store generated values into columns.
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    grnxx::Int row_id;
    assert(test.table->insert_row(&error, grnxx::NULL_ROW_ID,
                                  grnxx::Datum(), &row_id));
    assert(bool_column->set(&error, row_id, test.bool_values[i]));
    assert(bool2_column->set(&error, row_id, test.bool2_values[i]));
    assert(float_column->set(&error, row_id, test.float_values[i]));
    assert(float2_column->set(&error, row_id, test.float2_values[i]));
  }
}

void test_and() {
  grnxx::Error error;

  // Create cursors to read all the records.
  auto cursor = test.table->create_cursor(&error);
  assert(cursor);
  auto cursor2 = test.table->create_cursor(&error);
  assert(cursor2);

  // Create expressions to read values.
  auto expression_builder =
      grnxx::ExpressionBuilder::create(&error, test.table);
  assert(expression_builder);
  assert(expression_builder->push_column(&error, "Bool"));
  auto expression = expression_builder->release(&error);
  assert(expression);
  assert(expression_builder->push_column(&error, "Bool2"));
  auto expression2 = expression_builder->release(&error);
  assert(expression2);

  // Create input records.
  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) == test.table->num_rows());
  assert(expression->filter(&error, &records));
  grnxx::Array<grnxx::Record> records2;
  assert(cursor2->read_all(&error, &records2) == test.table->num_rows());
  assert(expression2->filter(&error, &records2));

  // Merge records.
  auto merger = grnxx::Merger::create(&error);
  assert(merger);
  grnxx::Array<grnxx::Record> result_records;
  assert(merger->merge(&error, &records, &records2, &result_records));

  for (grnxx::Int i = 0; i < result_records.size(); ++i) {
    grnxx::Int row_id = result_records.get_row_id(i);
    assert(test.bool_values[row_id] && test.bool2_values[row_id]);
  }

  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i <= test.table->max_row_id(); ++i) {
    if (test.bool_values[i] && test.bool2_values[i]) {
      assert(result_records.get_row_id(count) == i);
      ++count;
    }
  }
  assert(count == result_records.size());
}

void test_or() {
  grnxx::Error error;

  // Create cursors to read all the records.
  auto cursor = test.table->create_cursor(&error);
  assert(cursor);
  auto cursor2 = test.table->create_cursor(&error);
  assert(cursor2);

  // Create expressions to read values.
  auto expression_builder =
      grnxx::ExpressionBuilder::create(&error, test.table);
  assert(expression_builder);
  assert(expression_builder->push_column(&error, "Bool"));
  auto expression = expression_builder->release(&error);
  assert(expression);
  assert(expression_builder->push_column(&error, "Bool2"));
  auto expression2 = expression_builder->release(&error);
  assert(expression2);

  // Create input records.
  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) == test.table->num_rows());
  assert(expression->filter(&error, &records));
  grnxx::Array<grnxx::Record> records2;
  assert(cursor2->read_all(&error, &records2) == test.table->num_rows());
  assert(expression2->filter(&error, &records2));

  // Merge records.
  grnxx::MergerOptions options;
  options.type = grnxx::OR_MERGER;
  options.operator_type = grnxx::PLUS_MERGER_OPERATOR;
  auto merger = grnxx::Merger::create(&error, options);
  assert(merger);
  grnxx::Array<grnxx::Record> result_records;
  assert(merger->merge(&error, &records, &records2, &result_records));

  for (grnxx::Int i = 0; i < result_records.size(); ++i) {
    grnxx::Int row_id = result_records.get_row_id(i);
    assert(test.bool_values[row_id] || test.bool2_values[row_id]);
  }

  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i <= test.table->max_row_id(); ++i) {
    if (test.bool_values[i] || test.bool2_values[i]) {
      ++count;
    }
  }
  assert(count == result_records.size());
}

void test_minus() {
  grnxx::Error error;

  // Create cursors to read all the records.
  auto cursor = test.table->create_cursor(&error);
  assert(cursor);
  auto cursor2 = test.table->create_cursor(&error);
  assert(cursor2);

  // Create expressions to read values.
  auto expression_builder =
      grnxx::ExpressionBuilder::create(&error, test.table);
  assert(expression_builder);
  assert(expression_builder->push_column(&error, "Bool"));
  auto expression = expression_builder->release(&error);
  assert(expression);
  assert(expression_builder->push_column(&error, "Bool2"));
  auto expression2 = expression_builder->release(&error);
  assert(expression2);

  // Create input records.
  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) == test.table->num_rows());
  assert(expression->filter(&error, &records));
  grnxx::Array<grnxx::Record> records2;
  assert(cursor2->read_all(&error, &records2) == test.table->num_rows());
  assert(expression2->filter(&error, &records2));

  // Merge records.
  grnxx::MergerOptions options;
  options.type = grnxx::MINUS_MERGER;
  options.operator_type = grnxx::PLUS_MERGER_OPERATOR;
  auto merger = grnxx::Merger::create(&error, options);
  assert(merger);
  grnxx::Array<grnxx::Record> result_records;
  assert(merger->merge(&error, &records, &records2, &result_records));

  for (grnxx::Int i = 0; i < result_records.size(); ++i) {
    grnxx::Int row_id = result_records.get_row_id(i);
    assert(test.bool_values[row_id] && !test.bool2_values[row_id]);
  }

  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i <= test.table->max_row_id(); ++i) {
    if (test.bool_values[i] && !test.bool2_values[i]) {
      ++count;
    }
  }
  assert(count == result_records.size());
}

void test_plus() {
  grnxx::Error error;

  // Create the first input records.
  auto cursor = test.table->create_cursor(&error);
  assert(cursor);

  auto expression_builder =
      grnxx::ExpressionBuilder::create(&error, test.table);
  assert(expression_builder);
  assert(expression_builder->push_column(&error, "Bool"));
  auto expression = expression_builder->release(&error);
  assert(expression);

  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) == test.table->num_rows());
  assert(expression->filter(&error, &records));

  assert(expression_builder->push_column(&error, "Float"));
  expression = expression_builder->release(&error);
  assert(expression);

  assert(expression->adjust(&error, &records));

  // Create the second input records.
  cursor = test.table->create_cursor(&error);
  assert(cursor);

  assert(expression_builder->push_column(&error, "Bool2"));
  expression = expression_builder->release(&error);
  assert(expression);

  grnxx::Array<grnxx::Record> records2;
  assert(cursor->read_all(&error, &records2) == test.table->num_rows());
  assert(expression->filter(&error, &records2));

  assert(expression_builder->push_column(&error, "Float2"));
  expression = expression_builder->release(&error);
  assert(expression);

  assert(expression->adjust(&error, &records2));

  // Merge records.
  grnxx::MergerOptions options;
  options.type = grnxx::AND_MERGER;
  options.operator_type = grnxx::PLUS_MERGER_OPERATOR;
  auto merger = grnxx::Merger::create(&error, options);
  assert(merger);
  grnxx::Array<grnxx::Record> result_records;
  assert(merger->merge(&error, &records, &records2, &result_records));

  for (grnxx::Int i = 0; i < result_records.size(); ++i) {
    grnxx::Int row_id = result_records.get_row_id(i);
    grnxx::Float score = result_records.get_score(i);
    assert(test.bool_values[row_id] && test.bool2_values[row_id]);
    assert(score == (test.float_values[row_id] + test.float2_values[row_id]));
  }

  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i <= test.table->max_row_id(); ++i) {
    if (test.bool_values[i] && test.bool2_values[i]) {
      assert(result_records.get_row_id(count) == i);
      ++count;
    }
  }
  assert(count == result_records.size());
}

int main() {
  init_test();

  test_and();
  test_or();
//  test_xor();
  test_minus();
//  test_lhs();
//  test_rhs();

  test_plus();

  return 0;
}
