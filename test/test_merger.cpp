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

constexpr grnxx::Float NULL_SCORE = 0.125;

struct {
  grnxx::unique_ptr<grnxx::DB> db;
  grnxx::Table *table;
  grnxx::Array<grnxx::Bool> bool_values;
  grnxx::Array<grnxx::Bool> bool2_values;
  grnxx::Array<grnxx::Float> float_values;
  grnxx::Array<grnxx::Float> float2_values;
  grnxx::Array<grnxx::Float> scores;
  grnxx::Array<grnxx::Float> scores2;
} test;

std::mt19937_64 mersenne_twister;

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
  constexpr grnxx::Int NUM_ROWS = 1 << 12;
  assert(test.bool_values.resize(&error, NUM_ROWS + 1));
  assert(test.bool2_values.resize(&error, NUM_ROWS + 1));
  assert(test.float_values.resize(&error, NUM_ROWS + 1));
  assert(test.float2_values.resize(&error, NUM_ROWS + 1));
  assert(test.scores.resize(&error, NUM_ROWS + 1));
  assert(test.scores2.resize(&error, NUM_ROWS + 1));
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    test.bool_values.set(i, (mersenne_twister() & 1) != 0);
    test.bool2_values.set(i, (mersenne_twister() & 1) != 0);
    grnxx::Float float_value =
        1.0 * mersenne_twister() / mersenne_twister.max();
    test.float_values.set(i, float_value);
    float_value =
        1.0 * mersenne_twister() / mersenne_twister.max();
    test.float2_values.set(i, float_value);
    test.scores.set(i, test.bool_values[i] ?
                       test.float_values[i] : NULL_SCORE);
    test.scores2.set(i, test.bool2_values[i] ?
                        test.float2_values[i] : NULL_SCORE);
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

grnxx::Array<grnxx::Record> create_input(const char *bool_name,
                                         const char *float_name) {
  grnxx::Error error;

  // Create a cursor to read records.
  auto cursor = test.table->create_cursor(&error);
  assert(cursor);

  // Read all the records.
  grnxx::Array<grnxx::Record> records;
  auto result = cursor->read_all(&error, &records);
  assert(result.is_ok);
  assert(result.count == test.table->num_rows());

  // Create an object to create expressions.
  auto expression_builder =
      grnxx::ExpressionBuilder::create(&error, test.table);
  assert(expression_builder);

  // Apply a filter to the records.
  assert(expression_builder->push_column(&error, bool_name));
  auto expression = expression_builder->release(&error);
  assert(expression);
  assert(expression->filter(&error, &records));

  // Apply an adjuster to the records.
  assert(expression_builder->push_column(&error, float_name));
  expression = expression_builder->release(&error);
  assert(expression);
  assert(expression->adjust(&error, &records));

  return records;
}

grnxx::Array<grnxx::Record> create_input_1() {
  return create_input("Bool", "Float");
}

grnxx::Array<grnxx::Record> create_input_2() {
  return create_input("Bool2", "Float2");
}

grnxx::Array<grnxx::Record> merge_records(
    const grnxx::Array<grnxx::Record> &input_1,
    const grnxx::Array<grnxx::Record> &input_2,
    const grnxx::MergerOptions &options) {
  grnxx::Error error;

  // Create a merger.
  auto merger = grnxx::Merger::create(&error, options);
  assert(merger);

  // Create copies of input records.
  grnxx::Array<grnxx::Record> copy_1;
  assert(copy_1.resize(&error, input_1.size()));
  for (grnxx::Int i = 0; i < copy_1.size(); ++i) {
    copy_1.set(i, input_1.get(i));
  }
  grnxx::Array<grnxx::Record> copy_2;
  assert(copy_2.resize(&error, input_2.size()));
  for (grnxx::Int i = 0; i < copy_2.size(); ++i) {
    copy_2.set(i, input_2.get(i));
  }

  // Merge records.
  grnxx::Array<grnxx::Record> output;
  assert(merger->merge(&error, &copy_1, &copy_2, &output));
  return output;
}

void test_and() {
  grnxx::Error error;

  // Create input records.
  auto input_1 = create_input_1();
  auto input_2 = create_input_2();

  // Set options.
  grnxx::MergerOptions options;
  options.type = grnxx::AND_MERGER;
  options.null_score = NULL_SCORE;

  // Merge records (PLUS).
  options.operator_type = grnxx::PLUS_MERGER_OPERATOR;
  auto output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] && test.bool2_values[row_id]);
    assert(output.get_score(i) ==
           (test.scores[row_id] + test.scores2[row_id]));
  }
  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i <= test.table->max_row_id(); ++i) {
    if (test.bool_values[i] && test.bool2_values[i]) {
      assert(output.get_row_id(count) == i);
      ++count;
    }
  }
  assert(count == output.size());

  // Merge records (MINUS).
  options.operator_type = grnxx::MINUS_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] && test.bool2_values[row_id]);
    assert(output.get_score(i) ==
           (test.scores[row_id] - test.scores2[row_id]));
  }

  // Merge records (MULTIPLICATION).
  options.operator_type = grnxx::MULTIPLICATION_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] && test.bool2_values[row_id]);
    assert(output.get_score(i) ==
           (test.scores[row_id] * test.scores2[row_id]));
  }

  // Merge records (LHS).
  options.operator_type = grnxx::LHS_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] && test.bool2_values[row_id]);
    assert(output.get_score(i) == test.scores[row_id]);
  }

  // Merge records (RHS).
  options.operator_type = grnxx::RHS_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] && test.bool2_values[row_id]);
    assert(output.get_score(i) == test.scores2[row_id]);
  }

  // Merge records (ZERO).
  options.operator_type = grnxx::ZERO_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] && test.bool2_values[row_id]);
    assert(output.get_score(i) == 0.0);
  }
}

void test_or() {
  grnxx::Error error;

  // Create input records.
  auto input_1 = create_input_1();
  auto input_2 = create_input_2();

  // Set options.
  grnxx::MergerOptions options;
  options.type = grnxx::OR_MERGER;
  options.null_score = NULL_SCORE;

  // Merge records (PLUS).
  options.operator_type = grnxx::PLUS_MERGER_OPERATOR;
  auto output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] || test.bool2_values[row_id]);
    assert(output.get_score(i) ==
           (test.scores[row_id] + test.scores2[row_id]));
  }
  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i <= test.table->max_row_id(); ++i) {
    if (test.bool_values[i] || test.bool2_values[i]) {
      ++count;
    }
  }
  assert(count == output.size());

  // Merge records (MINUS).
  options.operator_type = grnxx::MINUS_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] || test.bool2_values[row_id]);
    assert(output.get_score(i) ==
           (test.scores[row_id] - test.scores2[row_id]));
  }

  // Merge records (MULTIPLICATION).
  options.operator_type = grnxx::MULTIPLICATION_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] || test.bool2_values[row_id]);
    assert(output.get_score(i) ==
           (test.scores[row_id] * test.scores2[row_id]));
  }

  // Merge records (LHS).
  options.operator_type = grnxx::LHS_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] || test.bool2_values[row_id]);
    assert(output.get_score(i) == test.scores[row_id]);
  }

  // Merge records (RHS).
  options.operator_type = grnxx::RHS_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] || test.bool2_values[row_id]);
    assert(output.get_score(i) == test.scores2[row_id]);
  }

  // Merge records (ZERO).
  options.operator_type = grnxx::ZERO_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] || test.bool2_values[row_id]);
    assert(output.get_score(i) == 0.0);
  }
}

void test_xor() {
  grnxx::Error error;

  // Create input records.
  auto input_1 = create_input_1();
  auto input_2 = create_input_2();

  // Set options.
  grnxx::MergerOptions options;
  options.type = grnxx::XOR_MERGER;
  options.null_score = NULL_SCORE;

  // Merge records (PLUS).
  options.operator_type = grnxx::PLUS_MERGER_OPERATOR;
  auto output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] ^ test.bool2_values[row_id]);
    assert(output.get_score(i) ==
           (test.scores[row_id] + test.scores2[row_id]));
  }
  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i <= test.table->max_row_id(); ++i) {
    if (test.bool_values[i] ^ test.bool2_values[i]) {
      ++count;
    }
  }
  assert(count == output.size());

  // Merge records (MINUS).
  options.operator_type = grnxx::MINUS_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] ^ test.bool2_values[row_id]);
    assert(output.get_score(i) ==
           (test.scores[row_id] - test.scores2[row_id]));
  }

  // Merge records (MULTIPLICATION).
  options.operator_type = grnxx::MULTIPLICATION_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] ^ test.bool2_values[row_id]);
    assert(output.get_score(i) ==
           (test.scores[row_id] * test.scores2[row_id]));
  }

  // Merge records (LHS).
  options.operator_type = grnxx::LHS_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] ^ test.bool2_values[row_id]);
    assert(output.get_score(i) == test.scores[row_id]);
  }

  // Merge records (RHS).
  options.operator_type = grnxx::RHS_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] ^ test.bool2_values[row_id]);
    assert(output.get_score(i) == test.scores2[row_id]);
  }

  // Merge records (ZERO).
  options.operator_type = grnxx::ZERO_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] ^ test.bool2_values[row_id]);
    assert(output.get_score(i) == 0.0);
  }
}

void test_minus() {
  grnxx::Error error;

  // Create input records.
  auto input_1 = create_input_1();
  auto input_2 = create_input_2();

  // Set options.
  grnxx::MergerOptions options;
  options.type = grnxx::MINUS_MERGER;
  options.null_score = NULL_SCORE;

  // Merge records (PLUS).
  options.operator_type = grnxx::PLUS_MERGER_OPERATOR;
  auto output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] && !test.bool2_values[row_id]);
    assert(output.get_score(i) ==
           (test.scores[row_id] + test.scores2[row_id]));
  }
  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i <= test.table->max_row_id(); ++i) {
    if (test.bool_values[i] && !test.bool2_values[i]) {
      ++count;
    }
  }
  assert(count == output.size());

  // Merge records (MINUS).
  options.operator_type = grnxx::MINUS_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] && !test.bool2_values[row_id]);
    assert(output.get_score(i) ==
           (test.scores[row_id] - test.scores2[row_id]));
  }

  // Merge records (MULTIPLICATION).
  options.operator_type = grnxx::MULTIPLICATION_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] && !test.bool2_values[row_id]);
    assert(output.get_score(i) ==
           (test.scores[row_id] * test.scores2[row_id]));
  }

  // Merge records (LHS).
  options.operator_type = grnxx::LHS_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] && !test.bool2_values[row_id]);
    assert(output.get_score(i) == test.scores[row_id]);
  }

  // Merge records (RHS).
  options.operator_type = grnxx::RHS_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] && !test.bool2_values[row_id]);
    assert(output.get_score(i) == test.scores2[row_id]);
  }

  // Merge records (ZERO).
  options.operator_type = grnxx::ZERO_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id] && !test.bool2_values[row_id]);
    assert(output.get_score(i) == 0.0);
  }
}

void test_lhs() {
  grnxx::Error error;

  // Create input records.
  auto input_1 = create_input_1();
  auto input_2 = create_input_2();

  // Set options.
  grnxx::MergerOptions options;
  options.type = grnxx::LHS_MERGER;
  options.null_score = NULL_SCORE;

  // Merge records (PLUS).
  options.operator_type = grnxx::PLUS_MERGER_OPERATOR;
  auto output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id]);
    assert(output.get_score(i) ==
           (test.scores[row_id] + test.scores2[row_id]));
  }
  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i <= test.table->max_row_id(); ++i) {
    if (test.bool_values[i]) {
      ++count;
    }
  }
  assert(count == output.size());

  // Merge records (MINUS).
  options.operator_type = grnxx::MINUS_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id]);
    assert(output.get_score(i) ==
           (test.scores[row_id] - test.scores2[row_id]));
  }

  // Merge records (MULTIPLICATION).
  options.operator_type = grnxx::MULTIPLICATION_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id]);
    assert(output.get_score(i) ==
           (test.scores[row_id] * test.scores2[row_id]));
  }

  // Merge records (LHS).
  options.operator_type = grnxx::LHS_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id]);
    assert(output.get_score(i) == test.scores[row_id]);
  }

  // Merge records (RHS).
  options.operator_type = grnxx::RHS_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id]);
    assert(output.get_score(i) == test.scores2[row_id]);
  }

  // Merge records (ZERO).
  options.operator_type = grnxx::ZERO_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool_values[row_id]);
    assert(output.get_score(i) == 0.0);
  }
}

void test_rhs() {
  grnxx::Error error;

  // Create input records.
  auto input_1 = create_input_1();
  auto input_2 = create_input_2();

  // Set options.
  grnxx::MergerOptions options;
  options.type = grnxx::RHS_MERGER;
  options.null_score = NULL_SCORE;

  // Merge records (PLUS).
  options.operator_type = grnxx::PLUS_MERGER_OPERATOR;
  auto output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool2_values[row_id]);
    assert(output.get_score(i) ==
           (test.scores[row_id] + test.scores2[row_id]));
  }
  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i <= test.table->max_row_id(); ++i) {
    if (test.bool2_values[i]) {
      ++count;
    }
  }
  assert(count == output.size());

  // Merge records (MINUS).
  options.operator_type = grnxx::MINUS_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool2_values[row_id]);
    assert(output.get_score(i) ==
           (test.scores[row_id] - test.scores2[row_id]));
  }

  // Merge records (MULTIPLICATION).
  options.operator_type = grnxx::MULTIPLICATION_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool2_values[row_id]);
    assert(output.get_score(i) ==
           (test.scores[row_id] * test.scores2[row_id]));
  }

  // Merge records (LHS).
  options.operator_type = grnxx::LHS_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool2_values[row_id]);
    assert(output.get_score(i) == test.scores[row_id]);
  }

  // Merge records (RHS).
  options.operator_type = grnxx::RHS_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool2_values[row_id]);
    assert(output.get_score(i) == test.scores2[row_id]);
  }

  // Merge records (ZERO).
  options.operator_type = grnxx::ZERO_MERGER_OPERATOR;
  output = merge_records(input_1, input_2, options);
  for (grnxx::Int i = 0; i < output.size(); ++i) {
    grnxx::Int row_id = output.get_row_id(i);
    assert(test.bool2_values[row_id]);
    assert(output.get_score(i) == 0.0);
  }
}

int main() {
  for (int i = 0; i < 5; ++i) {
    init_test();
    test_and();
    test_or();
    test_xor();
    test_minus();
    test_lhs();
    test_rhs();
  }
  return 0;
}
