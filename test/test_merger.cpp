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

constexpr grnxx::Float MISSING_SCORE(0.125);

struct {
  std::unique_ptr<grnxx::DB> db;
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
  // Create a database with the default options.
  test.db = grnxx::open_db("");

  // Create a table with the default options.
  test.table = test.db->create_table("Table");

  // Create columns for Bool and Float values.
  grnxx::DataType data_type = GRNXX_BOOL;
  auto bool_column = test.table->create_column("Bool", data_type);
  auto bool2_column = test.table->create_column("Bool2", data_type);

  data_type = GRNXX_FLOAT;
  auto float_column = test.table->create_column("Float", data_type);
  auto float2_column = test.table->create_column("Float2", data_type);

  // Generate random values.
  // Bool: true, false, and N/A.
  // Float: [0.0, 1.0) and N/A.
  constexpr size_t NUM_ROWS = 1 << 12;
  test.bool_values.resize(NUM_ROWS);
  test.bool2_values.resize(NUM_ROWS);
  test.float_values.resize(NUM_ROWS);
  test.float2_values.resize(NUM_ROWS);
  test.scores.resize(NUM_ROWS);
  test.scores2.resize(NUM_ROWS);
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    uint64_t source = mersenne_twister() % 3;
    test.bool_values[i] = (source == 0) ? grnxx::Bool::na() :
        ((source == 1) ? grnxx::Bool(false) : grnxx::Bool(true));
    source = mersenne_twister() % 3;
    test.bool2_values[i] = (source == 0) ? grnxx::Bool::na() :
        ((source == 1) ? grnxx::Bool(false) : grnxx::Bool(true));

    source = mersenne_twister() % 129;
    test.float_values[i] =
        (source == 128) ? grnxx::Float::na() : grnxx::Float(source / 128.0);
    source = mersenne_twister() % 129;
    test.float2_values[i] =
        (source == 128) ? grnxx::Float::na() : grnxx::Float(source / 128.0);

    test.scores[i] = test.bool_values[i].is_true() ?
                     test.float_values[i] : MISSING_SCORE;
    test.scores2[i] = test.bool2_values[i].is_true() ?
                      test.float2_values[i] : MISSING_SCORE;
  }

  // Store generated values into columns.
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = test.table->insert_row();
    bool_column->set(row_id, test.bool_values[i]);
    bool2_column->set(row_id, test.bool2_values[i]);
    float_column->set(row_id, test.float_values[i]);
    float2_column->set(row_id, test.float2_values[i]);
  }
}

grnxx::Array<grnxx::Record> create_input(const char *bool_name,
                                         const char *float_name) {
  // Create a cursor to read records.
  auto cursor = test.table->create_cursor();

  // Read all the records.
  grnxx::Array<grnxx::Record> records;
  size_t count = cursor->read_all(&records);
  assert(count == test.table->num_rows());

  // Create an object to create expressions.
  auto expression_builder = grnxx::ExpressionBuilder::create(test.table);

  // Apply a filter to the records.
  expression_builder->push_column(bool_name);
  auto expression = expression_builder->release();
  expression->filter(&records);

  // Apply an adjuster to the records.
  expression_builder->push_column(float_name);
  expression = expression_builder->release();
  expression->adjust(&records);

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
  // Create a merger.
  auto merger = grnxx::Merger::create(options);

  // Create copies of input records.
  grnxx::Array<grnxx::Record> copy_1;
  copy_1.resize(input_1.size());
  for (size_t i = 0; i < copy_1.size(); ++i) {
    copy_1[i] = input_1[i];
  }
  grnxx::Array<grnxx::Record> copy_2;
  copy_2.resize(input_2.size());
  for (size_t i = 0; i < copy_2.size(); ++i) {
    copy_2[i] = input_2[i];
  }

  // Merge records.
  grnxx::Array<grnxx::Record> output;
  merger->merge(&copy_1, &copy_2, &output);
  return output;
}

void test_and() {
  // Create input records.
  auto input_1 = create_input_1();
  auto input_2 = create_input_2();

  // Set options.
  grnxx::MergerOptions options;
  options.logical_operator_type = GRNXX_MERGER_AND;
  options.missing_score = MISSING_SCORE;

  // Merge records (PLUS).
  options.score_operator_type = GRNXX_MERGER_PLUS;
  auto output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() &
           test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id] + test.scores2[row_id]));
  }
  size_t count = 0;
  for (size_t i = 0; i < test.table->num_rows(); ++i) {
    if (test.bool_values[i].is_true() & test.bool2_values[i].is_true()) {
      assert(output[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(count == output.size());

  // Merge records (MINUS).
  options.score_operator_type = GRNXX_MERGER_MINUS;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() &
           test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id] - test.scores2[row_id]));
  }

  // Merge records (MULTIPLICATION).
  options.score_operator_type = GRNXX_MERGER_MULTIPLICATION;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() &
           test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id] * test.scores2[row_id]));
  }

  // Merge records (LEFT).
  options.score_operator_type = GRNXX_MERGER_LEFT;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() &
           test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id]));
  }

  // Merge records (RIGHT).
  options.score_operator_type = GRNXX_MERGER_RIGHT;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() &
           test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores2[row_id]));
  }

  // Merge records (ZERO).
  options.score_operator_type = GRNXX_MERGER_ZERO;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() &
           test.bool2_values[row_id].is_true());
    assert(output[i].score.raw() == 0.0);
  }
}

void test_or() {
  // Create input records.
  auto input_1 = create_input_1();
  auto input_2 = create_input_2();

  // Set options.
  grnxx::MergerOptions options;
  options.logical_operator_type = GRNXX_MERGER_OR;
  options.missing_score = MISSING_SCORE;

  // Merge records (PLUS).
  options.score_operator_type = GRNXX_MERGER_PLUS;
  auto output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() |
           test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id] + test.scores2[row_id]));
  }
  size_t count = 0;
  for (size_t i = 0; i < test.table->num_rows(); ++i) {
    if (test.bool_values[i].is_true() |
        test.bool2_values[i].is_true()) {
      ++count;
    }
  }
  assert(count == output.size());

  // Merge records (MINUS).
  options.score_operator_type = GRNXX_MERGER_MINUS;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() |
           test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id] - test.scores2[row_id]));
  }

  // Merge records (MULTIPLICATION).
  options.score_operator_type = GRNXX_MERGER_MULTIPLICATION;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() |
           test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id] * test.scores2[row_id]));
  }

  // Merge records (LEFT).
  options.score_operator_type = GRNXX_MERGER_LEFT;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() |
           test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id]));
  }

  // Merge records (RIGHT).
  options.score_operator_type = GRNXX_MERGER_RIGHT;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() |
           test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores2[row_id]));
  }

  // Merge records (ZERO).
  options.score_operator_type = GRNXX_MERGER_ZERO;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() |
           test.bool2_values[row_id].is_true());
    assert(output[i].score.raw() == 0.0);
  }
}

void test_xor() {
  // Create input records.
  auto input_1 = create_input_1();
  auto input_2 = create_input_2();

  // Set options.
  grnxx::MergerOptions options;
  options.logical_operator_type = GRNXX_MERGER_XOR;
  options.missing_score = MISSING_SCORE;

  // Merge records (PLUS).
  options.score_operator_type = GRNXX_MERGER_PLUS;
  auto output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() ^
           test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id] + test.scores2[row_id]));
  }
  size_t count = 0;
  for (size_t i = 0; i < test.table->num_rows(); ++i) {
    if (test.bool_values[i].is_true() ^ test.bool2_values[i].is_true()) {
      ++count;
    }
  }
  assert(count == output.size());

  // Merge records (MINUS).
  options.score_operator_type = GRNXX_MERGER_MINUS;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() ^
           test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id] - test.scores2[row_id]));
  }

  // Merge records (MULTIPLICATION).
  options.score_operator_type = GRNXX_MERGER_MULTIPLICATION;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() ^
           test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id] * test.scores2[row_id]));
  }

  // Merge records (LEFT).
  options.score_operator_type = GRNXX_MERGER_LEFT;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() ^
           test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id]));
  }

  // Merge records (RIGHT).
  options.score_operator_type = GRNXX_MERGER_RIGHT;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() ^
           test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores2[row_id]));
  }

  // Merge records (ZERO).
  options.score_operator_type = GRNXX_MERGER_ZERO;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() ^
           test.bool2_values[row_id].is_true());
    assert(output[i].score.raw() == 0.0);
  }
}

void test_minus() {
  // Create input records.
  auto input_1 = create_input_1();
  auto input_2 = create_input_2();

  // Set options.
  grnxx::MergerOptions options;
  options.logical_operator_type = GRNXX_MERGER_MINUS;
  options.missing_score = MISSING_SCORE;

  // Merge records (PLUS).
  options.score_operator_type = GRNXX_MERGER_PLUS;
  auto output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() &
           !test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id] + test.scores2[row_id]));
  }
  size_t count = 0;
  for (size_t i = 0; i < test.table->num_rows(); ++i) {
    if (test.bool_values[i].is_true() & !test.bool2_values[i].is_true()) {
      ++count;
    }
  }
  assert(count == output.size());

  // Merge records (MINUS).
  options.score_operator_type = GRNXX_MERGER_MINUS;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() &
           !test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id] - test.scores2[row_id]));
  }

  // Merge records (MULTIPLICATION).
  options.score_operator_type = GRNXX_MERGER_MULTIPLICATION;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() &
           !test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id] * test.scores2[row_id]));
  }

  // Merge records (LEFT).
  options.score_operator_type = GRNXX_MERGER_LEFT;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() &
           !test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id]));
  }

  // Merge records (RIGHT).
  options.score_operator_type = GRNXX_MERGER_RIGHT;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() &
           !test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores2[row_id]));
  }

  // Merge records (ZERO).
  options.score_operator_type = GRNXX_MERGER_ZERO;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true() &
           !test.bool2_values[row_id].is_true());
    assert(output[i].score.raw() == 0.0);
  }
}

void test_left() {
  // Create input records.
  auto input_1 = create_input_1();
  auto input_2 = create_input_2();

  // Set options.
  grnxx::MergerOptions options;
  options.logical_operator_type = GRNXX_MERGER_LEFT;
  options.missing_score = MISSING_SCORE;

  // Merge records (PLUS).
  options.score_operator_type = GRNXX_MERGER_PLUS;
  auto output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id] + test.scores2[row_id]));
  }
  size_t count = 0;
  for (size_t i = 0; i < test.table->num_rows(); ++i) {
    if (test.bool_values[i].is_true()) {
      ++count;
    }
  }
  assert(count == output.size());

  // Merge records (MINUS).
  options.score_operator_type = GRNXX_MERGER_MINUS;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id] - test.scores2[row_id]));
  }

  // Merge records (MULTIPLICATION).
  options.score_operator_type = GRNXX_MERGER_MULTIPLICATION;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id] * test.scores2[row_id]));
  }

  // Merge records (LEFT).
  options.score_operator_type = GRNXX_MERGER_LEFT;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id]));
  }

  // Merge records (RIGHT).
  options.score_operator_type = GRNXX_MERGER_RIGHT;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true());
    assert(output[i].score.match(test.scores2[row_id]));
  }

  // Merge records (ZERO).
  options.score_operator_type = GRNXX_MERGER_ZERO;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool_values[row_id].is_true());
    assert(output[i].score.raw() == 0.0);
  }
}

void test_right() {
  // Create input records.
  auto input_1 = create_input_1();
  auto input_2 = create_input_2();

  // Set options.
  grnxx::MergerOptions options;
  options.logical_operator_type = GRNXX_MERGER_RIGHT;
  options.missing_score = MISSING_SCORE;

  // Merge records (PLUS).
  options.score_operator_type = GRNXX_MERGER_PLUS;
  auto output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id] + test.scores2[row_id]));
  }
  size_t count = 0;
  for (size_t i = 0; i < test.table->num_rows(); ++i) {
    if (test.bool2_values[i].is_true()) {
      ++count;
    }
  }
  assert(count == output.size());

  // Merge records (MINUS).
  options.score_operator_type = GRNXX_MERGER_MINUS;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id] - test.scores2[row_id]));
  }

  // Merge records (MULTIPLICATION).
  options.score_operator_type = GRNXX_MERGER_MULTIPLICATION;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id] * test.scores2[row_id]));
  }

  // Merge records (LEFT).
  options.score_operator_type = GRNXX_MERGER_LEFT;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores[row_id]));
  }

  // Merge records (RIGHT).
  options.score_operator_type = GRNXX_MERGER_RIGHT;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool2_values[row_id].is_true());
    assert(output[i].score.match(test.scores2[row_id]));
  }

  // Merge records (ZERO).
  options.score_operator_type = GRNXX_MERGER_ZERO;
  output = merge_records(input_1, input_2, options);
  for (size_t i = 0; i < output.size(); ++i) {
    size_t row_id = output[i].row_id.raw();
    assert(test.bool2_values[row_id].is_true());
    assert(output[i].score.raw() == 0.0);
  }
}

int main() {
  for (size_t i = 0; i < 5; ++i) {
    init_test();
    test_and();
    test_or();
    test_xor();
    test_minus();
    test_left();
    test_right();
  }
  return 0;
}
