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
#include "grnxx/datum.hpp"
#include "grnxx/db.hpp"
#include "grnxx/error.hpp"
#include "grnxx/expression.hpp"
#include "grnxx/table.hpp"

struct {
  grnxx::unique_ptr<grnxx::DB> db;
  grnxx::Table *table;
  grnxx::Array<grnxx::Bool> bool_values;
  grnxx::Array<grnxx::Int> int_values;
  grnxx::Array<grnxx::Float> float_values;
  grnxx::Array<grnxx::Text> text_values;
  grnxx::Array<std::string> text_bodies;
} test;

void init_test() {
  grnxx::Error error;

  // Create a database with the default options.
  test.db = grnxx::open_db(&error, "");
  assert(test.db);

  // Create a table with the default options.
  test.table = test.db->create_table(&error, "Table");
  assert(test.table);

  // Create columns for Bool, Int, Float, and Text values.
  auto bool_column =
      test.table->create_column(&error, "Bool", grnxx::BOOL_DATA);
  assert(bool_column);
  auto int_column =
      test.table->create_column(&error, "Int", grnxx::INT_DATA);
  assert(int_column);
  auto float_column =
      test.table->create_column(&error, "Float", grnxx::FLOAT_DATA);
  assert(float_column);
  auto text_column =
      test.table->create_column(&error, "Text", grnxx::TEXT_DATA);
  assert(text_column);

  // Generate random values.
  // Bool: true or false.
  // Int: [0, 100).
  // Float: [0.0, 1.0].
  // Text: length = [1, 4], byte = ['0', '9'].
  constexpr grnxx::Int NUM_ROWS = 1 << 16;
  std::mt19937_64 mersenne_twister;
  assert(test.bool_values.resize(&error, NUM_ROWS + 1));
  assert(test.int_values.resize(&error, NUM_ROWS + 1));
  assert(test.float_values.resize(&error, NUM_ROWS + 1));
  assert(test.text_values.resize(&error, NUM_ROWS + 1));
  assert(test.text_bodies.resize(&error, NUM_ROWS + 1));
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    test.bool_values.set(i, (mersenne_twister() & 1) != 0);
    test.int_values.set(i, mersenne_twister() % 100);
    test.float_values.set(i, 1.0 * mersenne_twister() / mersenne_twister.max());

    grnxx::Int length = (mersenne_twister() % 4) + 1;
    test.text_bodies[i].resize(length);
    for (grnxx::Int j = 0; j < length; ++j) {
      test.text_bodies[i][j] = '0' + (mersenne_twister() % 10);
    }
    test.text_values.set(i, grnxx::Text(test.text_bodies[i].data(), length));
  }

  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    grnxx::Int row_id;
    assert(test.table->insert_row(&error, grnxx::NULL_ROW_ID,
                                  grnxx::Datum(), &row_id));
    assert(bool_column->set(&error, row_id, test.bool_values[i]));
    assert(int_column->set(&error, row_id, test.int_values[i]));
    assert(float_column->set(&error, row_id, test.float_values[i]));
    assert(text_column->set(&error, row_id, test.text_values[i]));
  }
}

void test_constant() {
  grnxx::Error error;

  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
  assert(builder);

  // Test an expression (true).
  assert(builder->push_datum(&error, grnxx::Bool(true)));
  auto expression = builder->release(&error);
  assert(expression);

  grnxx::Array<grnxx::Record> records;
  auto cursor = test.table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == test.table->num_rows());

  grnxx::Array<grnxx::Bool> bool_results;
  assert(expression->evaluate(&error, records, &bool_results));
  assert(bool_results.size() == test.table->num_rows());
  for (grnxx::Int i = 0; i < bool_results.size(); ++i) {
    assert(bool_results[i]);
  }

  assert(expression->filter(&error, &records));
  assert(records.size() == test.table->num_rows());

  // Test an expression (false).
  assert(builder->push_datum(&error, grnxx::Bool(false)));
  expression = builder->release(&error);
  assert(expression);

  bool_results.clear();
  assert(expression->evaluate(&error, records, &bool_results));
  assert(bool_results.size() == test.table->num_rows());
  for (grnxx::Int i = 0; i < bool_results.size(); ++i) {
    assert(!bool_results[i]);
  }

  assert(expression->filter(&error, &records));
  assert(records.size() == 0);

  // Test an expression (100).
  assert(builder->push_datum(&error, grnxx::Int(100)));
  expression = builder->release(&error);
  assert(expression);

  cursor = test.table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == test.table->num_rows());

  grnxx::Array<grnxx::Int> int_results;
  assert(expression->evaluate(&error, records, &int_results));
  assert(int_results.size() == test.table->num_rows());
  for (grnxx::Int i = 0; i < int_results.size(); ++i) {
    assert(int_results[i] == 100);
  }

  // Test an expression (1.25).
  assert(builder->push_datum(&error, grnxx::Float(1.25)));
  expression = builder->release(&error);
  assert(expression);

  grnxx::Array<grnxx::Float> float_results;
  assert(expression->evaluate(&error, records, &float_results));
  assert(float_results.size() == test.table->num_rows());
  for (grnxx::Int i = 0; i < float_results.size(); ++i) {
    assert(float_results[i] == 1.25);
  }

  assert(expression->adjust(&error, &records));
  assert(records.size() == test.table->num_rows());
  for (grnxx::Int i = 0; i < records.size(); ++i) {
    assert(records.get_score(i) == 1.25);
  }

  // Test an expression ("ABC").
  assert(builder->push_datum(&error, grnxx::Text("ABC")));
  expression = builder->release(&error);
  assert(expression);

  grnxx::Array<grnxx::Text> text_results;
  assert(expression->evaluate(&error, records, &text_results));
  assert(text_results.size() == test.table->num_rows());
  for (grnxx::Int i = 0; i < text_results.size(); ++i) {
    assert(text_results[i] == "ABC");
  }
}

void test_column() {
  grnxx::Error error;

  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
  assert(builder);

  // Test an expression (Bool).
  assert(builder->push_column(&error, "Bool"));
  auto expression = builder->release(&error);
  assert(expression);

  grnxx::Array<grnxx::Record> records;
  auto cursor = test.table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == test.table->num_rows());

  grnxx::Array<grnxx::Bool> bool_results;
  assert(expression->evaluate(&error, records, &bool_results));
  assert(bool_results.size() == test.table->num_rows());
  for (grnxx::Int i = 0; i < bool_results.size(); ++i) {
    grnxx::Int row_id = records.get_row_id(i);
    assert(bool_results[i] == test.bool_values[row_id]);
  }

  assert(expression->filter(&error, &records));
  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i < test.bool_values.size(); ++i) {
    if (test.bool_values[i]) {
      assert(records.get_row_id(count) == i);
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (Int).
  assert(builder->push_column(&error, "Int"));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = test.table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == test.table->num_rows());

  grnxx::Array<grnxx::Int> int_results;
  assert(expression->evaluate(&error, records, &int_results));
  assert(int_results.size() == test.table->num_rows());
  for (grnxx::Int i = 0; i < int_results.size(); ++i) {
    grnxx::Int row_id = records.get_row_id(i);
    assert(int_results[i] == test.int_values[row_id]);
  }

  // Test an expression (Float).
  assert(builder->push_column(&error, "Float"));
  expression = builder->release(&error);
  assert(expression);

  grnxx::Array<grnxx::Float> float_results;
  assert(expression->evaluate(&error, records, &float_results));
  assert(float_results.size() == test.table->num_rows());
  for (grnxx::Int i = 0; i < float_results.size(); ++i) {
    grnxx::Int row_id = records.get_row_id(i);
    assert(float_results[i] == test.float_values[row_id]);
  }

  assert(expression->adjust(&error, &records));
  assert(records.size() == test.table->num_rows());
  for (grnxx::Int i = 0; i < records.size(); ++i) {
    grnxx::Int row_id = records.get_row_id(i);
    assert(records.get_score(i) == test.float_values[row_id]);
  }

  // Test an expression (Text).
  assert(builder->push_column(&error, "Text"));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = test.table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == test.table->num_rows());

  grnxx::Array<grnxx::Text> text_results;
  assert(expression->evaluate(&error, records, &text_results));
  assert(text_results.size() == test.table->num_rows());
  for (grnxx::Int i = 0; i < text_results.size(); ++i) {
    grnxx::Int row_id = records.get_row_id(i);
    assert(text_results[i] == test.text_values[row_id]);
  }

  // Test and expression (_id).
  assert(builder->push_column(&error, "_id"));
  expression = builder->release(&error);
  assert(expression);

  grnxx::Array<grnxx::Int> id_results;
  assert(expression->evaluate(&error, records, &id_results));
  assert(id_results.size() == test.table->num_rows());
  for (grnxx::Int i = 0; i < id_results.size(); ++i) {
    assert(id_results[i] == records.get_row_id(i));
  }

  // Test and expression (_score).
  assert(builder->push_column(&error, "_score"));
  expression = builder->release(&error);
  assert(expression);

  grnxx::Array<grnxx::Float> score_results;
  assert(expression->evaluate(&error, records, &score_results));
  assert(score_results.size() == test.table->num_rows());
  for (grnxx::Int i = 0; i < score_results.size(); ++i) {
    assert(score_results[i] == records.get_score(i));
  }

  assert(expression->adjust(&error, &records));
  assert(records.size() == test.table->num_rows());
  for (grnxx::Int i = 0; i < records.size(); ++i) {
    assert(records.get_score(i) == 0.0);
  }
}

void test_logical_not() {
  grnxx::Error error;

  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
  assert(builder);

  // Test an expression (!Bool).
  assert(builder->push_column(&error, "Bool"));
  assert(builder->push_operator(&error, grnxx::LOGICAL_NOT_OPERATOR));
  auto expression = builder->release(&error);
  assert(expression);

  grnxx::Array<grnxx::Record> records;
  auto cursor = test.table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == test.table->num_rows());

  grnxx::Array<grnxx::Bool> bool_results;
  assert(expression->evaluate(&error, records, &bool_results));
  assert(bool_results.size() == test.table->num_rows());
  for (grnxx::Int i = 0; i < bool_results.size(); ++i) {
    grnxx::Int row_id = records.get_row_id(i);
    assert(bool_results[i] == !test.bool_values[row_id]);
  }

  assert(expression->filter(&error, &records));
  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i < test.bool_values.size(); ++i) {
    if (!test.bool_values[i]) {
      assert(records.get_row_id(count) == i);
      ++count;
    }
  }
  assert(records.size() == count);
}

void test_bitwise_not() {
  grnxx::Error error;

  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
  assert(builder);

  // Test an expression (~Bool).
  assert(builder->push_column(&error, "Bool"));
  assert(builder->push_operator(&error, grnxx::BITWISE_NOT_OPERATOR));
  auto expression = builder->release(&error);
  assert(expression);

  grnxx::Array<grnxx::Record> records;
  auto cursor = test.table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == test.table->num_rows());

  grnxx::Array<grnxx::Bool> bool_results;
  assert(expression->evaluate(&error, records, &bool_results));
  assert(bool_results.size() == test.table->num_rows());
  for (grnxx::Int i = 0; i < bool_results.size(); ++i) {
    grnxx::Int row_id = records.get_row_id(i);
    assert(bool_results[i] == !test.bool_values[row_id]);
  }

  assert(expression->filter(&error, &records));
  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i < test.bool_values.size(); ++i) {
    if (!test.bool_values[i]) {
      assert(records.get_row_id(count) == i);
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (~Int).
  assert(builder->push_column(&error, "Int"));
  assert(builder->push_operator(&error, grnxx::BITWISE_NOT_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = test.table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == test.table->num_rows());

  grnxx::Array<grnxx::Int> int_results;
  assert(expression->evaluate(&error, records, &int_results));
  assert(int_results.size() == test.table->num_rows());
  for (grnxx::Int i = 0; i < int_results.size(); ++i) {
    grnxx::Int row_id = records.get_row_id(i);
    assert(int_results[i] == ~test.int_values[row_id]);
  }
}

void test_positive() {
}

void test_negative() {
}

void test_expression() {
  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table with the default options.
  auto table = db->create_table(&error, "Table");
  assert(table);

  // Create columns for Bool, Int, Float, and Text values.
  auto bool_column = table->create_column(&error, "BoolColumn",
                                          grnxx::BOOL_DATA);
  assert(bool_column);
  auto int_column = table->create_column(&error, "IntColumn",
                                         grnxx::INT_DATA);
  assert(int_column);
  auto float_column = table->create_column(&error, "FloatColumn",
                                           grnxx::FLOAT_DATA);
  assert(float_column);
  auto text_column = table->create_column(&error, "TextColumn",
                                           grnxx::TEXT_DATA);
  assert(text_column);

  // Store the following data.
  //
  // RowID BoolColumn IntColumn FloatColumn TextColumn
  //     1      false       123       -0.25      "ABC"
  //     2       true       456        0.25      "XYZ"
  grnxx::Int row_id;
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));
  assert(bool_column->set(&error, row_id, grnxx::Bool(false)));
  assert(int_column->set(&error, row_id, grnxx::Int(123)));
  assert(float_column->set(&error, row_id, grnxx::Float(-0.25)));
  assert(text_column->set(&error, row_id, grnxx::Text("ABC")));

  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));
  assert(bool_column->set(&error, row_id, grnxx::Bool(true)));
  assert(int_column->set(&error, row_id, grnxx::Int(456)));
  assert(float_column->set(&error, row_id, grnxx::Float(0.25)));
  assert(text_column->set(&error, row_id, grnxx::Text("XYZ")));

  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(&error, table);
  assert(builder);

  // Test an expression (true).
  assert(builder->push_datum(&error, grnxx::Bool(true)));
  auto expression = builder->release(&error);
  assert(expression);

  grnxx::Array<grnxx::Record> records;
  auto cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 2);

  // Test an expression (100 == 100).
  assert(builder->push_datum(&error, grnxx::Int(100)));
  assert(builder->push_datum(&error, grnxx::Int(100)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 2);

  // Test an expression (BoolColumn).
  assert(builder->push_column(&error, "BoolColumn"));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 2);

  // Test an expression (IntColumn == 123).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(123)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 1);

  // Test an expression (IntColumn != 123).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(123)));
  assert(builder->push_operator(&error, grnxx::NOT_EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 2);

  // Test an expresison (IntColumn < 300).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(300)));
  assert(builder->push_operator(&error, grnxx::LESS_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 1);

  // Test an expresison (TextColumn <= "ABC").
  assert(builder->push_column(&error, "TextColumn"));
  assert(builder->push_datum(&error, grnxx::Text("ABC")));
  assert(builder->push_operator(&error, grnxx::LESS_EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 1);

  // Test an expresison (TextColumn > "ABC").
  assert(builder->push_column(&error, "TextColumn"));
  assert(builder->push_datum(&error, grnxx::Text("ABC")));
  assert(builder->push_operator(&error, grnxx::GREATER_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 2);

  // Test an expresison (IntColumn >= 456).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(456)));
  assert(builder->push_operator(&error, grnxx::GREATER_EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 2);

  // Test an expresison ((FloatColumn > 0.0) && BoolColumn).
  assert(builder->push_column(&error, "FloatColumn"));
  assert(builder->push_datum(&error, grnxx::Float(0.0)));
  assert(builder->push_operator(&error, grnxx::GREATER_OPERATOR));
  assert(builder->push_column(&error, "BoolColumn"));
  assert(builder->push_operator(&error, grnxx::LOGICAL_AND_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 2);

  // Test an expresison (false || BoolColumn).
  assert(builder->push_datum(&error, grnxx::Bool(false)));
  assert(builder->push_column(&error, "BoolColumn"));
  assert(builder->push_operator(&error, grnxx::LOGICAL_OR_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 2);

  // Test an expresison (+IntColumn == 123).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_operator(&error, grnxx::POSITIVE_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Int(123)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 1);

  // Test an expresison (-IntColumn == 456).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_operator(&error, grnxx::NEGATIVE_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Int(-456)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 2);

  // Test an expresison (Int(FloatColumn) == 0).
  assert(builder->push_column(&error, "FloatColumn"));
  assert(builder->push_operator(&error, grnxx::TO_INT_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Int(0)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 2);

  // Test an expresison (Float(IntColumn) < 300.0).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_operator(&error, grnxx::TO_FLOAT_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Float(300.0)));
  assert(builder->push_operator(&error, grnxx::LESS_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 1);

  // Test an expresison ((IntColumn & 255) == 200).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(255)));
  assert(builder->push_operator(&error, grnxx::BITWISE_AND_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Int(200)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 2);

  // Test an expresison ((IntColumn | 256) == 379).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(256)));
  assert(builder->push_operator(&error, grnxx::BITWISE_OR_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Int(379)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 1);

  // Test an expresison ((IntColumn ^ 255) == 132).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(255)));
  assert(builder->push_operator(&error, grnxx::BITWISE_XOR_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Int(132)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 1);

  // Test an expresison ((IntColumn + 100) == 223).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(100)));
  assert(builder->push_operator(&error, grnxx::PLUS_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Int(223)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 1);

  // Test an expresison ((FloatColumn - 0.25) == 0.0).
  assert(builder->push_column(&error, "FloatColumn"));
  assert(builder->push_datum(&error, grnxx::Float(0.25)));
  assert(builder->push_operator(&error, grnxx::MINUS_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Float(0.0)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 2);

  // Test an expresison ((IntColumn * 2) == 912).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(2)));
  assert(builder->push_operator(&error, grnxx::MULTIPLICATION_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Int(912)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 2);

  // Test an expresison (_score + 1.0).
  assert(builder->push_column(&error, "_score"));
  assert(builder->push_datum(&error, grnxx::Float(1.0)));
  assert(builder->push_operator(&error, grnxx::PLUS_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->adjust(&error, &records));
  assert(records.size() == 2);
  assert(records.get(0).row_id == 1);
  assert(records.get(0).score == 1.0);
  assert(records.get(1).row_id == 2);
  assert(records.get(1).score == 1.0);

  // Test an expresison (IntColumn + 100).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(100)));
  assert(builder->push_operator(&error, grnxx::PLUS_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  grnxx::Array<grnxx::Int> result_set;
  assert(expression->evaluate(&error, records, &result_set));
  assert(result_set.size() == 2);
  assert(result_set[0] == 223);
  assert(result_set[1] == 556);
}

int main() {
  init_test();
  test_constant();
  test_column();
  test_logical_not();
  test_bitwise_not();
  test_positive();
  test_negative();
  test_expression();
  return 0;
}
