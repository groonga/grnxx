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
#include "grnxx/sorter.hpp"

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
  grnxx::DataType data_type = grnxx::BOOL_DATA;
  auto bool_column = test.table->create_column(&error, "Bool", data_type);
  assert(bool_column);

  data_type = grnxx::INT_DATA;
  auto int_column = test.table->create_column(&error, "Int", data_type);
  assert(int_column);

  data_type = grnxx::FLOAT_DATA;
  auto float_column = test.table->create_column(&error, "Float", data_type);
  assert(float_column);

  data_type = grnxx::TEXT_DATA;
  auto text_column = test.table->create_column(&error, "Text", data_type);
  assert(text_column);

  // Generate random values.
  // Bool: true or false.
  // Int: [0, 100).
  // Float: [0.0, 1.0].
  // Text: length = [1, 4], byte = ['0', '9'].
  constexpr grnxx::Int NUM_ROWS = 1 << 16;
  constexpr grnxx::Int MIN_LENGTH = 1;
  constexpr grnxx::Int MAX_LENGTH = 4;
  std::mt19937_64 mersenne_twister;
  assert(test.bool_values.resize(&error, NUM_ROWS + 1));
  assert(test.int_values.resize(&error, NUM_ROWS + 1));
  assert(test.float_values.resize(&error, NUM_ROWS + 1));
  assert(test.text_values.resize(&error, NUM_ROWS + 1));
  assert(test.text_bodies.resize(&error, NUM_ROWS + 1));
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    test.bool_values.set(i, (mersenne_twister() & 1) != 0);
    test.int_values.set(i, mersenne_twister() % 100);
    if ((mersenne_twister() % 16) == 0) {
      test.float_values.set(i, std::numeric_limits<grnxx::Float>::quiet_NaN());
    } else {
      constexpr auto MAX_VALUE = mersenne_twister.max();
      test.float_values.set(i, 1.0 * mersenne_twister() / MAX_VALUE);
    }
    grnxx::Int length =
        (mersenne_twister() % (MAX_LENGTH - MIN_LENGTH)) + MIN_LENGTH;
    test.text_bodies[i].resize(length);
    for (grnxx::Int j = 0; j < length; ++j) {
      test.text_bodies[i][j] = '0' + (mersenne_twister() % 10);
    }
    test.text_values.set(i, grnxx::Text(test.text_bodies[i].data(), length));
  }

  // Store generated values into columns.
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

void test_bool() {
  grnxx::Error error;

  // Create a cursor which reads all the records.
  auto cursor = test.table->create_cursor(&error);
  assert(cursor);
  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) == test.table->num_rows());

  // Create an object for building expressions.
  auto expression_builder =
      grnxx::ExpressionBuilder::create(&error, test.table);
  assert(expression_builder);

  // Create a regular sorter.
  grnxx::Array<grnxx::SortOrder> orders;
  assert(orders.resize(&error, 1));
  assert(expression_builder->push_column(&error, "Bool"));
  auto expression = expression_builder->release(&error);
  assert(expression);
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::REGULAR_ORDER;
  auto sorter = grnxx::Sorter::create(&error, std::move(orders));

  sorter->sort(&error, &records);
  for (grnxx::Int i = 1; i < records.size(); ++i) {
    grnxx::Int lhs_row_id = records.get_row_id(i - 1);
    grnxx::Int rhs_row_id = records.get_row_id(i);
    grnxx::Bool lhs_value = test.bool_values[lhs_row_id];
    grnxx::Bool rhs_value = test.bool_values[rhs_row_id];
    assert(!lhs_value || rhs_value);
  }

  // Create a reverse sorter.
  assert(orders.resize(&error, 1));
  assert(expression_builder->push_column(&error, "Bool"));
  expression = expression_builder->release(&error);
  assert(expression);
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::REVERSE_ORDER;
  sorter = grnxx::Sorter::create(&error, std::move(orders));

  sorter->sort(&error, &records);
  for (grnxx::Int i = 1; i < records.size(); ++i) {
    grnxx::Int lhs_row_id = records.get_row_id(i - 1);
    grnxx::Int rhs_row_id = records.get_row_id(i);
    grnxx::Bool lhs_value = test.bool_values[lhs_row_id];
    grnxx::Bool rhs_value = test.bool_values[rhs_row_id];
    assert(lhs_value || !rhs_value);
  }

  // Create a multiple order sorter.
  assert(orders.resize(&error, 2));
  assert(expression_builder->push_column(&error, "Bool"));
  expression = expression_builder->release(&error);
  assert(expression);
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::REGULAR_ORDER;
  assert(expression_builder->push_column(&error, "_id"));
  expression = expression_builder->release(&error);
  assert(expression);
  orders[1].expression = std::move(expression);
  orders[1].type = grnxx::REGULAR_ORDER;
  sorter = grnxx::Sorter::create(&error, std::move(orders));

  sorter->sort(&error, &records);
  for (grnxx::Int i = 1; i < records.size(); ++i) {
    grnxx::Int lhs_row_id = records.get_row_id(i - 1);
    grnxx::Int rhs_row_id = records.get_row_id(i);
    grnxx::Bool lhs_value = test.bool_values[lhs_row_id];
    grnxx::Bool rhs_value = test.bool_values[rhs_row_id];
    assert(!lhs_value || rhs_value);
    if (lhs_value == rhs_value) {
      assert(lhs_row_id < rhs_row_id);
    }
  }
}

void test_int() {
  grnxx::Error error;

  // Create a cursor which reads all the records.
  auto cursor = test.table->create_cursor(&error);
  assert(cursor);
  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) == test.table->num_rows());

  // Create an object for building expressions.
  auto expression_builder =
      grnxx::ExpressionBuilder::create(&error, test.table);
  assert(expression_builder);

  // Create a regular sorter.
  grnxx::Array<grnxx::SortOrder> orders;
  assert(orders.resize(&error, 1));
  assert(expression_builder->push_column(&error, "Int"));
  auto expression = expression_builder->release(&error);
  assert(expression);
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::REGULAR_ORDER;
  auto sorter = grnxx::Sorter::create(&error, std::move(orders));

  sorter->sort(&error, &records);
  for (grnxx::Int i = 1; i < records.size(); ++i) {
    grnxx::Int lhs_row_id = records.get_row_id(i - 1);
    grnxx::Int rhs_row_id = records.get_row_id(i);
    grnxx::Int lhs_value = test.int_values[lhs_row_id];
    grnxx::Int rhs_value = test.int_values[rhs_row_id];
    assert(lhs_value <= rhs_value);
  }

  // Create a reverse sorter.
  assert(orders.resize(&error, 1));
  assert(expression_builder->push_column(&error, "Int"));
  expression = expression_builder->release(&error);
  assert(expression);
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::REVERSE_ORDER;
  sorter = grnxx::Sorter::create(&error, std::move(orders));

  sorter->sort(&error, &records);
  for (grnxx::Int i = 1; i < records.size(); ++i) {
    grnxx::Int lhs_row_id = records.get_row_id(i - 1);
    grnxx::Int rhs_row_id = records.get_row_id(i);
    grnxx::Int lhs_value = test.int_values[lhs_row_id];
    grnxx::Int rhs_value = test.int_values[rhs_row_id];
    assert(lhs_value >= rhs_value);
  }

  // Create a multiple order sorter.
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
  sorter = grnxx::Sorter::create(&error, std::move(orders));

  sorter->sort(&error, &records);
  for (grnxx::Int i = 1; i < records.size(); ++i) {
    grnxx::Int lhs_row_id = records.get_row_id(i - 1);
    grnxx::Int rhs_row_id = records.get_row_id(i);
    grnxx::Int lhs_value = test.int_values[lhs_row_id];
    grnxx::Int rhs_value = test.int_values[rhs_row_id];
    assert(lhs_value <= rhs_value);
    if (lhs_value == rhs_value) {
      assert(lhs_row_id < rhs_row_id);
    }
  }
}

struct LessEqual {
  bool operator()(grnxx::Float lhs, grnxx::Float rhs) const {
    // Numbers are prior to NaN.
    if (std::isnan(rhs)) {
      return true;
    } else if (std::isnan(lhs)) {
      return false;
    }
    return lhs <= rhs;
  }
};

struct Equal {
  bool operator()(grnxx::Float lhs, grnxx::Float rhs) const {
    return (lhs == rhs) || (std::isnan(lhs) && std::isnan(rhs));
  }
};

void test_float() {
  grnxx::Error error;

  // Create a cursor which reads all the records.
  auto cursor = test.table->create_cursor(&error);
  assert(cursor);
  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) == test.table->num_rows());

  // Create an object for building expressions.
  auto expression_builder =
      grnxx::ExpressionBuilder::create(&error, test.table);
  assert(expression_builder);

  // Create a regular sorter.
  grnxx::Array<grnxx::SortOrder> orders;
  assert(orders.resize(&error, 1));
  assert(expression_builder->push_column(&error, "Float"));
  auto expression = expression_builder->release(&error);
  assert(expression);
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::REGULAR_ORDER;
  auto sorter = grnxx::Sorter::create(&error, std::move(orders));

  sorter->sort(&error, &records);
  for (grnxx::Int i = 1; i < records.size(); ++i) {
    grnxx::Int lhs_row_id = records.get_row_id(i - 1);
    grnxx::Int rhs_row_id = records.get_row_id(i);
    grnxx::Float lhs_value = test.float_values[lhs_row_id];
    grnxx::Float rhs_value = test.float_values[rhs_row_id];
    assert(LessEqual()(lhs_value, rhs_value));
  }

  // Create a reverse sorter.
  assert(orders.resize(&error, 1));
  assert(expression_builder->push_column(&error, "Float"));
  expression = expression_builder->release(&error);
  assert(expression);
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::REVERSE_ORDER;
  sorter = grnxx::Sorter::create(&error, std::move(orders));

  sorter->sort(&error, &records);
  for (grnxx::Int i = 1; i < records.size(); ++i) {
    grnxx::Int lhs_row_id = records.get_row_id(i - 1);
    grnxx::Int rhs_row_id = records.get_row_id(i);
    grnxx::Float lhs_value = test.float_values[lhs_row_id];
    grnxx::Float rhs_value = test.float_values[rhs_row_id];
    assert(LessEqual()(rhs_value, lhs_value));
  }

  // Create a multiple order sorter.
  assert(orders.resize(&error, 2));
  assert(expression_builder->push_column(&error, "Float"));
  expression = expression_builder->release(&error);
  assert(expression);
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::REGULAR_ORDER;
  assert(expression_builder->push_column(&error, "_id"));
  expression = expression_builder->release(&error);
  assert(expression);
  orders[1].expression = std::move(expression);
  orders[1].type = grnxx::REGULAR_ORDER;
  sorter = grnxx::Sorter::create(&error, std::move(orders));

  sorter->sort(&error, &records);
  for (grnxx::Int i = 1; i < records.size(); ++i) {
    grnxx::Int lhs_row_id = records.get_row_id(i - 1);
    grnxx::Int rhs_row_id = records.get_row_id(i);
    grnxx::Float lhs_value = test.float_values[lhs_row_id];
    grnxx::Float rhs_value = test.float_values[rhs_row_id];
    assert(LessEqual()(lhs_value, rhs_value));
    if (Equal()(lhs_value, rhs_value)) {
      assert(lhs_row_id < rhs_row_id);
    }
  }
}

void test_text() {
  grnxx::Error error;

  // Create a cursor which reads all the records.
  auto cursor = test.table->create_cursor(&error);
  assert(cursor);
  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) == test.table->num_rows());

  // Create an object for building expressions.
  auto expression_builder =
      grnxx::ExpressionBuilder::create(&error, test.table);
  assert(expression_builder);

  // Create a regular sorter.
  grnxx::Array<grnxx::SortOrder> orders;
  assert(orders.resize(&error, 1));
  assert(expression_builder->push_column(&error, "Text"));
  auto expression = expression_builder->release(&error);
  assert(expression);
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::REGULAR_ORDER;
  auto sorter = grnxx::Sorter::create(&error, std::move(orders));

  sorter->sort(&error, &records);
  for (grnxx::Int i = 1; i < records.size(); ++i) {
    grnxx::Int lhs_row_id = records.get_row_id(i - 1);
    grnxx::Int rhs_row_id = records.get_row_id(i);
    grnxx::Text lhs_value = test.text_values[lhs_row_id];
    grnxx::Text rhs_value = test.text_values[rhs_row_id];
    assert(lhs_value <= rhs_value);
  }

  // Create a reverse sorter.
  assert(orders.resize(&error, 1));
  assert(expression_builder->push_column(&error, "Text"));
  expression = expression_builder->release(&error);
  assert(expression);
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::REVERSE_ORDER;
  sorter = grnxx::Sorter::create(&error, std::move(orders));

  sorter->sort(&error, &records);
  for (grnxx::Int i = 1; i < records.size(); ++i) {
    grnxx::Int lhs_row_id = records.get_row_id(i - 1);
    grnxx::Int rhs_row_id = records.get_row_id(i);
    grnxx::Text lhs_value = test.text_values[lhs_row_id];
    grnxx::Text rhs_value = test.text_values[rhs_row_id];
    assert(lhs_value >= rhs_value);
  }

  // Create a multiple order sorter.
  assert(orders.resize(&error, 2));
  assert(expression_builder->push_column(&error, "Text"));
  expression = expression_builder->release(&error);
  assert(expression);
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::REGULAR_ORDER;
  assert(expression_builder->push_column(&error, "_id"));
  expression = expression_builder->release(&error);
  assert(expression);
  orders[1].expression = std::move(expression);
  orders[1].type = grnxx::REGULAR_ORDER;
  sorter = grnxx::Sorter::create(&error, std::move(orders));

  sorter->sort(&error, &records);
  for (grnxx::Int i = 1; i < records.size(); ++i) {
    grnxx::Int lhs_row_id = records.get_row_id(i - 1);
    grnxx::Int rhs_row_id = records.get_row_id(i);
    grnxx::Text lhs_value = test.text_values[lhs_row_id];
    grnxx::Text rhs_value = test.text_values[rhs_row_id];
    assert(lhs_value <= rhs_value);
    if (lhs_value == rhs_value) {
      assert(lhs_row_id < rhs_row_id);
    }
  }
}

void test_composite() {
  grnxx::Error error;

  // Create a cursor which reads all the records.
  auto cursor = test.table->create_cursor(&error);
  assert(cursor);
  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) == test.table->num_rows());

  // Create an object for building expressions.
  auto expression_builder =
      grnxx::ExpressionBuilder::create(&error, test.table);
  assert(expression_builder);

  // Create a composite sorter.
  grnxx::Array<grnxx::SortOrder> orders;
  assert(orders.resize(&error, 3));
  assert(expression_builder->push_column(&error, "Bool"));
  auto expression = expression_builder->release(&error);
  assert(expression);
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::REGULAR_ORDER;
  assert(expression_builder->push_column(&error, "Int"));
  expression = expression_builder->release(&error);
  assert(expression);
  orders[1].expression = std::move(expression);
  orders[1].type = grnxx::REVERSE_ORDER;
  assert(expression_builder->push_column(&error, "Text"));
  expression = expression_builder->release(&error);
  assert(expression);
  orders[2].expression = std::move(expression);
  orders[2].type = grnxx::REGULAR_ORDER;
  auto sorter = grnxx::Sorter::create(&error, std::move(orders));

  sorter->sort(&error, &records);
  for (grnxx::Int i = 1; i < records.size(); ++i) {
    grnxx::Int lhs_row_id = records.get_row_id(i - 1);
    grnxx::Int rhs_row_id = records.get_row_id(i);
    grnxx::Bool lhs_value = test.bool_values[lhs_row_id];
    grnxx::Bool rhs_value = test.bool_values[rhs_row_id];
    assert(!lhs_value || rhs_value);
    if (lhs_value == rhs_value) {
      grnxx::Int lhs_value = test.int_values[lhs_row_id];
      grnxx::Int rhs_value = test.int_values[rhs_row_id];
      assert(lhs_value >= rhs_value);
      if (lhs_value == rhs_value) {
        grnxx::Text lhs_value = test.text_values[lhs_row_id];
        grnxx::Text rhs_value = test.text_values[rhs_row_id];
        assert(lhs_value <= rhs_value);
      }
    }
  }
}

int main() {
  init_test();
  test_bool();
  test_int();
  test_float();
  test_text();
  test_composite();
  return 0;
}
