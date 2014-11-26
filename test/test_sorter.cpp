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
  std::unique_ptr<grnxx::DB> db;
  grnxx::Table *table;
  grnxx::Array<grnxx::Bool> bool_values;
  grnxx::Array<grnxx::Int> int_values;
  grnxx::Array<grnxx::Float> float_values;
  grnxx::Array<grnxx::Text> text_values;
  grnxx::Array<std::string> text_bodies;
} test;

void init_test() {
  // Create a database with the default options.
  test.db = grnxx::open_db("");

  // Create a table with the default options.
  test.table = test.db->create_table("Table");

  // Create columns for Bool, Int, Float, and Text values.
  auto bool_column = test.table->create_column("Bool", grnxx::BOOL_DATA);
  auto int_column = test.table->create_column("Int", grnxx::INT_DATA);
  auto float_column = test.table->create_column("Float", grnxx::FLOAT_DATA);
  auto text_column = test.table->create_column("Text", grnxx::TEXT_DATA);

  // Generate random values.
  // Bool: true, false, and N/A.
  // Int: [-128, 128) and N/A.
  // Float: [-1.0, 1.0) and N/A.
  // Text: length = [1, 4], byte = ['0', '9'], and N/A.
  constexpr size_t NUM_ROWS = 1 << 16;
  std::mt19937_64 mersenne_twister;
  test.bool_values.resize(NUM_ROWS);
  test.int_values.resize(NUM_ROWS);
  test.float_values.resize(NUM_ROWS);
  test.text_values.resize(NUM_ROWS);
  test.text_bodies.resize(NUM_ROWS);
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

    source = mersenne_twister() % 257;
    if (source == 256) {
      test.int_values[i] = grnxx::Int::na();
    } else {
      test.int_values[i] =
          grnxx::Int(static_cast<int64_t>(source) - 128);
    }

    source = mersenne_twister() % 257;
    if (source == 256) {
      test.float_values[i] = grnxx::Float::na();
    } else {
      test.float_values[i] =
          grnxx::Float((static_cast<int64_t>(source) - 128) / 128.0);
    }

    source = mersenne_twister() % 5;
    if (source == 0) {
      test.text_values[i] = grnxx::Text::na();
    } else {
      size_t size = source - 1;
      test.text_bodies[i].resize(size);
      for (size_t j = 0; j < size; ++j) {
        test.text_bodies[i][j] = '0' + (mersenne_twister() % 10);
      }
      test.text_values[i] = grnxx::Text(test.text_bodies[i].data(), size);
    }
  }

  // Store generated values into columns.
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = test.table->insert_row();
    bool_column->set(row_id, test.bool_values[i]);
    int_column->set(row_id, test.int_values[i]);
    float_column->set(row_id, test.float_values[i]);
    text_column->set(row_id, test.text_values[i]);
  }
}

grnxx::Array<grnxx::Record> create_input_records() {
  auto cursor = test.table->create_cursor();

  grnxx::Array<grnxx::Record> records;
  size_t count = cursor->read_all(&records);
  assert(count == test.table->num_rows());

  return records;
}

void test_row_id() {
  // Create a cursor which reads all the records.
  auto records = create_input_records();

  // Create an object for building expressions.
  auto expression_builder = grnxx::ExpressionBuilder::create(test.table);

  // Create a reverse sorter.
  grnxx::Array<grnxx::SorterOrder> orders;
  orders.resize(1);
  expression_builder->push_row_id();
  auto expression = expression_builder->release();
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::SORTER_REVERSE_ORDER;
  auto sorter = grnxx::Sorter::create(std::move(orders));

  sorter->sort(&records);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    assert(lhs_row_id > rhs_row_id);
  }

  // Create a regular sorter.
  orders.resize(1);
  expression_builder->push_row_id();
  expression = expression_builder->release();
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  sorter = grnxx::Sorter::create(std::move(orders));

  sorter->sort(&records);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    assert(lhs_row_id < rhs_row_id);
  }
}

void test_score() {
  // Create a cursor which reads all the records.
  auto records = create_input_records();

  // Create an object for building expressions.
  auto expression_builder = grnxx::ExpressionBuilder::create(test.table);

  expression_builder->push_column("Float");
  auto expression = expression_builder->release();
  expression->adjust(&records);

  // Create a regular sorter.
  grnxx::Array<grnxx::SorterOrder> orders;
  orders.resize(1);
  expression_builder->push_score();
  expression = expression_builder->release();
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  auto sorter = grnxx::Sorter::create(std::move(orders));

  sorter->sort(&records);
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    grnxx::Float value = test.float_values[row_id];
    assert(records[i].score.match(value));
  }
  for (size_t i = 1; i < records.size(); ++i) {
    grnxx::Float lhs_score = records[i - 1].score;
    grnxx::Float rhs_score = records[i].score;
    if (lhs_score.is_na()) {
      assert(rhs_score.is_na());
    } else {
      assert(rhs_score.is_na() || (lhs_score <= rhs_score).is_true());
    }
  }

  // Create a reverse sorter.
  orders.resize(1);
  expression_builder->push_score();
  expression = expression_builder->release();
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::SORTER_REVERSE_ORDER;
  sorter = grnxx::Sorter::create(std::move(orders));

  sorter->sort(&records);
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    grnxx::Float value = test.float_values[row_id];
    assert(records[i].score.match(value));
  }
  for (size_t i = 1; i < records.size(); ++i) {
    grnxx::Float lhs_score = records[i - 1].score;
    grnxx::Float rhs_score = records[i].score;
    if (lhs_score.is_na()) {
      assert(rhs_score.is_na());
    } else {
      assert(rhs_score.is_na() || (lhs_score >= rhs_score).is_true());
    }
  }

  // Create a multiple order sorter.
  orders.resize(2);
  expression_builder->push_score();
  expression = expression_builder->release();
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  expression_builder->push_row_id();
  expression = expression_builder->release();
  orders[1].expression = std::move(expression);
  orders[1].type = grnxx::SORTER_REGULAR_ORDER;
  sorter = grnxx::Sorter::create(std::move(orders));

  sorter->sort(&records);
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    grnxx::Float value = test.float_values[row_id];
    assert(records[i].score.match(value));
  }
  for (size_t i = 1; i < records.size(); ++i) {
    grnxx::Float lhs_score = records[i - 1].score;
    grnxx::Float rhs_score = records[i].score;
    if (lhs_score.is_na()) {
      assert(rhs_score.is_na());
    } else {
      assert(rhs_score.is_na() || (lhs_score <= rhs_score).is_true());
    }
    if (lhs_score.match(rhs_score)) {
      assert(records[i - 1].row_id.raw() < records[i].row_id.raw());
    }
  }
}

void test_bool() {
  // Create a cursor which reads all the records.
  auto records = create_input_records();

  // Create an object for building expressions.
  auto expression_builder = grnxx::ExpressionBuilder::create(test.table);

  // Create a regular sorter.
  grnxx::Array<grnxx::SorterOrder> orders;
  orders.resize(1);
  expression_builder->push_column("Bool");
  auto expression = expression_builder->release();
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  auto sorter = grnxx::Sorter::create(std::move(orders));

  sorter->sort(&records);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    grnxx::Bool lhs_value = test.bool_values[lhs_row_id];
    grnxx::Bool rhs_value = test.bool_values[rhs_row_id];
    if (lhs_value.is_true()) {
      assert(rhs_value.is_true() || rhs_value.is_na());
    } else if (lhs_value.is_na()) {
      assert(rhs_value.is_na());
    }
  }

  // Create a reverse sorter.
  orders.resize(1);
  expression_builder->push_column("Bool");
  expression = expression_builder->release();
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::SORTER_REVERSE_ORDER;
  sorter = grnxx::Sorter::create(std::move(orders));

  sorter->sort(&records);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    grnxx::Bool lhs_value = test.bool_values[lhs_row_id];
    grnxx::Bool rhs_value = test.bool_values[rhs_row_id];
    if (lhs_value.is_false()) {
      assert(rhs_value.is_false() || rhs_value.is_na());
    } else if (lhs_value.is_na()) {
      assert(rhs_value.is_na());
    }
  }

  // Create a multiple order sorter.
  orders.resize(2);
  expression_builder->push_column("Bool");
  expression = expression_builder->release();
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  expression_builder->push_row_id();
  expression = expression_builder->release();
  orders[1].expression = std::move(expression);
  orders[1].type = grnxx::SORTER_REGULAR_ORDER;
  sorter = grnxx::Sorter::create(std::move(orders));

  sorter->sort(&records);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    grnxx::Bool lhs_value = test.bool_values[lhs_row_id];
    grnxx::Bool rhs_value = test.bool_values[rhs_row_id];
    if (lhs_value.is_true()) {
      assert(rhs_value.is_true() || rhs_value.is_na());
    } else if (lhs_value.is_na()) {
      assert(rhs_value.is_na());
    }
    if (lhs_value.match(rhs_value)) {
      assert(lhs_row_id < rhs_row_id);
    }
  }
}

void test_int() {
  // Create a cursor which reads all the records.
  auto records = create_input_records();

  // Create an object for building expressions.
  auto expression_builder = grnxx::ExpressionBuilder::create(test.table);

  // Create a regular sorter.
  grnxx::Array<grnxx::SorterOrder> orders;
  orders.resize(1);
  expression_builder->push_column("Int");
  auto expression = expression_builder->release();
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  auto sorter = grnxx::Sorter::create(std::move(orders));

  sorter->sort(&records);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    grnxx::Int lhs_value = test.int_values[lhs_row_id];
    grnxx::Int rhs_value = test.int_values[rhs_row_id];
    if (lhs_value.is_na()) {
      assert(rhs_value.is_na());
    } else {
      assert(rhs_value.is_na() || (lhs_value <= rhs_value).is_true());
    }
  }

  // Create a reverse sorter.
  orders.resize(1);
  expression_builder->push_column("Int");
  expression = expression_builder->release();
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::SORTER_REVERSE_ORDER;
  sorter = grnxx::Sorter::create(std::move(orders));

  sorter->sort(&records);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    grnxx::Int lhs_value = test.int_values[lhs_row_id];
    grnxx::Int rhs_value = test.int_values[rhs_row_id];
    if (lhs_value.is_na()) {
      assert(rhs_value.is_na());
    } else {
      assert(rhs_value.is_na() || (lhs_value >= rhs_value).is_true());
    }
  }

  // Create a multiple order sorter.
  orders.resize(2);
  expression_builder->push_column("Int");
  expression = expression_builder->release();
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  expression_builder->push_row_id();
  expression = expression_builder->release();
  orders[1].expression = std::move(expression);
  orders[1].type = grnxx::SORTER_REGULAR_ORDER;
  sorter = grnxx::Sorter::create(std::move(orders));

  sorter->sort(&records);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    grnxx::Int lhs_value = test.int_values[lhs_row_id];
    grnxx::Int rhs_value = test.int_values[rhs_row_id];
    if (lhs_value.is_na()) {
      assert(rhs_value.is_na());
    } else {
      assert(rhs_value.is_na() || (lhs_value <= rhs_value).is_true());
    }
    if (lhs_value.match(rhs_value)) {
      assert(lhs_row_id < rhs_row_id);
    }
  }
}

void test_float() {
  // Create a cursor which reads all the records.
  auto records = create_input_records();

  // Create an object for building expressions.
  auto expression_builder = grnxx::ExpressionBuilder::create(test.table);

  // Create a regular sorter.
  grnxx::Array<grnxx::SorterOrder> orders;
  orders.resize(1);
  expression_builder->push_column("Float");
  auto expression = expression_builder->release();
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  auto sorter = grnxx::Sorter::create(std::move(orders));

  sorter->sort(&records);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    grnxx::Float lhs_value = test.float_values[lhs_row_id];
    grnxx::Float rhs_value = test.float_values[rhs_row_id];
    if (lhs_value.is_na()) {
      assert(rhs_value.is_na());
    } else {
      assert(rhs_value.is_na() || (lhs_value <= rhs_value).is_true());
    }
  }

  // Create a reverse sorter.
  orders.resize(1);
  expression_builder->push_column("Float");
  expression = expression_builder->release();
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::SORTER_REVERSE_ORDER;
  sorter = grnxx::Sorter::create(std::move(orders));

  sorter->sort(&records);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    grnxx::Float lhs_value = test.float_values[lhs_row_id];
    grnxx::Float rhs_value = test.float_values[rhs_row_id];
    if (lhs_value.is_na()) {
      assert(rhs_value.is_na());
    } else {
      assert(rhs_value.is_na() || (lhs_value >= rhs_value).is_true());
    }
  }

  // Create a multiple order sorter.
  orders.resize(2);
  expression_builder->push_column("Float");
  expression = expression_builder->release();
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  expression_builder->push_row_id();
  expression = expression_builder->release();
  orders[1].expression = std::move(expression);
  orders[1].type = grnxx::SORTER_REGULAR_ORDER;
  sorter = grnxx::Sorter::create(std::move(orders));

  sorter->sort(&records);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    grnxx::Float lhs_value = test.float_values[lhs_row_id];
    grnxx::Float rhs_value = test.float_values[rhs_row_id];
    if (lhs_value.is_na()) {
      assert(rhs_value.is_na());
    } else {
      assert(rhs_value.is_na() || (lhs_value <= rhs_value).is_true());
    }
    if (lhs_value.match(rhs_value)) {
      assert(lhs_row_id < rhs_row_id);
    }
  }
}

void test_text() {
  // Create a cursor which reads all the records.
  auto records = create_input_records();

  // Create an object for building expressions.
  auto expression_builder = grnxx::ExpressionBuilder::create(test.table);

  // Create a regular sorter.
  grnxx::Array<grnxx::SorterOrder> orders;
  orders.resize(1);
  expression_builder->push_column("Text");
  auto expression = expression_builder->release();
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  auto sorter = grnxx::Sorter::create(std::move(orders));

  sorter->sort(&records);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    grnxx::Text lhs_value = test.text_values[lhs_row_id];
    grnxx::Text rhs_value = test.text_values[rhs_row_id];
    if (lhs_value.is_na()) {
      assert(rhs_value.is_na());
    } else {
      assert(rhs_value.is_na() || (lhs_value <= rhs_value).is_true());
    }
  }

  // Create a reverse sorter.
  orders.resize(1);
  expression_builder->push_column("Text");
  expression = expression_builder->release();
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::SORTER_REVERSE_ORDER;
  sorter = grnxx::Sorter::create(std::move(orders));

  sorter->sort(&records);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    grnxx::Text lhs_value = test.text_values[lhs_row_id];
    grnxx::Text rhs_value = test.text_values[rhs_row_id];
    if (lhs_value.is_na()) {
      assert(rhs_value.is_na());
    } else {
      assert(rhs_value.is_na() || (lhs_value >= rhs_value).is_true());
    }
  }

  // Create a multiple order sorter.
  orders.resize(2);
  expression_builder->push_column("Text");
  expression = expression_builder->release();
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  expression_builder->push_row_id();
  expression = expression_builder->release();
  orders[1].expression = std::move(expression);
  orders[1].type = grnxx::SORTER_REGULAR_ORDER;
  sorter = grnxx::Sorter::create(std::move(orders));

  sorter->sort(&records);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    grnxx::Text lhs_value = test.text_values[lhs_row_id];
    grnxx::Text rhs_value = test.text_values[rhs_row_id];
    if (lhs_value.is_na()) {
      assert(rhs_value.is_na());
    } else {
      assert(rhs_value.is_na() || (lhs_value <= rhs_value).is_true());
    }
    if (lhs_value.match(rhs_value)) {
      assert(lhs_row_id < rhs_row_id);
    }
  }
}

void test_composite() {
  // Create a cursor which reads all the records.
  auto records = create_input_records();

  // Create an object for building expressions.
  auto expression_builder = grnxx::ExpressionBuilder::create(test.table);

  // Create a composite sorter.
  grnxx::Array<grnxx::SorterOrder> orders;
  orders.resize(3);
  expression_builder->push_column("Bool");
  auto expression = expression_builder->release();
  assert(expression);
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  expression_builder->push_column("Int");
  expression = expression_builder->release();
  orders[1].expression = std::move(expression);
  orders[1].type = grnxx::SORTER_REVERSE_ORDER;
  expression_builder->push_column("Text");
  expression = expression_builder->release();
  orders[2].expression = std::move(expression);
  orders[2].type = grnxx::SORTER_REGULAR_ORDER;
  auto sorter = grnxx::Sorter::create(std::move(orders));

  sorter->sort(&records);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    grnxx::Bool lhs_value = test.bool_values[lhs_row_id];
    grnxx::Bool rhs_value = test.bool_values[rhs_row_id];
    if (lhs_value.is_true()) {
      assert(rhs_value.is_true() || rhs_value.is_na());
    } else if (lhs_value.is_na()) {
      assert(rhs_value.is_na());
    }
    if (lhs_value.match(rhs_value)) {
      grnxx::Int lhs_value = test.int_values[lhs_row_id];
      grnxx::Int rhs_value = test.int_values[rhs_row_id];
      if (lhs_value.is_na()) {
        assert(rhs_value.is_na());
      } else {
        assert(rhs_value.is_na() || (lhs_value >= rhs_value).is_true());
      }
      if (lhs_value.match(rhs_value)) {
        grnxx::Text lhs_value = test.text_values[lhs_row_id];
        grnxx::Text rhs_value = test.text_values[rhs_row_id];
        if (lhs_value.is_na()) {
          assert(rhs_value.is_na());
        } else {
          assert(rhs_value.is_na() || (lhs_value <= rhs_value).is_true());
        }
      }
    }
  }
}

int main() {
  init_test();
  test_row_id();
  test_score();
  test_bool();
  test_int();
  test_float();
  test_text();
  test_composite();
  return 0;
}
