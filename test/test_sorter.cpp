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
#include "grnxx/sorter.hpp"

void test_sorter() {
  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table with the default options.
  auto table = db->create_table(&error, "Table");
  assert(table);

  // Create a Bool column named "BoolColumn".
  auto bool_column = table->create_column(&error, "BoolColumn",
                                          grnxx::BOOL_DATA);
  assert(bool_column);

  // Create a Int column named "IntColumn".
  auto int_column = table->create_column(&error, "IntColumn",
                                         grnxx::INT_DATA);
  assert(int_column);

  // Generate 1,024 random integers in a range [0, 64).
  constexpr size_t NUM_VALUES = 1024;
  std::vector<grnxx::Bool> bool_values(NUM_VALUES);
  std::vector<grnxx::Int> int_values(NUM_VALUES);
  std::mt19937_64 mersenne_twister;
  for (size_t i = 0; i < bool_values.size(); ++i) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    bool_values[i] = (mersenne_twister() & 1) != 0;
    int_values[i] = mersenne_twister() % 64;
    assert(bool_column->set(&error, row_id, grnxx::Bool(bool_values[i])));
    assert(int_column->set(&error, row_id, int_values[i]));
  }

  grnxx::Array<grnxx::Record> records;
  auto cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) ==
         static_cast<grnxx::Int>(int_values.size()));
  assert(records.size() == static_cast<grnxx::Int>(int_values.size()));

  // Sort records in order of BoolColumn value and row ID.
  grnxx::Array<grnxx::SortOrder> orders;
  assert(orders.resize(&error, 2));
  auto expression_builder =
      grnxx::ExpressionBuilder::create(&error, table);
  assert(expression_builder->push_column(&error, "BoolColumn"));
  auto expression = expression_builder->release(&error);
  assert(expression);
  orders[0].expression = std::move(expression);

  assert(expression_builder->push_column(&error, "_id"));
  expression = expression_builder->release(&error);
  assert(expression);
  orders[1].expression = std::move(expression);

  auto sorter = grnxx::Sorter::create(&error, std::move(orders));
  assert(sorter);

  assert(sorter->sort(&error, &records));
  assert(records.size() == static_cast<grnxx::Int>(int_values.size()));

  for (grnxx::Int i = 1; i < records.size(); ++i) {
    grnxx::Int lhs_id = records.get_row_id(i - 1) - 1;
    grnxx::Int rhs_id = records.get_row_id(i) - 1;
    grnxx::Bool lhs_value = bool_values[lhs_id];
    grnxx::Bool rhs_value = bool_values[rhs_id];
    assert(!lhs_value || rhs_value);
    if (lhs_value == rhs_value) {
      assert(lhs_id < rhs_id);
    }
  }

  // Sort records in reverse order of BoolColumn value and row ID.
  assert(orders.resize(&error, 2));
  expression_builder = grnxx::ExpressionBuilder::create(&error, table);
  assert(expression_builder->push_column(&error, "BoolColumn"));
  expression = expression_builder->release(&error);
  assert(expression);
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::REVERSE_ORDER;

  assert(expression_builder->push_column(&error, "_id"));
  expression = expression_builder->release(&error);
  assert(expression);
  orders[1].expression = std::move(expression);
  orders[1].type = grnxx::REVERSE_ORDER;

  sorter = grnxx::Sorter::create(&error, std::move(orders));
  assert(sorter);

  assert(sorter->sort(&error, &records));
  assert(records.size() == static_cast<grnxx::Int>(int_values.size()));

  for (grnxx::Int i = 1; i < records.size(); ++i) {
    grnxx::Int lhs_id = records.get_row_id(i - 1) - 1;
    grnxx::Int rhs_id = records.get_row_id(i) - 1;
    grnxx::Bool lhs_value = bool_values[lhs_id];
    grnxx::Bool rhs_value = bool_values[rhs_id];
    assert(lhs_value || !rhs_value);
    if (lhs_value == rhs_value) {
      assert(lhs_id > rhs_id);
    }
  }

  // Sort records in order of IntColumn value and row ID.
  assert(orders.resize(&error, 2));
  expression_builder = grnxx::ExpressionBuilder::create(&error, table);
  assert(expression_builder->push_column(&error, "IntColumn"));
  expression = expression_builder->release(&error);
  orders[0].expression = std::move(expression);

  assert(expression_builder->push_column(&error, "_id"));
  expression = expression_builder->release(&error);
  assert(expression);
  orders[1].expression = std::move(expression);

  sorter = grnxx::Sorter::create(&error, std::move(orders));
  assert(sorter);

  assert(sorter->sort(&error, &records));
  assert(records.size() == static_cast<grnxx::Int>(int_values.size()));

  for (grnxx::Int i = 1; i < records.size(); ++i) {
    grnxx::Int lhs_id = records.get_row_id(i - 1) - 1;
    grnxx::Int rhs_id = records.get_row_id(i) - 1;
    grnxx::Int lhs_value = int_values[lhs_id];
    grnxx::Int rhs_value = int_values[rhs_id];
    assert(lhs_value <= rhs_value);
    if (lhs_value == rhs_value) {
      assert(lhs_id < rhs_id);
    }
  }

  // Sort records in reverse order of IntColumn value and row ID.
  assert(orders.resize(&error, 2));
  assert(expression_builder->push_column(&error, "IntColumn"));
  expression = expression_builder->release(&error);
  assert(expression);
  orders[0].expression = std::move(expression);
  orders[0].type = grnxx::REVERSE_ORDER;

  assert(expression_builder->push_column(&error, "_id"));
  expression = expression_builder->release(&error);
  assert(expression);
  orders[1].expression = std::move(expression);
  orders[1].type = grnxx::REVERSE_ORDER;

  sorter = grnxx::Sorter::create(&error, std::move(orders));
  assert(sorter);

  assert(sorter->sort(&error, &records));
  assert(records.size() == static_cast<grnxx::Int>(int_values.size()));

  for (grnxx::Int i = 1; i < records.size(); ++i) {
    grnxx::Int lhs_id = records.get_row_id(i - 1) - 1;
    grnxx::Int rhs_id = records.get_row_id(i) - 1;
    grnxx::Int lhs_value = int_values[lhs_id];
    grnxx::Int rhs_value = int_values[rhs_id];
    assert(lhs_value >= rhs_value);
    if (lhs_value == rhs_value) {
      assert(lhs_id > rhs_id);
    }
  }
}

int main() {
  test_sorter();
  return 0;
}
