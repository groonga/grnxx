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

std::mt19937_64 mersenne_twister;

void test_scored_subexpression() {
  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table with the default options.
  auto table = db->create_table(&error, "Table");
  assert(table);

  constexpr grnxx::Int NUM_ROWS = 1 << 16;

  // Generate random values.
  grnxx::Array<grnxx::Float> float_values;
  grnxx::Array<grnxx::Int> ref_values;
  assert(float_values.resize(&error, NUM_ROWS + 1));
  assert(ref_values.resize(&error, NUM_ROWS + 1));
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    float_values[i] = 1.0 * mersenne_twister() / mersenne_twister.max();
//    ref_values[i] = (mersenne_twister() % NUM_ROWS) + 1;
    ref_values[i] = 1;
  }

  // Create columns for Float and Int values.
  auto float_column = table->create_column(&error, "Float", grnxx::FLOAT_DATA);
  assert(float_column);
  grnxx::ColumnOptions options;
  options.ref_table_name = "Table";
  auto ref_column = table->create_column(&error, "Ref", grnxx::INT_DATA,
                                         options);
  assert(ref_column);

  // Store generated values into columns.
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    assert(row_id == i);
    assert(float_column->set(&error, row_id, float_values[i]));
  }
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    assert(ref_column->set(&error, i, ref_values[i]));
  }

  // Generate a list of records.
  grnxx::Array<grnxx::Record> records;
  auto cursor = table->create_cursor(&error);
  assert(cursor);
  auto result = cursor->read_all(&error, &records);
  assert(result.is_ok);
  assert(result.count == table->num_rows());

  // Set scores (Float).
  auto builder = grnxx::ExpressionBuilder::create(&error, table);
  assert(builder);
  assert(builder->push_column(&error, "Float"));
  auto expression = builder->release(&error);
  assert(expression);
  assert(expression->adjust(&error, &records));

  // Test an expression (Ref.(_score > 0.5)).
  assert(builder->push_column(&error, "Ref"));
  assert(builder->begin_subexpression(&error));
  assert(builder->push_score(&error));
  assert(builder->push_constant(&error, grnxx::Float(0.5)));
  assert(builder->push_operator(&error, grnxx::GREATER_OPERATOR));
  assert(builder->end_subexpression(&error));
  expression = builder->release(&error);
  assert(expression);

  assert(expression->filter(&error, &records));
  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    if (float_values[i] > 0.5) {
      assert(records.get_row_id(count) == i);
      ++count;
    }
  }
  assert(records.size() == count);
}

int main() {
  // Test a subexpression using scores.
  test_scored_subexpression();

  return 0;
}
