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

std::mt19937_64 rng;

void test_scored_subexpression() {
  // Create a database with the default options.
  auto db = grnxx::open_db("");

  // Create a table with the default options.
  auto table = db->create_table("Table");

  constexpr size_t NUM_ROWS = 1 << 16;

  // Generate random values.
  grnxx::Array<grnxx::Float> float_values;
  grnxx::Array<grnxx::Int> ref_values;
  float_values.resize(NUM_ROWS);
  ref_values.resize(NUM_ROWS);
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    float_values[i] = grnxx::Float(1.0 * rng() / rng.max());
//    ref_values[i] = mersenne_twister() % NUM_ROWS;
    ref_values[i] = grnxx::Int(0);
  }

  // Create columns for Float and Int values.
  auto float_column = table->create_column("Float", GRNXX_FLOAT);
  grnxx::ColumnOptions options;
  options.reference_table_name = "Table";
  auto ref_column = table->create_column("Ref", GRNXX_INT, options);

  // Store generated values into columns.
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = table->insert_row();
    assert(row_id.match(grnxx::Int(i)));
    float_column->set(row_id, float_values[i]);
  }
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    ref_column->set(grnxx::Int(i), ref_values[i]);
  }

  // Generate a list of records.
  grnxx::Array<grnxx::Record> records;
  auto cursor = table->create_cursor();
  assert(cursor->read_all(&records) == table->num_rows());

  // Set scores (Float).
  auto builder = grnxx::ExpressionBuilder::create(table);
  builder->push_column("Float");
  auto expression = builder->release();
  expression->adjust(&records);

  // Test an expression (Ref.(_score > 0.5)).
  builder->push_column("Ref");
  builder->begin_subexpression();
  builder->push_score();
  builder->push_constant(grnxx::Float(0.5));
  builder->push_operator(GRNXX_GREATER);
  builder->end_subexpression();
  expression = builder->release();

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    if (float_values[i].raw() > 0.5) {
      assert(records[count].row_id.match(grnxx::Int(i)));
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
