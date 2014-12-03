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

#include "grnxx/column.hpp"
#include "grnxx/db.hpp"
#include "grnxx/table.hpp"

template <typename T>
void test_column(const T &value) {
  constexpr grnxx::DataType data_type = T::type();

  // Create a table and insert the first row.
  auto db = grnxx::open_db("");
  auto table = db->create_table("Table");
  grnxx::Int row_id = table->insert_row();

  // Create a column named "Column".
  auto column = table->create_column("Column", data_type);
  assert(column->table() == table);
  assert(column->name() == "Column");
  assert(column->data_type() == data_type);
  assert(!column->reference_table());
  assert(!column->is_key());
  assert(column->num_indexes() == 0);

  // Check if N/A is stored or not.
  grnxx::Datum datum;
  T stored_value;
  column->get(row_id, &datum);
  assert(datum.type() == data_type);
  datum.force(&stored_value);
  assert(stored_value.is_na());

  // Set a value and get it.
  column->set(row_id, value);
  column->get(row_id, &datum);
  assert(datum.type() == data_type);
  datum.force(&stored_value);
  assert(stored_value.match(value));
}

int main() {
  test_column(grnxx::Bool(true));
  test_column(grnxx::Int(123));
  test_column(grnxx::Float(1.25));
  test_column(grnxx::GeoPoint(grnxx::Int(123), grnxx::Int(456)));
  test_column(grnxx::GeoPoint(grnxx::Float(1.25), grnxx::Float(-1.25)));
  test_column(grnxx::Text("ABC"));

  grnxx::Bool bool_values[] = {
    grnxx::Bool(true),
    grnxx::Bool(false),
    grnxx::Bool(true)
  };
  grnxx::BoolVector bool_vector(bool_values, 3);
  test_column(bool_vector);

  grnxx::Int int_values[] = {
    grnxx::Int(123),
    grnxx::Int(-456),
    grnxx::Int(789)
  };
  grnxx::IntVector int_vector(int_values, 3);
  test_column(int_vector);

  grnxx::Float float_values[] = {
    grnxx::Float(1.23),
    grnxx::Float(-4.56),
    grnxx::Float(7.89)
  };
  grnxx::FloatVector float_vector(float_values, 3);
  test_column(float_vector);

  grnxx::GeoPoint geo_point_values[] = {
    { grnxx::Float(43.068661), grnxx::Float(141.350755) },  // Sapporo.
    { grnxx::Float(35.681382), grnxx::Float(139.766084) },  // Tokyo.
    { grnxx::Float(34.702485), grnxx::Float(135.495951) },  // Osaka.
  };
  grnxx::GeoPointVector geo_point_vector(geo_point_values, 3);
  test_column(geo_point_vector);

  grnxx::Text text_values[] = {
    grnxx::Text("abc"),
    grnxx::Text("DEF"),
    grnxx::Text("ghi")
  };
  grnxx::TextVector text_vector(text_values, 3);
  test_column(text_vector);

  return 0;
}
