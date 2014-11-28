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

void test_column() {
  // Create a database with the default options.
  auto db = grnxx::open_db("");

  // Create a table with the default options.
  auto table = db->create_table("Table");

  // Append the first row.
  grnxx::Int row_id = table->insert_row();

  // Create a column named "Bool".
  auto bool_column = table->create_column("Bool", grnxx::BOOL_DATA);
  assert(bool_column->table() == table);
  assert(bool_column->name() == "Bool");
  assert(bool_column->data_type() == grnxx::BOOL_DATA);
  assert(!bool_column->reference_table());
  assert(!bool_column->is_key());
  assert(bool_column->num_indexes() == 0);

  // Create a column named "Int".
  auto int_column = table->create_column("Int", grnxx::INT_DATA);
  assert(int_column->table() == table);
  assert(int_column->name() == "Int");
  assert(int_column->data_type() == grnxx::INT_DATA);
  assert(!int_column->reference_table());
  assert(!int_column->is_key());
  assert(int_column->num_indexes() == 0);

  // Create a column named "Float".
  auto float_column = table->create_column("Float", grnxx::FLOAT_DATA);
  assert(float_column->table() == table);
  assert(float_column->name() == "Float");
  assert(float_column->data_type() == grnxx::FLOAT_DATA);
  assert(!float_column->reference_table());
  assert(!float_column->is_key());
  assert(float_column->num_indexes() == 0);

  // Create a column named "GeoPoint".
  auto geo_point_column =
      table->create_column("GeoPoint", grnxx::GEO_POINT_DATA);
  assert(geo_point_column->table() == table);
  assert(geo_point_column->name() == "GeoPoint");
  assert(geo_point_column->data_type() == grnxx::GEO_POINT_DATA);
  assert(!geo_point_column->reference_table());
  assert(!geo_point_column->is_key());
  assert(geo_point_column->num_indexes() == 0);

//  // Create a column named "Text".
  auto text_column = table->create_column("Text", grnxx::TEXT_DATA);
  assert(text_column->table() == table);
  assert(text_column->name() == "Text");
  assert(text_column->data_type() == grnxx::TEXT_DATA);
  assert(!text_column->reference_table());
  assert(!text_column->is_key());
  assert(text_column->num_indexes() == 0);

  // Create a column named "Reference".
  grnxx::ColumnOptions options;
  options.reference_table_name = "Table";
  auto reference_column =
      table->create_column("Reference", grnxx::INT_DATA, options);
  assert(reference_column->table() == table);
  assert(reference_column->name() == "Reference");
  assert(reference_column->data_type() == grnxx::INT_DATA);
  assert(reference_column->reference_table() == table);
  assert(!reference_column->is_key());
  assert(int_column->num_indexes() == 0);

  // Create a column named "BoolVector".
  auto bool_vector_column =
      table->create_column("BoolVector", grnxx::BOOL_VECTOR_DATA);
  assert(bool_vector_column->table() == table);
  assert(bool_vector_column->name() == "BoolVector");
  assert(bool_vector_column->data_type() == grnxx::BOOL_VECTOR_DATA);
  assert(!bool_vector_column->reference_table());
  assert(!bool_vector_column->is_key());
  assert(bool_vector_column->num_indexes() == 0);

  // Create a column named "IntVector".
  auto int_vector_column =
      table->create_column("IntVector", grnxx::INT_VECTOR_DATA);
  assert(int_vector_column->table() == table);
  assert(int_vector_column->name() == "IntVector");
  assert(int_vector_column->data_type() == grnxx::INT_VECTOR_DATA);
  assert(!int_vector_column->reference_table());
  assert(!int_vector_column->is_key());
  assert(int_vector_column->num_indexes() == 0);

  // Create a column named "FloatVector".
  auto float_vector_column =
      table->create_column("FloatVector", grnxx::FLOAT_VECTOR_DATA);
  assert(float_vector_column->table() == table);
  assert(float_vector_column->name() == "FloatVector");
  assert(float_vector_column->data_type() == grnxx::FLOAT_VECTOR_DATA);
  assert(!float_vector_column->reference_table());
  assert(!float_vector_column->is_key());
  assert(float_vector_column->num_indexes() == 0);

  // Create a column named "GeoPointVector".
  auto geo_point_vector_column =
      table->create_column("GeoPointVector", grnxx::GEO_POINT_VECTOR_DATA);
  assert(geo_point_vector_column->table() == table);
  assert(geo_point_vector_column->name() == "GeoPointVector");
  assert(geo_point_vector_column->data_type() == grnxx::GEO_POINT_VECTOR_DATA);
  assert(!geo_point_vector_column->reference_table());
  assert(!geo_point_vector_column->is_key());
  assert(geo_point_vector_column->num_indexes() == 0);

  // Create a column named "TextVector".
  auto text_vector_column =
      table->create_column("TextVector", grnxx::TEXT_VECTOR_DATA);
  assert(text_vector_column->table() == table);
  assert(text_vector_column->name() == "TextVector");
  assert(text_vector_column->data_type() == grnxx::TEXT_VECTOR_DATA);
  assert(!text_vector_column->reference_table());
  assert(!text_vector_column->is_key());
  assert(text_vector_column->num_indexes() == 0);

  // Create a column named "ReferenceVector".
  options.reference_table_name = "Table";
  auto reference_vector_column =
      table->create_column("ReferenceVector", grnxx::INT_VECTOR_DATA, options);
  assert(reference_vector_column->table() == table);
  assert(reference_vector_column->name() == "ReferenceVector");
  assert(reference_vector_column->data_type() == grnxx::INT_VECTOR_DATA);
  assert(reference_vector_column->reference_table() == table);
  assert(!reference_vector_column->is_key());
  assert(reference_vector_column->num_indexes() == 0);

  // Check that the default values are stored.
  grnxx::Datum datum;

  bool_column->get(row_id, &datum);
  assert(datum.type() == grnxx::BOOL_DATA);
  assert(datum.as_bool().is_na());

  int_column->get(row_id, &datum);
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.as_int().is_na());

  float_column->get(row_id, &datum);
  assert(datum.type() == grnxx::FLOAT_DATA);
  assert(datum.as_float().is_na());

  geo_point_column->get(row_id, &datum);
  assert(datum.type() == grnxx::GEO_POINT_DATA);
  assert(datum.as_geo_point().is_na());

  text_column->get(row_id, &datum);
  assert(datum.type() == grnxx::TEXT_DATA);
  assert(datum.as_text().is_na());

  reference_column->get(row_id, &datum);
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.as_int().is_na());

  bool_vector_column->get(row_id, &datum);
  assert(datum.type() == grnxx::BOOL_VECTOR_DATA);
  assert(datum.as_bool_vector().is_na());

  int_vector_column->get(row_id, &datum);
  assert(datum.type() == grnxx::INT_VECTOR_DATA);
  assert(datum.as_int_vector().is_na());

  float_vector_column->get(row_id, &datum);
  assert(datum.type() == grnxx::FLOAT_VECTOR_DATA);
  assert(datum.as_float_vector().is_na());

  geo_point_vector_column->get(row_id, &datum);
  assert(datum.type() == grnxx::GEO_POINT_VECTOR_DATA);
  assert(datum.as_geo_point_vector().is_na());

  text_vector_column->get(row_id, &datum);
  assert(datum.type() == grnxx::TEXT_VECTOR_DATA);
  assert(datum.as_text_vector().is_na());

  reference_vector_column->get(row_id, &datum);
  assert(datum.type() == grnxx::INT_VECTOR_DATA);
  assert(datum.as_int_vector().is_na());

  // Set and get values.
  bool_column->set(row_id, grnxx::Bool(true));
  bool_column->get(row_id, &datum);
  assert(datum.type() == grnxx::BOOL_DATA);
  assert(datum.as_bool().is_true());

  int_column->set(row_id, grnxx::Int(123));
  int_column->get(row_id, &datum);
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.as_int().raw() == 123);

  float_column->set(row_id, grnxx::Float(1.25));
  float_column->get(row_id, &datum);
  assert(datum.type() == grnxx::FLOAT_DATA);
  assert(datum.as_float().raw() == 1.25);

  grnxx::GeoPoint geo_point(grnxx::Int(123), grnxx::Int(456));
  geo_point_column->set(row_id, geo_point);
  geo_point_column->get(row_id, &datum);
  assert(datum.type() == grnxx::GEO_POINT_DATA);
  assert(datum.as_geo_point().raw_latitude() == 123);
  assert(datum.as_geo_point().raw_longitude() == 456);

  grnxx::Text text(grnxx::Text("ABC"));
  text_column->set(row_id, text);
  text_column->get(row_id, &datum);
  assert(datum.type() == grnxx::TEXT_DATA);
  assert((datum.as_text() == text).is_true());

  reference_column->set(row_id, row_id);
  reference_column->get(row_id, &datum);
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.as_int().match(row_id));

  grnxx::Bool bool_vector_value[] = {
    grnxx::Bool(true),
    grnxx::Bool(false),
    grnxx::Bool(true)
  };
  grnxx::BoolVector bool_vector(bool_vector_value, 3);
  bool_vector_column->set(row_id, bool_vector);
  bool_vector_column->get(row_id, &datum);
  assert(datum.type() == grnxx::BOOL_VECTOR_DATA);
  assert((datum.as_bool_vector() == bool_vector).is_true());

  grnxx::Int int_vector_value[] = {
    grnxx::Int(123),
    grnxx::Int(-456),
    grnxx::Int(789)
  };
  grnxx::IntVector int_vector(int_vector_value, 3);
  int_vector_column->set(row_id, int_vector);
  int_vector_column->get(row_id, &datum);
  assert(datum.type() == grnxx::INT_VECTOR_DATA);
  assert((datum.as_int_vector() == int_vector).is_true());

  grnxx::Float float_vector_value[] = {
    grnxx::Float(1.23),
    grnxx::Float(-4.56),
    grnxx::Float(7.89)
  };
  grnxx::FloatVector float_vector(float_vector_value, 3);
  float_vector_column->set(row_id, float_vector);
  float_vector_column->get(row_id, &datum);
  assert(datum.type() == grnxx::FLOAT_VECTOR_DATA);
  assert((datum.as_float_vector() == float_vector).is_true());

  grnxx::GeoPoint geo_point_vector_value[] = {
    { grnxx::Float(43.068661), grnxx::Float(141.350755) },  // Sapporo.
    { grnxx::Float(35.681382), grnxx::Float(139.766084) },  // Tokyo.
    { grnxx::Float(34.702485), grnxx::Float(135.495951) },  // Osaka.
  };
  grnxx::GeoPointVector geo_point_vector(geo_point_vector_value, 3);
  geo_point_vector_column->set(row_id, geo_point_vector);
  geo_point_vector_column->get(row_id, &datum);
  assert(datum.type() == grnxx::GEO_POINT_VECTOR_DATA);
  assert((datum.as_geo_point_vector() == geo_point_vector).is_true());

  grnxx::Int reference_vector_value[] = {
    grnxx::Int(0),
    grnxx::Int(0),
    grnxx::Int(0)
  };
  grnxx::IntVector reference_vector(reference_vector_value, 3);
  reference_vector_column->set(row_id, reference_vector);
  reference_vector_column->get(row_id, &datum);
  assert(datum.type() == grnxx::INT_VECTOR_DATA);
  assert((datum.as_int_vector() == reference_vector).is_true());

  grnxx::Text text_values[] = {
    grnxx::Text("abc"),
    grnxx::Text("DEF"),
    grnxx::Text("ghi")
  };
  grnxx::TextVector text_vector(text_values, 3);
  text_vector_column->set(row_id, text_vector);
  text_vector_column->get(row_id, &datum);
  assert(datum.type() == grnxx::TEXT_VECTOR_DATA);
  assert((datum.as_text_vector() == text_vector).is_true());
}

int main() {
  test_column();
  return 0;
}
