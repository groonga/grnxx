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
//  assert(bool_column->num_indexes() == 0);

  // Create a column named "Int".
  auto int_column = table->create_column("Int", grnxx::INT_DATA);
  assert(int_column->table() == table);
  assert(int_column->name() == "Int");
  assert(int_column->data_type() == grnxx::INT_DATA);
  assert(!int_column->reference_table());
  assert(!int_column->is_key());
//  assert(int_column->num_indexes() == 0);

  // Create a column named "Float".
  auto float_column = table->create_column("Float", grnxx::FLOAT_DATA);
  assert(float_column->table() == table);
  assert(float_column->name() == "Float");
  assert(float_column->data_type() == grnxx::FLOAT_DATA);
  assert(!float_column->reference_table());
  assert(!float_column->is_key());
//  assert(float_column->num_indexes() == 0);

//  // Create a column named "GeoPointColumn".
//  // The column stores GeoPoint values.
//  auto geo_point_column = table->create_column(&error, "GeoPointColumn",
//                                               grnxx::GEO_POINT_DATA);
//  assert(geo_point_column);
//  assert(geo_point_column->table() == table);
//  assert(geo_point_column->name() == "GeoPointColumn");
//  assert(geo_point_column->data_type() == grnxx::GEO_POINT_DATA);
//  assert(!geo_point_column->ref_table());
//  assert(!geo_point_column->has_key_attribute());
//  assert(geo_point_column->num_indexes() == 0);

//  // Create a column named "TextColumn".
//  // The column stores Text values.
//  auto text_column = table->create_column(&error, "TextColumn",
//                                           grnxx::TEXT_DATA);
//  assert(text_column);
//  assert(text_column->table() == table);
//  assert(text_column->name() == "TextColumn");
//  assert(text_column->data_type() == grnxx::TEXT_DATA);
//  assert(!text_column->ref_table());
//  assert(!text_column->has_key_attribute());
//  assert(text_column->num_indexes() == 0);

  // Create a column named "Reference".
  grnxx::ColumnOptions options;
  options.reference_table_name = "Table";
  auto reference_column =
      table->create_column("Reference", grnxx::INT_DATA, options);
  assert(reference_column->table() == table);
  assert(reference_column->name() == "Reference");
  assert(reference_column->data_type() == grnxx::INT_DATA);
  assert(reference_column->reference_table());
  assert(!reference_column->is_key());
//  assert(int_column->num_indexes() == 0);

//  // Create a column named "BoolVectorColumn".
//  // The column stores Text values.
//  auto bool_vector_column = table->create_column(&error, "BoolVectorColumn",
//                                                 grnxx::BOOL_VECTOR_DATA);
//  assert(bool_vector_column);
//  assert(bool_vector_column->table() == table);
//  assert(bool_vector_column->name() == "BoolVectorColumn");
//  assert(bool_vector_column->data_type() == grnxx::BOOL_VECTOR_DATA);
//  assert(!bool_vector_column->ref_table());
//  assert(!bool_vector_column->has_key_attribute());
//  assert(bool_vector_column->num_indexes() == 0);

//  // Create a column named "IntVectorColumn".
//  // The column stores Text values.
//  auto int_vector_column = table->create_column(&error, "IntVectorColumn",
//                                                 grnxx::INT_VECTOR_DATA);
//  assert(int_vector_column);
//  assert(int_vector_column->table() == table);
//  assert(int_vector_column->name() == "IntVectorColumn");
//  assert(int_vector_column->data_type() == grnxx::INT_VECTOR_DATA);
//  assert(!int_vector_column->ref_table());
//  assert(!int_vector_column->has_key_attribute());
//  assert(int_vector_column->num_indexes() == 0);

//  // Create a column named "FloatVectorColumn".
//  // The column stores Text values.
//  auto float_vector_column = table->create_column(&error, "FloatVectorColumn",
//                                                  grnxx::FLOAT_VECTOR_DATA);
//  assert(float_vector_column);
//  assert(float_vector_column->table() == table);
//  assert(float_vector_column->name() == "FloatVectorColumn");
//  assert(float_vector_column->data_type() == grnxx::FLOAT_VECTOR_DATA);
//  assert(!float_vector_column->ref_table());
//  assert(!float_vector_column->has_key_attribute());
//  assert(float_vector_column->num_indexes() == 0);

//  // Create a column named "GeoPointVectorColumn".
//  // The column stores Text values.
//  auto geo_point_vector_column = table->create_column(
//      &error, "GeoPointVectorColumn", grnxx::GEO_POINT_VECTOR_DATA);
//  assert(geo_point_vector_column);
//  assert(geo_point_vector_column->table() == table);
//  assert(geo_point_vector_column->name() == "GeoPointVectorColumn");
//  assert(geo_point_vector_column->data_type() == grnxx::GEO_POINT_VECTOR_DATA);
//  assert(!geo_point_vector_column->ref_table());
//  assert(!geo_point_vector_column->has_key_attribute());
//  assert(geo_point_vector_column->num_indexes() == 0);

//  // Create a column named "TextVectorColumn".
//  // The column stores Text values.
//  auto text_vector_column = table->create_column(&error, "TextVectorColumn",
//                                                 grnxx::TEXT_VECTOR_DATA);
//  assert(text_vector_column);
//  assert(text_vector_column->table() == table);
//  assert(text_vector_column->name() == "TextVectorColumn");
//  assert(text_vector_column->data_type() == grnxx::TEXT_VECTOR_DATA);
//  assert(!text_vector_column->ref_table());
//  assert(!text_vector_column->has_key_attribute());
//  assert(text_vector_column->num_indexes() == 0);

//  // Create a column named "RefVectorColumn".
//  // The column stores Int values.
//  options.ref_table_name = "Table";
//  auto ref_vector_column =
//      table->create_column(&error, "RefVectorColumn",
//                           grnxx::INT_VECTOR_DATA, options);
//  assert(ref_vector_column);
//  assert(ref_vector_column->table() == table);
//  assert(ref_vector_column->name() == "RefVectorColumn");
//  assert(ref_vector_column->data_type() == grnxx::INT_VECTOR_DATA);
//  assert(ref_vector_column->ref_table());
//  assert(!ref_vector_column->has_key_attribute());
//  assert(ref_vector_column->num_indexes() == 0);

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

//  assert(geo_point_column->get(&error, 1, &datum));
//  assert(datum.type() == grnxx::GEO_POINT_DATA);
//  assert(datum.force_geo_point() == grnxx::GeoPoint(0, 0));

//  assert(text_column->get(&error, 1, &datum));
//  assert(datum.type() == grnxx::TEXT_DATA);
//  assert(datum.force_text() == "");

  reference_column->get(row_id, &datum);
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.as_int().is_na());

//  assert(bool_vector_column->get(&error, 1, &datum));
//  assert(datum.type() == grnxx::BOOL_VECTOR_DATA);
//  assert(datum.force_bool_vector() == grnxx::BoolVector{});

//  assert(int_vector_column->get(&error, 1, &datum));
//  assert(datum.type() == grnxx::INT_VECTOR_DATA);
//  assert(datum.force_int_vector() == grnxx::IntVector(nullptr, 0));

//  assert(float_vector_column->get(&error, 1, &datum));
//  assert(datum.type() == grnxx::FLOAT_VECTOR_DATA);
//  assert(datum.force_float_vector() == grnxx::FloatVector(nullptr, 0));

//  assert(geo_point_vector_column->get(&error, 1, &datum));
//  assert(datum.type() == grnxx::GEO_POINT_VECTOR_DATA);
//  assert(datum.force_geo_point_vector() == grnxx::GeoPointVector(nullptr, 0));

//  assert(text_vector_column->get(&error, 1, &datum));
//  assert(datum.type() == grnxx::TEXT_VECTOR_DATA);
//  assert(datum.force_text_vector() == grnxx::TextVector(nullptr, 0));

//  assert(ref_vector_column->get(&error, 1, &datum));
//  assert(datum.type() == grnxx::INT_VECTOR_DATA);
//  assert(datum.force_int_vector() == grnxx::IntVector(nullptr, 0));

  // Set and get values.
  bool_column->set(row_id, grnxx::Bool(true));
  bool_column->get(row_id, &datum);
  assert(datum.type() == grnxx::BOOL_DATA);
  assert(datum.as_bool() == grnxx::Bool(true));

  int_column->set(row_id, grnxx::Int(123));
  int_column->get(row_id, &datum);
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.as_int() == grnxx::Int(123));

  float_column->set(row_id, grnxx::Float(1.25));
  float_column->get(row_id, &datum);
  assert(datum.type() == grnxx::FLOAT_DATA);
  assert(datum.as_float() == grnxx::Float(1.25));

  reference_column->set(row_id, row_id);
  reference_column->get(row_id, &datum);
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.as_int() == row_id);

//  // Set and get values.
//  assert(geo_point_column->set(&error, 1, grnxx::GeoPoint(123, 456)));
//  assert(text_column->set(&error, 1, grnxx::Text("Hello, world!")));
//  assert(bool_vector_column->set(&error, 1,
//                                 grnxx::BoolVector{ true, false, true }));
//  grnxx::Int int_vector_value[] = { 123, -456, 789 };
//  assert(int_vector_column->set(&error, 1,
//                                grnxx::IntVector(int_vector_value, 3)));
//  grnxx::Float float_vector_value[] = { 1.23, -4.56, 7.89 };
//  assert(float_vector_column->set(&error, 1,
//                                  grnxx::FloatVector(float_vector_value, 3)));
//  grnxx::GeoPoint geo_point_vector_value[] = {
//    { 123, 456 }, { 789, 123 }, { 456, 789 }
//  };
//  assert(geo_point_vector_column->set(
//      &error, 1, grnxx::GeoPointVector(geo_point_vector_value, 3)));
//  grnxx::Text text_vector_value[] = { "abc", "DEF", "ghi" };
//  assert(text_vector_column->set(&error, 1,
//                                 grnxx::TextVector(text_vector_value, 3)));
//  grnxx::Int ref_vector_value[] = { 1, 1, 1 };
//  assert(ref_vector_column->set(&error, 1,
//                                grnxx::IntVector(ref_vector_value, 3)));

//  assert(geo_point_column->get(&error, 1, &datum));
//  assert(datum.type() == grnxx::GEO_POINT_DATA);
//  assert(datum.force_geo_point() == grnxx::GeoPoint(123, 456));

//  assert(text_column->get(&error, 1, &datum));
//  assert(datum.type() == grnxx::TEXT_DATA);
//  assert(datum.force_text() == "Hello, world!");

//  assert(bool_vector_column->get(&error, 1, &datum));
//  assert(datum.type() == grnxx::BOOL_VECTOR_DATA);
//  assert(datum.force_bool_vector() ==
//         grnxx::BoolVector({ true, false, true }));

//  assert(int_vector_column->get(&error, 1, &datum));
//  assert(datum.type() == grnxx::INT_VECTOR_DATA);
//  assert(datum.force_int_vector() ==
//         grnxx::IntVector(int_vector_value, 3));

//  assert(float_vector_column->get(&error, 1, &datum));
//  assert(datum.type() == grnxx::FLOAT_VECTOR_DATA);
//  assert(datum.force_float_vector() ==
//         grnxx::FloatVector(float_vector_value, 3));

//  assert(geo_point_vector_column->get(&error, 1, &datum));
//  assert(datum.type() == grnxx::GEO_POINT_VECTOR_DATA);
//  assert(datum.force_geo_point_vector() ==
//         grnxx::GeoPointVector(geo_point_vector_value, 3));

//  assert(text_vector_column->get(&error, 1, &datum));
//  assert(datum.type() == grnxx::TEXT_VECTOR_DATA);
//  assert(datum.force_text_vector() ==
//         grnxx::TextVector(text_vector_value, 3));

//  assert(ref_vector_column->get(&error, 1, &datum));
//  assert(datum.type() == grnxx::INT_VECTOR_DATA);
//  assert(datum.force_int_vector() ==
//         grnxx::IntVector(ref_vector_value, 3));
}

int main() {
  test_column();
  return 0;
}
