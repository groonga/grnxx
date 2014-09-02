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
#include "grnxx/cursor.hpp"
#include "grnxx/datum.hpp"
#include "grnxx/db.hpp"
#include "grnxx/error.hpp"
#include "grnxx/table.hpp"

void test_column() {
  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table with the default options.
  auto table = db->create_table(&error, "Table");
  assert(table);

  // Append the first row.
  grnxx::Int row_id;
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));

  // Create a column named "BoolColumn".
  // The column stores Bool values.
  auto bool_column = table->create_column(&error, "BoolColumn",
                                          grnxx::BOOL_DATA);
  assert(bool_column);
  assert(bool_column->table() == table);
  assert(bool_column->name() == "BoolColumn");
  assert(bool_column->data_type() == grnxx::BOOL_DATA);
  assert(!bool_column->has_key_attribute());
  assert(bool_column->num_indexes() == 0);

  // Create a column named "IntColumn".
  // The column stores Int values.
  auto int_column = table->create_column(&error, "IntColumn",
                                         grnxx::INT_DATA);
  assert(int_column);
  assert(int_column->table() == table);
  assert(int_column->name() == "IntColumn");
  assert(int_column->data_type() == grnxx::INT_DATA);
  assert(!int_column->has_key_attribute());
  assert(int_column->num_indexes() == 0);

  // Create a column named "FloatColumn".
  // The column stores Float values.
  auto float_column = table->create_column(&error, "FloatColumn",
                                           grnxx::FLOAT_DATA);
  assert(float_column);
  assert(float_column->table() == table);
  assert(float_column->name() == "FloatColumn");
  assert(float_column->data_type() == grnxx::FLOAT_DATA);
  assert(!float_column->has_key_attribute());
  assert(float_column->num_indexes() == 0);

  // Create a column named "GeoPointColumn".
  // The column stores GeoPoint values.
  auto geo_point_column = table->create_column(&error, "GeoPointColumn",
                                               grnxx::GEO_POINT_DATA);
  assert(geo_point_column);
  assert(geo_point_column->table() == table);
  assert(geo_point_column->name() == "GeoPointColumn");
  assert(geo_point_column->data_type() == grnxx::GEO_POINT_DATA);
  assert(!geo_point_column->has_key_attribute());
  assert(geo_point_column->num_indexes() == 0);

  // Create a column named "TextColumn".
  // The column stores Text values.
  auto text_column = table->create_column(&error, "TextColumn",
                                           grnxx::TEXT_DATA);
  assert(text_column);
  assert(text_column->table() == table);
  assert(text_column->name() == "TextColumn");
  assert(text_column->data_type() == grnxx::TEXT_DATA);
  assert(!text_column->has_key_attribute());
  assert(text_column->num_indexes() == 0);

  // Create a column named "BoolVectorColumn".
  // The column stores Text values.
  auto bool_vector_column = table->create_column(&error, "BoolVectorColumn",
                                                 grnxx::BOOL_VECTOR_DATA);
  assert(bool_vector_column);
  assert(bool_vector_column->table() == table);
  assert(bool_vector_column->name() == "BoolVectorColumn");
  assert(bool_vector_column->data_type() == grnxx::BOOL_VECTOR_DATA);
  assert(!bool_vector_column->has_key_attribute());
  assert(bool_vector_column->num_indexes() == 0);

  // Create a column named "IntVectorColumn".
  // The column stores Text values.
  auto int_vector_column = table->create_column(&error, "IntVectorColumn",
                                                 grnxx::INT_VECTOR_DATA);
  assert(int_vector_column);
  assert(int_vector_column->table() == table);
  assert(int_vector_column->name() == "IntVectorColumn");
  assert(int_vector_column->data_type() == grnxx::INT_VECTOR_DATA);
  assert(!int_vector_column->has_key_attribute());
  assert(int_vector_column->num_indexes() == 0);

  // Create a column named "FloatVectorColumn".
  // The column stores Text values.
  auto float_vector_column = table->create_column(&error, "FloatVectorColumn",
                                                  grnxx::FLOAT_VECTOR_DATA);
  assert(float_vector_column);
  assert(float_vector_column->table() == table);
  assert(float_vector_column->name() == "FloatVectorColumn");
  assert(float_vector_column->data_type() == grnxx::FLOAT_VECTOR_DATA);
  assert(!float_vector_column->has_key_attribute());
  assert(float_vector_column->num_indexes() == 0);

  // Create a column named "GeoPointVectorColumn".
  // The column stores Text values.
  auto geo_point_vector_column = table->create_column(
      &error, "GeoPointVectorColumn", grnxx::GEO_POINT_VECTOR_DATA);
  assert(geo_point_vector_column);
  assert(geo_point_vector_column->table() == table);
  assert(geo_point_vector_column->name() == "GeoPointVectorColumn");
  assert(geo_point_vector_column->data_type() == grnxx::GEO_POINT_VECTOR_DATA);
  assert(!geo_point_vector_column->has_key_attribute());
  assert(geo_point_vector_column->num_indexes() == 0);

  grnxx::Datum datum;

  // Check that the default values are stored.
  assert(bool_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::BOOL_DATA);
  assert(!datum.force_bool());

  assert(int_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.force_int() == 0);

  assert(float_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::FLOAT_DATA);
  assert(datum.force_float() == 0.0);

  assert(geo_point_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::GEO_POINT_DATA);
  assert(datum.force_geo_point() == grnxx::GeoPoint(0, 0));

  assert(text_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::TEXT_DATA);
  assert(datum.force_text() == "");

  assert(bool_vector_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::BOOL_VECTOR_DATA);
  assert(datum.force_bool_vector() == grnxx::BoolVector{});

  assert(int_vector_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::INT_VECTOR_DATA);
  assert(datum.force_int_vector() == grnxx::IntVector(nullptr, 0));

  assert(float_vector_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::FLOAT_VECTOR_DATA);
  assert(datum.force_float_vector() == grnxx::FloatVector(nullptr, 0));

  assert(geo_point_vector_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::GEO_POINT_VECTOR_DATA);
  assert(datum.force_geo_point_vector() == grnxx::GeoPointVector(nullptr, 0));

  // Set and get values.
  assert(bool_column->set(&error, 1, grnxx::Bool(true)));
  assert(int_column->set(&error, 1, grnxx::Int(123)));
  assert(float_column->set(&error, 1, grnxx::Float(0.25)));
  assert(geo_point_column->set(&error, 1, grnxx::GeoPoint(123, 456)));
  assert(text_column->set(&error, 1, grnxx::Text("Hello, world!")));
  assert(bool_vector_column->set(&error, 1,
                                 grnxx::BoolVector{ true, false, true }));
  grnxx::Int int_vector_value[] = { 123, -456, 789 };
  assert(int_vector_column->set(&error, 1,
                                grnxx::IntVector(int_vector_value, 3)));
  grnxx::Float float_vector_value[] = { 1.23, -4.56, 7.89 };
  assert(float_vector_column->set(&error, 1,
                                  grnxx::FloatVector(float_vector_value, 3)));
  grnxx::GeoPoint geo_point_vector_value[] = {
    { 123, 456 }, { 789, 123 }, { 456, 789 }
  };
  assert(geo_point_vector_column->set(
      &error, 1, grnxx::GeoPointVector(geo_point_vector_value, 3)));

  assert(bool_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::BOOL_DATA);
  assert(datum.force_bool());

  assert(int_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.force_int() == 123);

  assert(float_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::FLOAT_DATA);
  assert(datum.force_float() == 0.25);

  assert(geo_point_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::GEO_POINT_DATA);
  assert(datum.force_geo_point() == grnxx::GeoPoint(123, 456));

  assert(text_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::TEXT_DATA);
  assert(datum.force_text() == "Hello, world!");

  assert(bool_vector_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::BOOL_VECTOR_DATA);
  assert(datum.force_bool_vector() ==
         grnxx::BoolVector({ true, false, true }));

  assert(int_vector_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::INT_VECTOR_DATA);
  assert(datum.force_int_vector() ==
         grnxx::IntVector(int_vector_value, 3));

  assert(float_vector_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::FLOAT_VECTOR_DATA);
  assert(datum.force_float_vector() ==
         grnxx::FloatVector(float_vector_value, 3));

  assert(geo_point_vector_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::GEO_POINT_VECTOR_DATA);
  assert(datum.force_geo_point_vector() ==
         grnxx::GeoPointVector(geo_point_vector_value, 3));
}

int main() {
  test_column();
  return 0;
}
