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

  // Create a column named "VectorBoolColumn".
  // The column stores Text values.
  auto vector_bool_column = table->create_column(&error, "VectorBoolColumn",
                                                 grnxx::VECTOR_BOOL_DATA);
  assert(vector_bool_column);
  assert(vector_bool_column->table() == table);
  assert(vector_bool_column->name() == "VectorBoolColumn");
  assert(vector_bool_column->data_type() == grnxx::VECTOR_BOOL_DATA);
  assert(!vector_bool_column->has_key_attribute());
  assert(vector_bool_column->num_indexes() == 0);

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

  assert(text_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::TEXT_DATA);
  assert(datum.force_text() == "");

  assert(vector_bool_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::VECTOR_BOOL_DATA);
  assert(datum.force_vector_bool() == grnxx::Vector<grnxx::Bool>{});

  // Set and get values.
  assert(bool_column->set(&error, 1, grnxx::Bool(true)));
  assert(int_column->set(&error, 1, grnxx::Int(123)));
  assert(float_column->set(&error, 1, grnxx::Float(0.25)));
  assert(text_column->set(&error, 1, grnxx::Text("Hello, world!")));
  assert(vector_bool_column->set(&error, 1,
         grnxx::Vector<grnxx::Bool>{ true, false, true }));

  assert(bool_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::BOOL_DATA);
  assert(datum.force_bool());

  assert(int_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.force_int() == 123);

  assert(float_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::FLOAT_DATA);
  assert(datum.force_float() == 0.25);

  assert(text_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::TEXT_DATA);
  assert(datum.force_text() == "Hello, world!");

  assert(vector_bool_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::VECTOR_BOOL_DATA);
  assert((datum.force_vector_bool() ==
          grnxx::Vector<grnxx::Bool>{ true, false, true }));
}

int main() {
  test_column();
  return 0;
}
