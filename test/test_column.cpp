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
#include "grnxx/db.hpp"
#include "grnxx/table.hpp"

std::mt19937_64 rng;

template <typename T>
void test_basic_operations(const T &value) {
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

void test_basic_operations() {
  test_basic_operations(grnxx::Bool(true));
  test_basic_operations(grnxx::Int(123));
  test_basic_operations(grnxx::Float(1.25));
  test_basic_operations(grnxx::GeoPoint(grnxx::Int(123), grnxx::Int(456)));
  test_basic_operations(
      grnxx::GeoPoint(grnxx::Float(35.681382), grnxx::Float(-139.766084)));
  test_basic_operations(grnxx::Text("ABC"));

  grnxx::Bool bool_values[] = {
    grnxx::Bool(true),
    grnxx::Bool(false),
    grnxx::Bool(true)
  };
  grnxx::BoolVector bool_vector(bool_values, 3);
  test_basic_operations(bool_vector);

  grnxx::Int int_values[] = {
    grnxx::Int(123),
    grnxx::Int(-456),
    grnxx::Int(789)
  };
  grnxx::IntVector int_vector(int_values, 3);
  test_basic_operations(int_vector);

  grnxx::Float float_values[] = {
    grnxx::Float(1.23),
    grnxx::Float(-4.56),
    grnxx::Float(7.89)
  };
  grnxx::FloatVector float_vector(float_values, 3);
  test_basic_operations(float_vector);

  grnxx::GeoPoint geo_point_values[] = {
    { grnxx::Float(43.068661), grnxx::Float(141.350755) },  // Sapporo.
    { grnxx::Float(35.681382), grnxx::Float(139.766084) },  // Tokyo.
    { grnxx::Float(34.702485), grnxx::Float(135.495951) },  // Osaka.
  };
  grnxx::GeoPointVector geo_point_vector(geo_point_values, 3);
  test_basic_operations(geo_point_vector);

  grnxx::Text text_values[] = {
    grnxx::Text("abc"),
    grnxx::Text("DEF"),
    grnxx::Text("ghi")
  };
  grnxx::TextVector text_vector(text_values, 3);
  test_basic_operations(text_vector);
}

template <typename T>
void generate_random_value(T *value);

template <>
void generate_random_value(grnxx::Bool *value) {
  if ((rng() % 256) == 0) {
    *value = grnxx::Bool::na();
  } else {
    *value = grnxx::Bool((rng() & 1) == 1);
  }
}

template <>
void generate_random_value(grnxx::Int *value) {
  if ((rng() % 256) == 0) {
    *value = grnxx::Int::na();
  } else {
    *value = grnxx::Int(static_cast<int64_t>(rng()));
  }
}

template <>
void generate_random_value(grnxx::Float *value) {
  if ((rng() % 256) == 0) {
    *value = grnxx::Float::na();
  } else {
    *value = grnxx::Float(1.0 * rng() / rng.max());
  }
}

template <>
void generate_random_value(grnxx::GeoPoint *value) {
  if ((rng() % 256) == 0) {
    *value = grnxx::GeoPoint::na();
  } else {
    grnxx::Float latitude(-90.0 + (180.0 * rng() / rng.max()));
    grnxx::Float longitude(-180.0 + (360.0 * rng() / rng.max()));
    *value = grnxx::GeoPoint(latitude, longitude);
  }
}

template <>
void generate_random_value(grnxx::Text *value) {
  static grnxx::Array<grnxx::String> bodies;
  if ((rng() % 256) == 0) {
    *value = grnxx::Text::na();
  } else {
    grnxx::String body;
    body.resize(rng() % 16);
    for (size_t i = 0; i < body.size(); ++i) {
      body[i] = 'A' + (rng() % 26);
    }
    *value = grnxx::Text(body.data(), body.size());
    bodies.push_back(std::move(body));
  }
}

template <typename T>
void test_random_values() {
  constexpr size_t NUM_ROWS = 1 << 16;

  // Create a table and insert the first row.
  auto db = grnxx::open_db("");
  auto table = db->create_table("Table");
  auto column = table->create_column("Column", T::type());
  grnxx::Array<T> values;
  values.resize(NUM_ROWS);
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    generate_random_value<T>(&values[i]);
    grnxx::Int row_id = table->insert_row();
    column->set(row_id, values[i]);
    grnxx::Datum datum;
    column->get(row_id, &datum);

    T stored_value;
    datum.force(&stored_value);
    assert(stored_value.match(values[i]));
  }

  // Test all the values again.
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = grnxx::Int(i);
    grnxx::Datum datum;
    column->get(row_id, &datum);
    T stored_value;
    datum.force(&stored_value);
    assert(stored_value.match(values[i]));
  }
}

void test_internal_type_conversion() {
  // Create a table and insert rows.
  auto db = grnxx::open_db("");
  auto table = db->create_table("Table");
  table->insert_row();
  table->insert_row();
  table->insert_row();
  table->insert_row();

  auto column = table->create_column("Column", grnxx::INT_DATA);

  // Set the first 8-bit integer.
  column->set(grnxx::Int(0), grnxx::Int(int64_t(1) << 0));
  grnxx::Datum datum;
  column->get(grnxx::Int(0), &datum);
  assert(datum.as_int().raw() == (int64_t(1) << 0));

  // Conversion from 8-bit to 16-bit.
  column->set(grnxx::Int(1), grnxx::Int(int64_t(1) << 8));
  column->get(grnxx::Int(0), &datum);
  assert(datum.as_int().raw() == (int64_t(1) << 0));
  column->get(grnxx::Int(1), &datum);
  assert(datum.as_int().raw() == (int64_t(1) << 8));

  // Conversion from 16-bit to 32-bit.
  column->set(grnxx::Int(2), grnxx::Int(int64_t(1) << 16));
  column->get(grnxx::Int(0), &datum);
  assert(datum.as_int().raw() == (int64_t(1) << 0));
  column->get(grnxx::Int(1), &datum);
  assert(datum.as_int().raw() == (int64_t(1) << 8));
  column->get(grnxx::Int(2), &datum);
  assert(datum.as_int().raw() == (int64_t(1) << 16));

  // Conversion from 32-bit to 64-bit.
  column->set(grnxx::Int(3), grnxx::Int(int64_t(1) << 32));
  column->get(grnxx::Int(0), &datum);
  assert(datum.as_int().raw() == (int64_t(1) << 0));
  column->get(grnxx::Int(1), &datum);
  assert(datum.as_int().raw() == (int64_t(1) << 8));
  column->get(grnxx::Int(2), &datum);
  assert(datum.as_int().raw() == (int64_t(1) << 16));
  column->get(grnxx::Int(3), &datum);
  assert(datum.as_int().raw() == (int64_t(1) << 32));

  table->remove_column("Column");
  column = table->create_column("Column", grnxx::INT_DATA);

  // Conversion from 8-bit to 32-bit.
  column->set(grnxx::Int(0), grnxx::Int(int64_t(1) << 0));
  column->set(grnxx::Int(1), grnxx::Int(int64_t(1) << 16));
  column->get(grnxx::Int(0), &datum);
  assert(datum.as_int().raw() == (int64_t(1) << 0));
  column->get(grnxx::Int(1), &datum);
  assert(datum.as_int().raw() == (int64_t(1) << 16));

  table->remove_column("Column");
  column = table->create_column("Column", grnxx::INT_DATA);

  // Conversion from 8-bit to 64-bit.
  column->set(grnxx::Int(0), grnxx::Int(int64_t(1) << 0));
  column->set(grnxx::Int(1), grnxx::Int(int64_t(1) << 32));
  column->get(grnxx::Int(0), &datum);
  assert(datum.as_int().raw() == (int64_t(1) << 0));
  column->get(grnxx::Int(1), &datum);
  assert(datum.as_int().raw() == (int64_t(1) << 32));

  table->remove_column("Column");
  column = table->create_column("Column", grnxx::INT_DATA);

  // Conversion from 16-bit to 64-bit.
  column->set(grnxx::Int(0), grnxx::Int(int64_t(1) << 8));
  column->set(grnxx::Int(1), grnxx::Int(int64_t(1) << 32));
  column->get(grnxx::Int(0), &datum);
  assert(datum.as_int().raw() == (int64_t(1) << 8));
  column->get(grnxx::Int(1), &datum);
  assert(datum.as_int().raw() == (int64_t(1) << 32));
}

int main() {
  test_basic_operations();

  test_random_values<grnxx::Bool>();
  test_random_values<grnxx::Int>();
  test_random_values<grnxx::Float>();
  test_random_values<grnxx::GeoPoint>();
  test_random_values<grnxx::Text>();

  test_internal_type_conversion();

  return 0;
}
