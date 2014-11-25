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
#include <memory>
#include <random>

#include "grnxx/column.hpp"
#include "grnxx/cursor.hpp"
#include "grnxx/db.hpp"
#include "grnxx/expression.hpp"
#include "grnxx/table.hpp"

std::mt19937_64 mersenne_twister;

struct {
  std::unique_ptr<grnxx::DB> db;
  grnxx::Table *table;
  grnxx::Array<grnxx::Bool> bool_values;
  grnxx::Array<grnxx::Bool> bool2_values;
  grnxx::Array<grnxx::Int> int_values;
  grnxx::Array<grnxx::Int> int2_values;
  grnxx::Array<grnxx::Float> float_values;
  grnxx::Array<grnxx::Float> float2_values;
  grnxx::Array<grnxx::GeoPoint> geo_point_values;
  grnxx::Array<grnxx::GeoPoint> geo_point2_values;
  grnxx::Array<grnxx::Text> text_values;
  grnxx::Array<grnxx::Text> text2_values;
  grnxx::Array<std::string> text_bodies;
  grnxx::Array<std::string> text2_bodies;
  grnxx::Array<grnxx::BoolVector> bool_vector_values;
  grnxx::Array<grnxx::BoolVector> bool_vector2_values;
  grnxx::Array<grnxx::Array<grnxx::Bool>> bool_vector_bodies;
  grnxx::Array<grnxx::Array<grnxx::Bool>> bool_vector2_bodies;
  grnxx::Array<grnxx::IntVector> int_vector_values;
  grnxx::Array<grnxx::IntVector> int_vector2_values;
  grnxx::Array<grnxx::Array<grnxx::Int>> int_vector_bodies;
  grnxx::Array<grnxx::Array<grnxx::Int>> int_vector2_bodies;
  grnxx::Array<grnxx::FloatVector> float_vector_values;
  grnxx::Array<grnxx::FloatVector> float_vector2_values;
  grnxx::Array<grnxx::Array<grnxx::Float>> float_vector_bodies;
  grnxx::Array<grnxx::Array<grnxx::Float>> float_vector2_bodies;
  grnxx::Array<grnxx::GeoPointVector> geo_point_vector_values;
  grnxx::Array<grnxx::GeoPointVector> geo_point_vector2_values;
  grnxx::Array<grnxx::Array<grnxx::GeoPoint>> geo_point_vector_bodies;
  grnxx::Array<grnxx::Array<grnxx::GeoPoint>> geo_point_vector2_bodies;
  grnxx::Array<grnxx::TextVector> text_vector_values;
  grnxx::Array<grnxx::TextVector> text_vector2_values;
  grnxx::Array<grnxx::Array<grnxx::Text>> text_vector_bodies;
  grnxx::Array<grnxx::Array<grnxx::Text>> text_vector2_bodies;
  grnxx::Array<grnxx::Int> ref_values;
  grnxx::Array<grnxx::Int> ref2_values;
  grnxx::Array<grnxx::IntVector> ref_vector_values;
  grnxx::Array<grnxx::IntVector> ref_vector2_values;
  grnxx::Array<grnxx::Array<grnxx::Int>> ref_vector_bodies;
  grnxx::Array<grnxx::Array<grnxx::Int>> ref_vector2_bodies;
} test;

void generate_text(size_t min_size, size_t max_size, std::string *string) {
  size_t size = (mersenne_twister() % (max_size - min_size + 1)) + min_size;
  string->resize(size);
  char *string_buffer = &(*string)[0];
  for (size_t i = 0; i < size; ++i) {
    string_buffer[i] = '0' + (mersenne_twister() % 10);
  }
}

void init_test() try {
  // Create a database with the default options.
  test.db = grnxx::open_db("");

  // Create a table with the default options.
  test.table = test.db->create_table("Table");

  // Create columns for various data types.
  grnxx::DataType data_type = grnxx::BOOL_DATA;
  auto bool_column = test.table->create_column("Bool", data_type);
  auto bool2_column = test.table->create_column("Bool2", data_type);

  data_type = grnxx::INT_DATA;
  auto int_column = test.table->create_column("Int", data_type);
  auto int2_column = test.table->create_column("Int2", data_type);

  data_type = grnxx::FLOAT_DATA;
  auto float_column = test.table->create_column("Float", data_type);
  auto float2_column = test.table->create_column("Float2", data_type);

  data_type = grnxx::GEO_POINT_DATA;
  auto geo_point_column = test.table->create_column("GeoPoint", data_type);
  auto geo_point2_column = test.table->create_column("GeoPoint2", data_type);

  data_type = grnxx::TEXT_DATA;
  auto text_column = test.table->create_column("Text", data_type);
  auto text2_column = test.table->create_column("Text2", data_type);

  data_type = grnxx::BOOL_VECTOR_DATA;
  auto bool_vector_column =
      test.table->create_column("BoolVector", data_type);
  auto bool_vector2_column =
      test.table->create_column("BoolVector2", data_type);

  data_type = grnxx::INT_VECTOR_DATA;
  auto int_vector_column =
      test.table->create_column("IntVector", data_type);
  auto int_vector2_column =
      test.table->create_column("IntVector2", data_type);

  data_type = grnxx::FLOAT_VECTOR_DATA;
  auto float_vector_column =
      test.table->create_column("FloatVector", data_type);
  auto float_vector2_column =
      test.table->create_column("FloatVector2", data_type);

  data_type = grnxx::GEO_POINT_VECTOR_DATA;
  auto geo_point_vector_column =
      test.table->create_column("GeoPointVector", data_type);
  auto geo_point_vector2_column =
      test.table->create_column("GeoPointVector2", data_type);

  data_type = grnxx::TEXT_VECTOR_DATA;
  auto text_vector_column =
      test.table->create_column("TextVector", data_type);
  auto text_vector2_column =
      test.table->create_column("TextVector2", data_type);

  data_type = grnxx::INT_DATA;
  grnxx::ColumnOptions options;
  options.reference_table_name = "Table";
  auto ref_column = test.table->create_column("Ref", data_type, options);
  auto ref2_column = test.table->create_column("Ref2", data_type, options);

  data_type = grnxx::INT_VECTOR_DATA;
  auto ref_vector_column =
      test.table->create_column("RefVector", data_type, options);
  auto ref_vector2_column =
      test.table->create_column("RefVector2", data_type, options);

  // Generate random values.
  // Bool: true or false.
  // Int: [0, 100).
  // Float: [0.0, 1.0].
  // GeoPoint: { [0, 100), [0, 100) }.
  // Text: byte = ['0', '9'], length = [1, 4].
  // BoolVector: value = true or false, size = [0, 4].
  // IntVector: value = [0, 100), size = [0, 4].
  // FloatVector: value = [0.0, 1.0), size = [0, 4].
  // GeoPointVector: value = { [0, 100), [0, 100) }, size = [0, 4].
  // TextVector: byte = ['0', '9'], length = [1, 4], size = [0, 4].
  constexpr std::size_t NUM_ROWS = 1 << 16;
  constexpr std::size_t MIN_LENGTH = 1;
  constexpr std::size_t MAX_LENGTH = 4;
  constexpr std::size_t MAX_SIZE = 4;
  test.bool_values.resize(NUM_ROWS);
  test.bool2_values.resize(NUM_ROWS);
  test.int_values.resize(NUM_ROWS);
  test.int2_values.resize(NUM_ROWS);
  test.float_values.resize(NUM_ROWS);
  test.float2_values.resize(NUM_ROWS);
  test.geo_point_values.resize(NUM_ROWS);
  test.geo_point2_values.resize(NUM_ROWS);
  test.text_values.resize(NUM_ROWS);
  test.text2_values.resize(NUM_ROWS);
  test.text_bodies.resize(NUM_ROWS);
  test.text2_bodies.resize(NUM_ROWS);
  test.bool_vector_values.resize(NUM_ROWS);
  test.bool_vector2_values.resize(NUM_ROWS);
  test.bool_vector_bodies.resize(NUM_ROWS);
  test.bool_vector2_bodies.resize(NUM_ROWS);
  test.int_vector_values.resize(NUM_ROWS);
  test.int_vector2_values.resize(NUM_ROWS);
  test.int_vector_bodies.resize(NUM_ROWS);
  test.int_vector2_bodies.resize(NUM_ROWS);
  test.float_vector_values.resize(NUM_ROWS);
  test.float_vector2_values.resize(NUM_ROWS);
  test.float_vector_bodies.resize(NUM_ROWS);
  test.float_vector2_bodies.resize(NUM_ROWS);
  test.geo_point_vector_values.resize(NUM_ROWS);
  test.geo_point_vector2_values.resize(NUM_ROWS);
  test.geo_point_vector_bodies.resize(NUM_ROWS);
  test.geo_point_vector2_bodies.resize(NUM_ROWS);
  test.text_vector_values.resize(NUM_ROWS);
  test.text_vector2_values.resize(NUM_ROWS);
  test.text_vector_bodies.resize(NUM_ROWS);
  test.text_vector2_bodies.resize(NUM_ROWS);
  test.ref_values.resize(NUM_ROWS + 1);
  test.ref2_values.resize(NUM_ROWS + 1);
  test.ref_vector_values.resize(NUM_ROWS);
  test.ref_vector2_values.resize(NUM_ROWS);
  test.ref_vector_bodies.resize(NUM_ROWS);
  test.ref_vector2_bodies.resize(NUM_ROWS);

  for (std::size_t i = 0; i < NUM_ROWS; ++i) {
    test.bool_values.set(i, grnxx::Bool((mersenne_twister() & 1) != 0));
    test.bool2_values.set(i, grnxx::Bool((mersenne_twister() & 1) != 0));

    test.int_values.set(i, grnxx::Int(mersenne_twister() % 100));
    test.int2_values.set(i, grnxx::Int(mersenne_twister() % 100));

    constexpr auto MAX_VALUE = mersenne_twister.max();
    test.float_values.set(i, grnxx::Float(1.0 * mersenne_twister() /
                                          MAX_VALUE));
    test.float2_values.set(i, grnxx::Float(1.0 * mersenne_twister() /
                                           MAX_VALUE));

    test.geo_point_values.set(
        i, grnxx::GeoPoint(grnxx::Int(mersenne_twister() % 100),
                           grnxx::Int(mersenne_twister() % 100)));
    test.geo_point2_values.set(
        i, grnxx::GeoPoint(grnxx::Int(mersenne_twister() % 100),
                           grnxx::Int(mersenne_twister() % 100)));

    std::string *text_body = &test.text_bodies[i];
    generate_text(MIN_LENGTH, MAX_LENGTH, text_body);
    test.text_values.set(i, grnxx::Text(text_body->data(),
                                        text_body->length()));

    text_body = &test.text2_bodies[i];
    generate_text(MIN_LENGTH, MAX_LENGTH, text_body);
    test.text2_values.set(i, grnxx::Text(text_body->data(),
                             text_body->length()));

    size_t size = mersenne_twister() % (MAX_SIZE + 1);
    test.bool_vector_bodies[i].resize(size);
    for (size_t j = 0; j < size; ++j) {
      test.bool_vector_bodies[i][j] =
          grnxx::Bool((mersenne_twister() & 1) == 1);
    }
    test.bool_vector_values.set(
        i, grnxx::BoolVector(test.bool_vector_bodies[i].data(), size));
    size = mersenne_twister() % (MAX_SIZE + 1);
    test.bool_vector2_bodies[i].resize(size);
    for (size_t j = 0; j < size; ++j) {
      test.bool_vector2_bodies[i][j] =
          grnxx::Bool((mersenne_twister() & 1) == 1);
    }
    test.bool_vector2_values.set(
        i, grnxx::BoolVector(test.bool_vector2_bodies[i].data(), size));

    size = mersenne_twister() % (MAX_SIZE + 1);
    test.int_vector_bodies[i].resize(size);
    for (size_t j = 0; j < size; ++j) {
      test.int_vector_bodies[i][j] = grnxx::Int(mersenne_twister() % 100);
    }
    test.int_vector_values.set(
        i, grnxx::IntVector(test.int_vector_bodies[i].data(), size));
    size = mersenne_twister() % (MAX_SIZE + 1);
    test.int_vector2_bodies[i].resize(size);
    for (size_t j = 0; j < size; ++j) {
      test.int_vector2_bodies[i][j] = grnxx::Int(mersenne_twister() % 100);
    }
    test.int_vector2_values.set(
        i, grnxx::IntVector(test.int_vector2_bodies[i].data(), size));

    size = mersenne_twister() % (MAX_SIZE + 1);
    test.float_vector_bodies[i].resize(size);
    for (size_t j = 0; j < size; ++j) {
      test.float_vector_bodies[i][j] =
          grnxx::Float((mersenne_twister() % 100) / 100.0);
    }
    test.float_vector_values.set(
        i, grnxx::FloatVector(test.float_vector_bodies[i].data(), size));
    size = mersenne_twister() % (MAX_SIZE + 1);
    test.float_vector2_bodies[i].resize(size);
    for (size_t j = 0; j < size; ++j) {
      test.float_vector2_bodies[i][j] =
          grnxx::Float((mersenne_twister() % 100) / 100.0);
    }
    test.float_vector2_values.set(
        i, grnxx::FloatVector(test.float_vector2_bodies[i].data(), size));

    size = mersenne_twister() % (MAX_SIZE + 1);
    test.geo_point_vector_bodies[i].resize(size);
    for (size_t j = 0; j < size; ++j) {
      test.geo_point_vector_bodies[i][j] =
          grnxx::GeoPoint(grnxx::Int(mersenne_twister() % 100),
                          grnxx::Int(mersenne_twister() % 100));
    }
    const grnxx::GeoPoint *geo_point_data =
        test.geo_point_vector_bodies[i].data();
    test.geo_point_vector_values.set(
        i, grnxx::GeoPointVector(geo_point_data, size));
    size = mersenne_twister() % (MAX_SIZE + 1);
    test.geo_point_vector2_bodies[i].resize(size);
    for (size_t j = 0; j < size; ++j) {
      test.geo_point_vector2_bodies[i][j] =
          grnxx::GeoPoint(grnxx::Int(mersenne_twister() % 100),
                          grnxx::Int(mersenne_twister() % 100));
    }
    geo_point_data = test.geo_point_vector2_bodies[i].data();
    test.geo_point_vector2_values.set(
        i, grnxx::GeoPointVector(geo_point_data, size));

    size = mersenne_twister() % (MAX_SIZE + 1);
    test.text_vector_bodies[i].resize(size);
    for (size_t j = 0; j < size; ++j) {
      test.text_vector_bodies[i][j] =
          test.text_values[mersenne_twister() % NUM_ROWS];
    }
    test.text_vector_values.set(
        i, grnxx::TextVector(test.text_vector_bodies[i].data(), size));
    size = mersenne_twister() % (MAX_SIZE + 1);
    test.text_vector2_bodies[i].resize(size);
    for (size_t j = 0; j < size; ++j) {
      test.text_vector2_bodies[i][j] =
          test.text_values[mersenne_twister() % NUM_ROWS];
    }
    test.text_vector2_values.set(
        i, grnxx::TextVector(test.text_vector2_bodies[i].data(), size));

    test.ref_values.set(i, grnxx::Int(mersenne_twister() % NUM_ROWS));
    test.ref2_values.set(i, grnxx::Int(mersenne_twister() % NUM_ROWS));

    size = mersenne_twister() % (MAX_SIZE + 1);
    test.ref_vector_bodies[i].resize(size);
    for (size_t j = 0; j < size; ++j) {
      test.ref_vector_bodies[i][j] =
          grnxx::Int(mersenne_twister() % NUM_ROWS);
    }
    test.ref_vector_values.set(
        i, grnxx::IntVector(test.ref_vector_bodies[i].data(), size));
    size = mersenne_twister() % (MAX_SIZE + 1);
    test.ref_vector2_bodies[i].resize(size);
    for (size_t j = 0; j < size; ++j) {
      test.ref_vector2_bodies[i][j] =
          grnxx::Int(mersenne_twister() % NUM_ROWS);
    }
    test.ref_vector2_values.set(
        i, grnxx::IntVector(test.ref_vector2_bodies[i].data(), size));
  }

  // Store generated values into columns.
  for (std::size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = test.table->insert_row();
    assert(row_id.match(grnxx::Int(i)));

    bool_column->set(row_id, test.bool_values[i]);
    bool2_column->set(row_id, test.bool2_values[i]);
    int_column->set(row_id, test.int_values[i]);
    int2_column->set(row_id, test.int2_values[i]);
    float_column->set(row_id, test.float_values[i]);
    float2_column->set(row_id, test.float2_values[i]);
    geo_point_column->set(row_id, test.geo_point_values[i]);
    geo_point2_column->set(row_id, test.geo_point2_values[i]);
    text_column->set(row_id, test.text_values[i]);
    text2_column->set(row_id, test.text2_values[i]);
    bool_vector_column->set(row_id, test.bool_vector_values[i]);
    bool_vector2_column->set(row_id, test.bool_vector2_values[i]);
    int_vector_column->set(row_id, test.int_vector_values[i]);
    int_vector2_column->set(row_id, test.int_vector2_values[i]);
    float_vector_column->set(row_id, test.float_vector_values[i]);
    float_vector2_column->set(row_id, test.float_vector2_values[i]);
    geo_point_vector_column->set(row_id, test.geo_point_vector_values[i]);
    geo_point_vector2_column->set(row_id, test.geo_point_vector2_values[i]);
    text_vector_column->set(row_id, test.text_vector_values[i]);
    text_vector2_column->set(row_id, test.text_vector2_values[i]);
  }

  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id(i);
    ref_column->set(row_id, test.ref_values[i]);
    ref2_column->set(row_id, test.ref2_values[i]);
    ref_vector_column->set(row_id, test.ref_vector_values[i]);
    ref_vector2_column->set(row_id, test.ref_vector2_values[i]);
  }
} catch (const char *msg) {
  std::cout << msg << std::endl;
  throw;
}

grnxx::Array<grnxx::Record> create_input_records() try {
  auto cursor = test.table->create_cursor();

  grnxx::Array<grnxx::Record> records;
  size_t count = cursor->read_all(&records);
  assert(count == test.table->num_rows());

  return records;
} catch (const char *msg) {
  std::cout << msg << std::endl;
  throw;
}

void test_constant() try {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (true).
  builder->push_constant(grnxx::Bool(true));
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Bool> bool_results;
  expression->evaluate(records, &bool_results);
  assert(bool_results.size() == test.table->num_rows());
  for (size_t i = 0; i < bool_results.size(); ++i) {
    assert(bool_results[i].is_true());
  }

  expression->filter(&records);
  assert(records.size() == test.table->num_rows());

  // Test an expression (false).
  builder->push_constant(grnxx::Bool(false));
  expression = builder->release();

  bool_results.clear();
  expression->evaluate(records, &bool_results);
  assert(bool_results.size() == test.table->num_rows());
  for (size_t i = 0; i < bool_results.size(); ++i) {
    assert(bool_results[i].is_false());
  }

  expression->filter(&records);
  assert(records.size() == 0);

  // Test an expression (100).
  builder->push_constant(grnxx::Int(100));
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::Int> int_results;
  expression->evaluate(records, &int_results);
  assert(int_results.size() == test.table->num_rows());
  for (size_t i = 0; i < int_results.size(); ++i) {
    assert(int_results[i].raw() == 100);
  }

  // Test an expression (1.25).
  builder->push_constant(grnxx::Float(1.25));
  expression = builder->release();

  grnxx::Array<grnxx::Float> float_results;
  expression->evaluate(records, &float_results);
  assert(float_results.size() == test.table->num_rows());
  for (size_t i = 0; i < float_results.size(); ++i) {
    assert(float_results[i].raw() == 1.25);
  }

  expression->adjust(&records);
  assert(records.size() == test.table->num_rows());
  for (size_t i = 0; i < records.size(); ++i) {
    assert(records[i].score.raw() == 1.25);
  }

  // Test an expression ({ 123, 456 }).
  grnxx::GeoPoint geo_point(grnxx::Int(123), grnxx::Int(456));
  builder->push_constant(geo_point);
  expression = builder->release();

  grnxx::Array<grnxx::GeoPoint> geo_point_results;
  expression->evaluate(records, &geo_point_results);
  assert(geo_point_results.size() == test.table->num_rows());
  for (size_t i = 0; i < geo_point_results.size(); ++i) {
    assert(geo_point_results[i].match(geo_point));
  }

  // Test an expression ("ABC").
  builder->push_constant(grnxx::Text("ABC"));
  expression = builder->release();

  grnxx::Array<grnxx::Text> text_results;
  expression->evaluate(records, &text_results);
  assert(text_results.size() == test.table->num_rows());
  for (size_t i = 0; i < text_results.size(); ++i) {
    assert(text_results[i].match(grnxx::Text("ABC")));
  }

  // Test an expression ({ true, false, true }).
  grnxx::Bool bool_values[] = {
    grnxx::Bool(true),
    grnxx::Bool(false),
    grnxx::Bool(true)
  };
  grnxx::BoolVector bool_vector(bool_values, 3);
  builder->push_constant(bool_vector);
  expression = builder->release();

  grnxx::Array<grnxx::BoolVector> bool_vector_results;
  expression->evaluate(records, &bool_vector_results);
  assert(bool_vector_results.size() == test.table->num_rows());
  for (size_t i = 0; i < bool_vector_results.size(); ++i) {
    assert(bool_vector_results[i].match(bool_vector));
  }

  // Test an expression ({ 123, -456, 789 }).
  grnxx::Int int_values[] = {
    grnxx::Int(123),
    grnxx::Int(-456),
    grnxx::Int(789)
  };
  grnxx::IntVector int_vector(int_values, 3);
  builder->push_constant(int_vector);
  expression = builder->release();

  grnxx::Array<grnxx::IntVector> int_vector_results;
  expression->evaluate(records, &int_vector_results);
  assert(int_vector_results.size() == test.table->num_rows());
  for (size_t i = 0; i < int_vector_results.size(); ++i) {
    assert(int_vector_results[i].match(int_vector));
  }

  // Test an expression ({ 1.25, -4.5, 6.75 }).
  grnxx::Float float_values[] = {
    grnxx::Float(1.25),
    grnxx::Float(-4.5),
    grnxx::Float(6.75)
  };
  grnxx::FloatVector float_vector(float_values, 3);
  builder->push_constant(float_vector);
  expression = builder->release();

  grnxx::Array<grnxx::FloatVector> float_vector_results;
  expression->evaluate(records, &float_vector_results);
  assert(float_vector_results.size() == test.table->num_rows());
  for (size_t i = 0; i < float_vector_results.size(); ++i) {
    assert(float_vector_results[i].match(float_vector));
  }

  // Test an expression ({ Sapporo, Tokyo, Osaka }).
  grnxx::GeoPoint geo_point_values[] = {
    { grnxx::Float(43.068661), grnxx::Float(141.350755) },  // Sapporo.
    { grnxx::Float(35.681382), grnxx::Float(139.766084) },  // Tokyo.
    { grnxx::Float(34.702485), grnxx::Float(135.495951) },  // Osaka.
  };
  grnxx::GeoPointVector geo_point_vector(geo_point_values, 3);
  builder->push_constant(geo_point_vector);
  expression = builder->release();

  grnxx::Array<grnxx::GeoPointVector> geo_point_vector_results;
  expression->evaluate(records, &geo_point_vector_results);
  assert(geo_point_vector_results.size() == test.table->num_rows());
  for (size_t i = 0; i < geo_point_vector_results.size(); ++i) {
    assert(geo_point_vector_results[i].match(geo_point_vector));
  }

  // Test an expression ({ "abc", "DEF", "ghi" }).
  grnxx::Text text_values[] = {
    grnxx::Text("abc"),
    grnxx::Text("DEF"),
    grnxx::Text("ghi")
  };
  grnxx::TextVector text_vector(text_values, 3);
  builder->push_constant(text_vector);
  expression = builder->release();

  grnxx::Array<grnxx::TextVector> text_vector_results;
  expression->evaluate(records, &text_vector_results);
  assert(text_vector_results.size() == test.table->num_rows());
  for (size_t i = 0; i < text_vector_results.size(); ++i) {
    assert(text_vector_results[i].match(text_vector));
  }
} catch (const char *msg) {
  std::cout << msg << std::endl;
  throw;
}

void test_row_id() try {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);
  // Test an expression (_id).
  builder->push_row_id();
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Int> id_results;
  expression->evaluate(records, &id_results);
  assert(id_results.size() == records.size());
  for (size_t i = 0; i < id_results.size(); ++i) {
    assert(id_results[i].match(records[i].row_id));
  }
} catch (const char *msg) {
  std::cout << msg << std::endl;
  throw;
}

void test_score() try {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (_score).
  builder->push_score();
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Float> score_results;
  expression->evaluate(records, &score_results);
  assert(score_results.size() == records.size());
  for (size_t i = 0; i < score_results.size(); ++i) {
    assert(score_results[i].match(records[i].score));
  }

  expression->adjust(&records);
  assert(records.size() == test.table->num_rows());
  for (size_t i = 0; i < records.size(); ++i) {
    assert(records[i].score.raw() == 0.0);
  }
} catch (const char *msg) {
  std::cout << msg << std::endl;
  throw;
}

void test_column() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (Bool).
  builder->push_column("Bool");
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Bool> bool_results;
  expression->evaluate(records, &bool_results);
  assert(bool_results.size() == test.table->num_rows());
  for (size_t i = 0; i < bool_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(bool_results[i].match(test.bool_values[row_id]));
  }

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 0; i < test.bool_values.size(); ++i) {
    if (test.bool_values[i].is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (Int).
  builder->push_column("Int");
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::Int> int_results;
  expression->evaluate(records, &int_results);
  assert(int_results.size() == test.table->num_rows());
  for (size_t i = 0; i < int_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(int_results[i].match(test.int_values[row_id]));
  }

  // Test an expression (Float).
  builder->push_column("Float");
  expression = builder->release();

  grnxx::Array<grnxx::Float> float_results;
  expression->evaluate(records, &float_results);
  assert(float_results.size() == test.table->num_rows());
  for (size_t i = 0; i < float_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(float_results[i].match(test.float_values[row_id]));
  }

  expression->adjust(&records);
  assert(records.size() == test.table->num_rows());
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(records[i].score.match(test.float_values[row_id]));
  }

  // Test an expression (GeoPoint).
  builder->push_column("GeoPoint");
  expression = builder->release();

  grnxx::Array<grnxx::GeoPoint> geo_point_results;
  expression->evaluate(records, &geo_point_results);
  assert(geo_point_results.size() == test.table->num_rows());
  for (size_t i = 0; i < geo_point_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(geo_point_results[i].match(test.geo_point_values[row_id]));
  }

  // Test an expression (Text).
  builder->push_column("Text");
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::Text> text_results;
  expression->evaluate(records, &text_results);
  assert(text_results.size() == test.table->num_rows());
  for (size_t i = 0; i < text_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(text_results[i].match(test.text_values[row_id]));
  }

  // Test an expression (BoolVector).
  builder->push_column("BoolVector");
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::BoolVector> bool_vector_results;
  expression->evaluate(records, &bool_vector_results);
  assert(bool_vector_results.size() == test.table->num_rows());
  for (size_t i = 0; i < bool_vector_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(bool_vector_results[i].match(test.bool_vector_values[row_id]));
  }

  // Test an expression (IntVector).
  builder->push_column("IntVector");
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::IntVector> int_vector_results;
  expression->evaluate(records, &int_vector_results);
  assert(int_vector_results.size() == test.table->num_rows());
  for (size_t i = 0; i < int_vector_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(int_vector_results[i].match(test.int_vector_values[row_id]));
  }

  // Test an expression (FloatVector).
  builder->push_column("FloatVector");
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::FloatVector> float_vector_results;
  expression->evaluate(records, &float_vector_results);
  assert(float_vector_results.size() == test.table->num_rows());
  for (size_t i = 0; i < float_vector_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(float_vector_results[i].match(test.float_vector_values[row_id]));
  }

  // Test an expression (GeoPointVector).
  builder->push_column("GeoPointVector");
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::GeoPointVector> geo_point_vector_results;
  expression->evaluate(records, &geo_point_vector_results);
  assert(geo_point_vector_results.size() == test.table->num_rows());
  for (size_t i = 0; i < geo_point_vector_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(geo_point_vector_results[i].match(
           test.geo_point_vector_values[row_id]));
  }

  // Test an expression (TextVector).
  builder->push_column("TextVector");
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::TextVector> text_vector_results;
  expression->evaluate(records, &text_vector_results);
  assert(text_vector_results.size() == test.table->num_rows());
  for (size_t i = 0; i < text_vector_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(text_vector_results[i].match(test.text_vector_values[row_id]));
  }

  // Test an expression (Ref).
  builder->push_column("Ref");
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::Int> ref_results;
  expression->evaluate(records, &ref_results);
  assert(ref_results.size() == test.table->num_rows());
  for (size_t i = 0; i < ref_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(ref_results[i].match(test.ref_values[row_id]));
  }

  // Test an expression (RefVector).
  builder->push_column("RefVector");
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::IntVector> ref_vector_results;
  expression->evaluate(records, &ref_vector_results);
  assert(ref_vector_results.size() == test.table->num_rows());
  for (size_t i = 0; i < ref_vector_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(ref_vector_results[i].match(test.ref_vector_values[row_id]));
  }
}

void test_logical_not() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (!Bool).
  builder->push_column("Bool");
  builder->push_operator(grnxx::LOGICAL_NOT_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Bool> bool_results;
  expression->evaluate(records, &bool_results);
  assert(bool_results.size() == test.table->num_rows());
  for (size_t i = 0; i < bool_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(bool_results[i].match(!test.bool_values[row_id]));
  }

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 0; i < test.bool_values.size(); ++i) {
    if ((!test.bool_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);
}

void test_bitwise_not() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (~Bool).
  builder->push_column("Bool");
  builder->push_operator(grnxx::BITWISE_NOT_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Bool> bool_results;
  expression->evaluate(records, &bool_results);
  assert(bool_results.size() == test.table->num_rows());
  for (size_t i = 0; i < bool_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(bool_results[i].match(~test.bool_values[row_id]));
  }

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 0; i < test.bool_values.size(); ++i) {
    if ((~test.bool_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (~Int).
  builder->push_column("Int");
  builder->push_operator(grnxx::BITWISE_NOT_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::Int> int_results;
  expression->evaluate(records, &int_results);
  assert(int_results.size() == test.table->num_rows());
  for (size_t i = 0; i < int_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(int_results[i].match(~test.int_values[row_id]));
  }
}

void test_positive() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (+Int).
  builder->push_column("Int");
  builder->push_operator(grnxx::POSITIVE_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Int> int_results;
  expression->evaluate(records, &int_results);
  assert(int_results.size() == test.table->num_rows());
  for (size_t i = 0; i < int_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(int_results[i].match(test.int_values[row_id]));
  }

  // Test an expression (+Float).
  builder->push_column("Float");
  builder->push_operator(grnxx::POSITIVE_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::Float> float_results;
  expression->evaluate(records, &float_results);
  assert(float_results.size() == test.table->num_rows());
  for (size_t i = 0; i < float_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(float_results[i].match(test.float_values[row_id]));
  }

  expression->adjust(&records);
  assert(records.size() == test.table->num_rows());
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(records[i].score.match(test.float_values[row_id]));
  }
}

void test_negative() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (-Int).
  builder->push_column("Int");
  builder->push_operator(grnxx::NEGATIVE_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Int> int_results;
  expression->evaluate(records, &int_results);
  assert(int_results.size() == test.table->num_rows());
  for (size_t i = 0; i < int_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(int_results[i].match(-test.int_values[row_id]));
  }

  // Test an expression (-Float).
  builder->push_column("Float");
  builder->push_operator(grnxx::NEGATIVE_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::Float> float_results;
  expression->evaluate(records, &float_results);
  assert(float_results.size() == test.table->num_rows());
  for (size_t i = 0; i < float_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(float_results[i].match(-test.float_values[row_id]));
  }

  expression->adjust(&records);
  assert(records.size() == test.table->num_rows());
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(records[i].score.match(-test.float_values[row_id]));
  }
}

void test_to_int() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (Int(Float)).
  builder->push_column("Float");
  builder->push_operator(grnxx::TO_INT_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Int> int_results;
  expression->evaluate(records, &int_results);
  assert(int_results.size() == test.table->num_rows());
  for (size_t i = 0; i < int_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(int_results[i].match(test.float_values[row_id].to_int()));
  }
}

void test_to_float() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);
  assert(builder);

  // Test an expression (Float(Int)).
  builder->push_column("Int");
  builder->push_operator(grnxx::TO_FLOAT_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Float> float_results;
  expression->evaluate(records, &float_results);
  assert(float_results.size() == test.table->num_rows());
  for (size_t i = 0; i < float_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(float_results[i].match(test.int_values[row_id].to_float()));
  }

  expression->adjust(&records);
  assert(records.size() == test.table->num_rows());
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(records[i].score.match(test.int_values[row_id].to_float()));
  }
}

void test_logical_and() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (Bool && Bool2).
  builder->push_column("Bool");
  builder->push_column("Bool2");
  builder->push_operator(grnxx::LOGICAL_AND_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Bool> bool_results;
  expression->evaluate(records, &bool_results);
  assert(bool_results.size() == test.table->num_rows());
  for (size_t i = 0; i < bool_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(bool_results[i].match(test.bool_values[row_id] &
                                 test.bool2_values[row_id]));
  }

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 0; i < test.bool_values.size(); ++i) {
    if ((test.bool_values[i] & test.bool2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);
}

void test_logical_or() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (Bool || Bool2).
  builder->push_column("Bool");
  builder->push_column("Bool2");
  builder->push_operator(grnxx::LOGICAL_OR_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Bool> bool_results;
  expression->evaluate(records, &bool_results);
  assert(bool_results.size() == test.table->num_rows());
  for (size_t i = 0; i < bool_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(bool_results[i].match(test.bool_values[row_id] |
                                 test.bool2_values[row_id]));
  }

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 0; i < test.bool_values.size(); ++i) {
    if ((test.bool_values[i] | test.bool2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);
}

void test_equal() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (Bool == Bool2).
  builder->push_column("Bool");
  builder->push_column("Bool2");
  builder->push_operator(grnxx::EQUAL_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Bool> results;
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.bool_values[row_id] ==
                            test.bool2_values[row_id]));
  }

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 0; i < test.bool_values.size(); ++i) {
    if ((test.bool_values[i] == test.bool2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (Int == Int2).
  builder->push_column("Int");
  builder->push_column("Int2");
  builder->push_operator(grnxx::EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.int_values[row_id] ==
                            test.int2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.int_values.size(); ++i) {
    if ((test.int_values[i] == test.int2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (Float == Float2).
  builder->push_column("Float");
  builder->push_column("Float2");
  builder->push_operator(grnxx::EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.float_values[row_id] ==
                            test.float2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.float_values.size(); ++i) {
    if ((test.float_values[i] == test.float2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (GeoPoint == GeoPoint2).
  builder->push_column("GeoPoint");
  builder->push_column("GeoPoint2");
  builder->push_operator(grnxx::EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.geo_point_values[row_id] ==
                            test.geo_point2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.geo_point_values.size(); ++i) {
    if ((test.geo_point_values[i] == test.geo_point2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (Text == Text2).
  builder->push_column("Text");
  builder->push_column("Text2");
  builder->push_operator(grnxx::EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.text_values[row_id] ==
                            test.text2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.text_values.size(); ++i) {
    if ((test.text_values[i] == test.text2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (BoolVector == BoolVector2).
  builder->push_column("BoolVector");
  builder->push_column("BoolVector2");
  builder->push_operator(grnxx::EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.bool_vector_values[row_id] ==
                            test.bool_vector2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.bool_vector_values.size(); ++i) {
    if ((test.bool_vector_values[i] ==
         test.bool_vector2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (IntVector == IntVector2).
  builder->push_column("IntVector");
  builder->push_column("IntVector2");
  builder->push_operator(grnxx::EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.int_vector_values[row_id] ==
                            test.int_vector2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.int_vector_values.size(); ++i) {
    if ((test.int_vector_values[i] == test.int_vector2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (FloatVector == FloatVector2).
  builder->push_column("FloatVector");
  builder->push_column("FloatVector2");
  builder->push_operator(grnxx::EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.float_vector_values[row_id] ==
                            test.float_vector2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.float_vector_values.size(); ++i) {
    if ((test.float_vector_values[i] ==
         test.float_vector2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (GeoPointVector == GeoPointVector2).
  builder->push_column("GeoPointVector");
  builder->push_column("GeoPointVector2");
  builder->push_operator(grnxx::EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.geo_point_vector_values[row_id] ==
                            test.geo_point_vector2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.geo_point_vector_values.size(); ++i) {
    if ((test.geo_point_vector_values[i] ==
         test.geo_point_vector2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (TextVector == TextVector2).
  builder->push_column("TextVector");
  builder->push_column("TextVector2");
  builder->push_operator(grnxx::EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.text_vector_values[row_id] ==
                            test.text_vector2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.text_vector_values.size(); ++i) {
    if ((test.text_vector_values[i] ==
         test.text_vector2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);
}

void test_not_equal() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (Bool != Bool2).
  builder->push_column("Bool");
  builder->push_column("Bool2");
  builder->push_operator(grnxx::NOT_EQUAL_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Bool> results;
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.bool_values[row_id] !=
                            test.bool2_values[row_id]));
  }

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 0; i < test.bool_values.size(); ++i) {
    if ((test.bool_values[i] != test.bool2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (Int != Int2).
  builder->push_column("Int");
  builder->push_column("Int2");
  builder->push_operator(grnxx::NOT_EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.int_values[row_id] !=
                            test.int2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.int_values.size(); ++i) {
    if ((test.int_values[i] != test.int2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (Float != Float2).
  builder->push_column("Float");
  builder->push_column("Float2");
  builder->push_operator(grnxx::NOT_EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.float_values[row_id] !=
                            test.float2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.float_values.size(); ++i) {
    if ((test.float_values[i] != test.float2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (GeoPoint != GeoPoint2).
  builder->push_column("GeoPoint");
  builder->push_column("GeoPoint2");
  builder->push_operator(grnxx::NOT_EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.geo_point_values[row_id] !=
                            test.geo_point2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.geo_point_values.size(); ++i) {
    if ((test.geo_point_values[i] != test.geo_point2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (Text != Text2).
  builder->push_column("Text");
  builder->push_column("Text2");
  builder->push_operator(grnxx::NOT_EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.text_values[row_id] !=
                            test.text2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.text_values.size(); ++i) {
    if ((test.text_values[i] != test.text2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (BoolVector != BoolVector2).
  builder->push_column("BoolVector");
  builder->push_column("BoolVector2");
  builder->push_operator(grnxx::NOT_EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.bool_vector_values[row_id] !=
                            test.bool_vector2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.bool_vector_values.size(); ++i) {
    if ((test.bool_vector_values[i] !=
         test.bool_vector2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (IntVector != IntVector2).
  builder->push_column("IntVector");
  builder->push_column("IntVector2");
  builder->push_operator(grnxx::NOT_EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.int_vector_values[row_id] !=
                            test.int_vector2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.int_vector_values.size(); ++i) {
    if ((test.int_vector_values[i] != test.int_vector2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (FloatVector != FloatVector2).
  builder->push_column("FloatVector");
  builder->push_column("FloatVector2");
  builder->push_operator(grnxx::NOT_EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.float_vector_values[row_id] !=
                            test.float_vector2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.float_vector_values.size(); ++i) {
    if ((test.float_vector_values[i] !=
         test.float_vector2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (GeoPointVector != GeoPointVector2).
  builder->push_column("GeoPointVector");
  builder->push_column("GeoPointVector2");
  builder->push_operator(grnxx::NOT_EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.geo_point_vector_values[row_id] !=
                            test.geo_point_vector2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.geo_point_vector_values.size(); ++i) {
    if ((test.geo_point_vector_values[i] !=
         test.geo_point_vector2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (TextVector != TextVector2).
  builder->push_column("TextVector");
  builder->push_column("TextVector2");
  builder->push_operator(grnxx::NOT_EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.text_vector_values[row_id] !=
                            test.text_vector2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.text_vector_values.size(); ++i) {
    if ((test.text_vector_values[i] !=
         test.text_vector2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);
}

void test_less() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (Int < Int2).
  builder->push_column("Int");
  builder->push_column("Int2");
  builder->push_operator(grnxx::LESS_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Bool> results;
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.int_values[row_id] <
                            test.int2_values[row_id]));
  }

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 0; i < test.int_values.size(); ++i) {
    if ((test.int_values[i] < test.int2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (Float < Float2).
  builder->push_column("Float");
  builder->push_column("Float2");
  builder->push_operator(grnxx::LESS_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.float_values[row_id] <
                            test.float2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.float_values.size(); ++i) {
    if ((test.float_values[i] < test.float2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (Text < Text2).
  builder->push_column("Text");
  builder->push_column("Text2");
  builder->push_operator(grnxx::LESS_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.text_values[row_id] <
                            test.text2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.text_values.size(); ++i) {
    if ((test.text_values[i] < test.text2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);
}

void test_less_equal() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (Int <= Int2).
  builder->push_column("Int");
  builder->push_column("Int2");
  builder->push_operator(grnxx::LESS_EQUAL_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Bool> results;
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.int_values[row_id] <=
                            test.int2_values[row_id]));
  }

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 0; i < test.int_values.size(); ++i) {
    if ((test.int_values[i] <= test.int2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (Float <= Float2).
  builder->push_column("Float");
  builder->push_column("Float2");
  builder->push_operator(grnxx::LESS_EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.float_values[row_id] <=
                            test.float2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.float_values.size(); ++i) {
    if ((test.float_values[i] <= test.float2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (Text <= Text2).
  builder->push_column("Text");
  builder->push_column("Text2");
  builder->push_operator(grnxx::LESS_EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.text_values[row_id] <=
                            test.text2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.text_values.size(); ++i) {
    if ((test.text_values[i] <= test.text2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);
}

void test_greater() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (Int > Int2).
  builder->push_column("Int");
  builder->push_column("Int2");
  builder->push_operator(grnxx::GREATER_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Bool> results;
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.int_values[row_id] >
                            test.int2_values[row_id]));
  }

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 0; i < test.int_values.size(); ++i) {
    if ((test.int_values[i] > test.int2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (Float > Float2).
  builder->push_column("Float");
  builder->push_column("Float2");
  builder->push_operator(grnxx::GREATER_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.float_values[row_id] >
                            test.float2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.float_values.size(); ++i) {
    if ((test.float_values[i] > test.float2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (Text > Text2).
  builder->push_column("Text");
  builder->push_column("Text2");
  builder->push_operator(grnxx::GREATER_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.text_values[row_id] >
                            test.text2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.text_values.size(); ++i) {
    if ((test.text_values[i] > test.text2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);
}

void test_greater_equal() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (Int >= Int2).
  builder->push_column("Int");
  builder->push_column("Int2");
  builder->push_operator(grnxx::GREATER_EQUAL_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Bool> results;
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.int_values[row_id] >=
                            test.int2_values[row_id]));
  }

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 0; i < test.int_values.size(); ++i) {
    if ((test.int_values[i] >= test.int2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (Float >= Float2).
  builder->push_column("Float");
  builder->push_column("Float2");
  builder->push_operator(grnxx::GREATER_EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.float_values[row_id] >=
                            test.float2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.float_values.size(); ++i) {
    if ((test.float_values[i] >= test.float2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (Text >= Text2).
  builder->push_column("Text");
  builder->push_column("Text2");
  builder->push_operator(grnxx::GREATER_EQUAL_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  results.clear();
  expression->evaluate(records, &results);
  assert(results.size() == test.table->num_rows());
  for (size_t i = 0; i < results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(test.text_values[row_id] >=
                            test.text2_values[row_id]));
  }

  expression->filter(&records);
  count = 0;
  for (size_t i = 0; i < test.text_values.size(); ++i) {
    if ((test.text_values[i] >= test.text2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);
}

void test_bitwise_and() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (Bool & Bool2).
  builder->push_column("Bool");
  builder->push_column("Bool2");
  builder->push_operator(grnxx::BITWISE_AND_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Bool> bool_results;
  expression->evaluate(records, &bool_results);
  assert(bool_results.size() == test.table->num_rows());
  for (size_t i = 0; i < bool_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(bool_results[i].match(test.bool_values[row_id] &
                                 test.bool2_values[row_id]));
  }

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 0; i < test.bool_values.size(); ++i) {
    if ((test.bool_values[i] & test.bool2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (Int & Int2).
  builder->push_column("Int");
  builder->push_column("Int2");
  builder->push_operator(grnxx::BITWISE_AND_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::Int> int_results;
  expression->evaluate(records, &int_results);
  assert(int_results.size() == test.table->num_rows());
  for (size_t i = 0; i < int_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(int_results[i].match(test.int_values[row_id] &
                                test.int2_values[row_id]));
  }
}

void test_bitwise_or() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (Bool | Bool2).
  builder->push_column("Bool");
  builder->push_column("Bool2");
  builder->push_operator(grnxx::BITWISE_OR_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Bool> bool_results;
  expression->evaluate(records, &bool_results);
  assert(bool_results.size() == test.table->num_rows());
  for (size_t i = 0; i < bool_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(bool_results[i].match(test.bool_values[row_id] |
                                 test.bool2_values[row_id]));
  }

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 0; i < test.bool_values.size(); ++i) {
    if ((test.bool_values[i] | test.bool2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (Int | Int2).
  builder->push_column("Int");
  builder->push_column("Int2");
  builder->push_operator(grnxx::BITWISE_OR_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::Int> int_results;
  expression->evaluate(records, &int_results);
  assert(int_results.size() == test.table->num_rows());
  for (size_t i = 0; i < int_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(int_results[i].match(test.int_values[row_id] |
                                test.int2_values[row_id]));
  }
}

void test_bitwise_xor() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (Bool ^ Bool2).
  builder->push_column("Bool");
  builder->push_column("Bool2");
  builder->push_operator(grnxx::BITWISE_XOR_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Bool> bool_results;
  expression->evaluate(records, &bool_results);
  assert(bool_results.size() == test.table->num_rows());
  for (size_t i = 0; i < bool_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(bool_results[i].match(test.bool_values[row_id] ^
                                 test.bool2_values[row_id]));
  }

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 0; i < test.bool_values.size(); ++i) {
    if ((test.bool_values[i] ^ test.bool2_values[i]).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (Int ^ Int2).
  builder->push_column("Int");
  builder->push_column("Int2");
  builder->push_operator(grnxx::BITWISE_XOR_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::Int> int_results;
  expression->evaluate(records, &int_results);
  assert(int_results.size() == test.table->num_rows());
  for (size_t i = 0; i < int_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(int_results[i].match(test.int_values[row_id] ^
                                test.int2_values[row_id]));
  }
}

void test_plus() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (Int + Int2).
  builder->push_column("Int");
  builder->push_column("Int2");
  builder->push_operator(grnxx::PLUS_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Int> int_results;
  expression->evaluate(records, &int_results);
  assert(int_results.size() == test.table->num_rows());
  for (size_t i = 0; i < int_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(int_results[i].match(test.int_values[row_id] +
                                test.int2_values[row_id]));
  }

  // Test an expression (Float + Float2).
  builder->push_column("Float");
  builder->push_column("Float2");
  builder->push_operator(grnxx::PLUS_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::Float> float_results;
  expression->evaluate(records, &float_results);
  assert(float_results.size() == test.table->num_rows());
  for (size_t i = 0; i < float_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(float_results[i].match(test.float_values[row_id] +
                                  test.float2_values[row_id]));
  }

  expression->adjust(&records);
  assert(records.size() == test.table->num_rows());
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(records[i].score.match(test.float_values[row_id] +
                                  test.float2_values[row_id]));
  }
}

void test_minus() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (Int - Int2).
  builder->push_column("Int");
  builder->push_column("Int2");
  builder->push_operator(grnxx::MINUS_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Int> int_results;
  expression->evaluate(records, &int_results);
  assert(int_results.size() == test.table->num_rows());
  for (size_t i = 0; i < int_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(int_results[i].match(test.int_values[row_id] -
                                test.int2_values[row_id]));
  }

  // Test an expression (Float - Float2).
  builder->push_column("Float");
  builder->push_column("Float2");
  builder->push_operator(grnxx::MINUS_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::Float> float_results;
  expression->evaluate(records, &float_results);
  assert(float_results.size() == test.table->num_rows());
  for (size_t i = 0; i < float_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(float_results[i].match(test.float_values[row_id] -
                                  test.float2_values[row_id]));
  }

  expression->adjust(&records);
  assert(records.size() == test.table->num_rows());
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(records[i].score.match(test.float_values[row_id] -
                                  test.float2_values[row_id]));
  }
}

void test_multiplication() {

  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (Int * Int2).
  builder->push_column("Int");
  builder->push_column("Int2");
  builder->push_operator(grnxx::MULTIPLICATION_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Int> int_results;
  expression->evaluate(records, &int_results);
  assert(int_results.size() == test.table->num_rows());
  for (size_t i = 0; i < int_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(int_results[i].match(test.int_values[row_id] *
                                test.int2_values[row_id]));
  }

  // Test an expression (Float * Float2).
  builder->push_column("Float");
  builder->push_column("Float2");
  builder->push_operator(grnxx::MULTIPLICATION_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::Float> float_results;
  expression->evaluate(records, &float_results);
  assert(float_results.size() == test.table->num_rows());
  for (size_t i = 0; i < float_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(float_results[i].match(test.float_values[row_id] *
                                  test.float2_values[row_id]));
  }

  expression->adjust(&records);
  assert(records.size() == test.table->num_rows());
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(records[i].score.match(test.float_values[row_id] *
                                  test.float2_values[row_id]));
  }
}

void test_division() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (Int / Int2).
  // Division by zero does not fail.
  builder->push_column("Int");
  builder->push_column("Int2");
  builder->push_operator(grnxx::DIVISION_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Int> int_results;
  expression->evaluate(records, &int_results);
  assert(int_results.size() == test.table->num_rows());
  for (size_t i = 0; i < int_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(int_results[i].match(test.int_values[row_id] /
                                test.int2_values[row_id]));
  }

  // Test an expression (Float / Float2).
  builder->push_column("Float");
  builder->push_column("Float2");
  builder->push_operator(grnxx::DIVISION_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::Float> float_results;
  expression->evaluate(records, &float_results);
  assert(float_results.size() == test.table->num_rows());
  for (size_t i = 0; i < float_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(float_results[i].match(test.float_values[row_id] /
                                  test.float2_values[row_id]));
  }

  expression->adjust(&records);
  assert(records.size() == test.table->num_rows());
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(records[i].score.match(test.float_values[row_id] /
                                  test.float2_values[row_id]));
  }
}

void test_modulus() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (Int % Int2).
  // An error occurs because of division by zero.
  builder->push_column("Int");
  builder->push_column("Int2");
  builder->push_operator(grnxx::MODULUS_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Int> int_results;
  expression->evaluate(records, &int_results);
  assert(int_results.size() == test.table->num_rows());
  for (size_t i = 0; i < int_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(int_results[i].match(test.int_values[row_id] %
                                test.int2_values[row_id]));
  }

  // Test an expression (Float % Float2).
  builder->push_column("Float");
  builder->push_column("Float2");
  builder->push_operator(grnxx::MODULUS_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::Float> float_results;
  expression->evaluate(records, &float_results);
  assert(float_results.size() == test.table->num_rows());
  for (size_t i = 0; i < float_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(float_results[i].match(test.float_values[row_id] %
                                  test.float2_values[row_id]));
  }

  expression->adjust(&records);
  assert(records.size() == test.table->num_rows());
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(records[i].score.match(test.float_values[row_id] %
                                  test.float2_values[row_id]));
  }
}

void test_subscript() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (BoolVector[Int]).
  builder->push_column("BoolVector");
  builder->push_column("Int");
  builder->push_operator(grnxx::SUBSCRIPT_OPERATOR);
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Bool> bool_results;
  expression->evaluate(records, &bool_results);
  assert(bool_results.size() == test.table->num_rows());
  for (size_t i = 0; i < bool_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    const auto int_value = test.int_values[row_id];
    const auto &bool_vector_value = test.bool_vector_values[row_id];
    assert(bool_results[i].match(bool_vector_value[int_value]));
  }

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 0; i < test.int_values.size(); ++i) {
    const auto int_value = test.int_values[i];
    const auto &bool_vector_value = test.bool_vector_values[i];
    if (bool_vector_value[int_value].is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (IntVector[Int]).
  builder->push_column("IntVector");
  builder->push_column("Int");
  builder->push_operator(grnxx::SUBSCRIPT_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::Int> int_results;
  expression->evaluate(records, &int_results);
  assert(int_results.size() == test.table->num_rows());
  for (size_t i = 0; i < int_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    const auto int_value = test.int_values[row_id];
    const auto &int_vector_value = test.int_vector_values[row_id];
    assert(int_results[i].match(int_vector_value[int_value]));
  }

  // Test an expression (FloatVector[Int]).
  builder->push_column("FloatVector");
  builder->push_column("Int");
  builder->push_operator(grnxx::SUBSCRIPT_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::Float> float_results;
  expression->evaluate(records, &float_results);
  assert(float_results.size() == test.table->num_rows());
  for (size_t i = 0; i < float_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    const auto int_value = test.int_values[row_id];
    const auto &float_vector_value = test.float_vector_values[row_id];
    assert(float_results[i].match(float_vector_value[int_value]));
  }

  expression->adjust(&records);
  assert(records.size() == test.table->num_rows());
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    const auto int_value = test.int_values[row_id];
    const auto &float_vector_value = test.float_vector_values[row_id];
    assert(records[i].score.match(float_vector_value[int_value]));
  }

  // Test an expression (GeoPointVector[Int]).
  builder->push_column("GeoPointVector");
  builder->push_column("Int");
  builder->push_operator(grnxx::SUBSCRIPT_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::GeoPoint> geo_point_results;
  expression->evaluate(records, &geo_point_results);
  assert(geo_point_results.size() == test.table->num_rows());
  for (size_t i = 0; i < geo_point_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    const auto int_value = test.int_values[row_id];
    const auto &geo_point_vector_value = test.geo_point_vector_values[row_id];
    assert(geo_point_results[i].match(geo_point_vector_value[int_value]));
  }

  // Test an expression (TextVector[Int]).
  builder->push_column("TextVector");
  builder->push_column("Int");
  builder->push_operator(grnxx::SUBSCRIPT_OPERATOR);
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::Text> text_results;
  expression->evaluate(records, &text_results);
  assert(text_results.size() == test.table->num_rows());
  for (size_t i = 0; i < text_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    const auto int_value = test.int_values[row_id];
    const auto &text_vector_value = test.text_vector_values[row_id];
    assert(text_results[i].match(text_vector_value[int_value]));
  }
}

void test_subexpression() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (Ref.Bool).
  builder->push_column("Ref");
  builder->begin_subexpression();
  builder->push_column("Bool");
  builder->end_subexpression();
  auto expression = builder->release();

  auto records = create_input_records();

  grnxx::Array<grnxx::Bool> bool_results;
  expression->evaluate(records, &bool_results);
  assert(bool_results.size() == test.table->num_rows());
  for (size_t i = 0; i < bool_results.size(); ++i) {
    const auto ref_value = test.ref_values[i];
    const auto bool_value = test.bool_values[ref_value.raw()];
    assert(bool_results[i].match(bool_value));
  }

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 0; i < test.ref_values.size(); ++i) {
    const auto ref_value = test.ref_values[i];
    const auto bool_value = test.bool_values[ref_value.raw()];
    if (bool_value.is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);

  // Test an expression (Ref.Float).
  builder->push_column("Ref");
  builder->begin_subexpression();
  builder->push_column("Float");
  builder->end_subexpression();
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::Float> float_results;
  expression->evaluate(records, &float_results);
  assert(float_results.size() == test.table->num_rows());
  for (size_t i = 0; i < float_results.size(); ++i) {
    const auto ref_value = test.ref_values[i];
    const auto float_value = test.float_values[ref_value.raw()];
    assert(float_results[i].match(float_value));
  }

  expression->adjust(&records);
  for (size_t i = 0; i < float_results.size(); ++i) {
    const auto ref_value = test.ref_values[i];
    const auto float_value = test.float_values[ref_value.raw()];
    assert(records[i].score.match(float_value));
  }

  // Test an expression (Ref.IntVector).
  builder->push_column("Ref");
  builder->begin_subexpression();
  builder->push_column("IntVector");
  builder->end_subexpression();
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::IntVector> int_vector_results;
  expression->evaluate(records, &int_vector_results);
  assert(int_vector_results.size() == test.table->num_rows());
  for (size_t i = 0; i < int_vector_results.size(); ++i) {
    const auto ref_value = test.ref_values[i];
    const auto int_vector_value = test.int_vector_values[ref_value.raw()];
    assert(int_vector_results[i].match(int_vector_value));
  }

  // Test an expression (Ref.(Ref.Text)).
  builder->push_column("Ref");
  builder->begin_subexpression();
  builder->push_column("Ref");
  builder->begin_subexpression();
  builder->push_column("Text");
  builder->end_subexpression();
  builder->end_subexpression();
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::Text> text_results;
  expression->evaluate(records, &text_results);
  assert(text_results.size() == test.table->num_rows());
  for (size_t i = 0; i < text_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    const auto ref_value = test.ref_values[row_id];
    const auto ref_ref_value = test.ref_values[ref_value.raw()];
    const auto text_value = test.text_values[ref_ref_value.raw()];
    assert(text_results[i].match(text_value));
  }

  // Test an expression ((Ref.Ref).Int).
  builder->push_column("Ref");
  builder->begin_subexpression();
  builder->push_column("Ref");
  builder->end_subexpression();
  builder->begin_subexpression();
  builder->push_column("Int");
  builder->end_subexpression();
  expression = builder->release();

  records = create_input_records();

  grnxx::Array<grnxx::Int> int_results;
  expression->evaluate(records, &int_results);
  assert(int_results.size() == test.table->num_rows());
  for (size_t i = 0; i < int_results.size(); ++i) {
    const auto ref_value = test.ref_values[i];
    const auto ref_ref_value = test.ref_values[ref_value.raw()];
    const auto int_value = test.int_values[ref_ref_value.raw()];
    assert(int_results[i].match(int_value));
  }

  // Test an expression (RefVector.Int).
  builder->push_column("RefVector");
  builder->begin_subexpression();
  builder->push_column("Int");
  builder->end_subexpression();
  expression = builder->release();

  records = create_input_records();

  int_vector_results.clear();
  expression->evaluate(records, &int_vector_results);
  assert(int_vector_results.size() == test.table->num_rows());
  for (size_t i = 0; i < int_vector_results.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    const auto ref_vector_value = test.ref_vector_values[row_id];
    assert(int_vector_results[i].size().match(ref_vector_value.size()));
    size_t value_size = ref_vector_value.raw_size();
    for (size_t j = 0; j < value_size; ++j) {
      grnxx::Int ref_value = ref_vector_value[j];
      const auto int_value = test.int_values[ref_value.raw()];
      assert(int_vector_results[i][j].match(int_value));
    }
  }
}

void test_sequential_filter() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression ((Int + Int2) < 100).
  builder->push_column("Int");
  builder->push_column("Int2");
  builder->push_operator(grnxx::PLUS_OPERATOR);
  builder->push_constant(grnxx::Int(100));
  builder->push_operator(grnxx::LESS_OPERATOR);
  auto expression = builder->release();

  auto cursor = test.table->create_cursor();

  // Read and filter records block by block.
  grnxx::Array<grnxx::Record> records;
  size_t offset = 0;
  for ( ; ; ) {
    size_t count = cursor->read(1024, &records);
    assert((offset + count) == records.size());
    if (count == 0) {
      break;
    }
    expression->filter(&records, offset);
    offset = records.size();
  }

  size_t count = 0;
  for (size_t i = 0; i < test.bool_values.size(); ++i) {
    grnxx::Int sum = test.int_values[i] + test.int2_values[i];
    if ((sum < grnxx::Int(100)).is_true()) {
      assert(records[count].row_id.match(grnxx::Int(i)));
      ++count;
    }
  }
  assert(records.size() == count);
}

void test_sequential_adjust() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (Float(Int) + Float).
  builder->push_column("Int");
  builder->push_operator(grnxx::TO_FLOAT_OPERATOR);
  builder->push_column("Float");
  builder->push_operator(grnxx::PLUS_OPERATOR);
  auto expression = builder->release();

  auto cursor = test.table->create_cursor();

  // Read and adjust records block by block.
  grnxx::Array<grnxx::Record> records;
  size_t offset = 0;
  for ( ; ; ) {
    size_t count = cursor->read(1024, &records);
    assert((offset + count) == records.size());
    if (count == 0) {
      break;
    }
    expression->adjust(&records, offset);
    offset += count;
  }

  assert(records.size() == test.table->num_rows());
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(records[i].score.match(test.int_values[row_id].to_float() +
                                  test.float_values[row_id]));
  }
}

void test_sequential_evaluate() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression (Int + Int(Float * 100.0)).
  builder->push_column("Int");
  builder->push_column("Float");
  builder->push_constant(grnxx::Float(100.0));
  builder->push_operator(grnxx::MULTIPLICATION_OPERATOR);
  builder->push_operator(grnxx::TO_INT_OPERATOR);
  builder->push_operator(grnxx::PLUS_OPERATOR);
  auto expression = builder->release();

  auto cursor = test.table->create_cursor();

  // Read and evaluate records block by block.
  grnxx::Array<grnxx::Record> records;
  grnxx::Array<grnxx::Int> results;
  size_t offset = 0;
  for ( ; ; ) {
    size_t count = cursor->read(1024, &records);
    assert((offset + count) == records.size());
    if (count == 0) {
      break;
    }
    results.resize(offset + count);
    expression->evaluate(records.cref(offset), results.ref(offset));
    offset += count;
  }

  assert(records.size() == test.table->num_rows());
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(results[i].match(
           (test.int_values[row_id] +
            (test.float_values[row_id] * grnxx::Float(100.0)).to_int())));
  }
}

void test_partial_filter() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an expression ((Float * Float2) > 0.25).
  builder->push_column("Float");
  builder->push_column("Float2");
  builder->push_operator(grnxx::MULTIPLICATION_OPERATOR);
  builder->push_constant(grnxx::Float(0.25));
  builder->push_operator(grnxx::GREATER_OPERATOR);
  auto expression = builder->release();

  // Read all records.
  auto records = create_input_records();

  // Extract a part of true records.
  constexpr size_t OFFSET = 12345;
  constexpr size_t LIMIT = 5000;
  expression->filter(&records, 0, OFFSET, LIMIT);
  assert(records.size() == 5000);
  size_t count = 0;
  for (size_t i = 1; i < test.bool_values.size(); ++i) {
    grnxx::Float product = test.float_values[i] * test.float2_values[i];
    if ((product > grnxx::Float(0.25)).is_true()) {
      if ((count >= OFFSET) && (count < (OFFSET + LIMIT))) {
        assert(records[count - OFFSET].row_id.match(grnxx::Int(i)));
      }
      ++count;
    }
  }
}

void test_error() {
  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(test.table);

  // Test an invalid expression (Int * Text).
  try {
    builder->push_column("Int");
    builder->push_column("Text");
    builder->push_operator(grnxx::MULTIPLICATION_OPERATOR);
    assert(false);
  } catch (...) {
    // OK.
  }

  // Clear the broken builder.
  builder->clear();

  // Test a valid expression (Int + Int).
  builder->push_column("Int");
  builder->push_column("Int");
  builder->push_operator(grnxx::PLUS_OPERATOR);
}

int main() {
  init_test();

  // Data.
  test_constant();
  test_row_id();
  test_score();
  test_column();

  // Unary operators.
  test_logical_not();
  test_bitwise_not();
  test_positive();
  test_negative();
  test_to_int();
  test_to_float();

  // Binary operators.
  test_logical_and();
  test_logical_or();
  test_equal();
  test_not_equal();
  test_less();
  test_less_equal();
  test_greater();
  test_greater_equal();
  test_bitwise_and();
  test_bitwise_or();
  test_bitwise_xor();
  test_plus();
  test_minus();
  test_multiplication();
  test_division();
  test_modulus();
  test_subscript();

  // Subexpression.
  test_subexpression();

  // Test sequential operations.
  test_sequential_filter();
  test_sequential_adjust();
  test_sequential_evaluate();

  // Test partial filtering.
  test_partial_filter();

  // Test error.
  test_error();

  return 0;
}
