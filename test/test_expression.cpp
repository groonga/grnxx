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
//  grnxx::Array<grnxx::Text> text_values;
//  grnxx::Array<grnxx::Text> text2_values;
//  grnxx::Array<std::string> text_bodies;
//  grnxx::Array<std::string> text2_bodies;
//  grnxx::Array<grnxx::BoolVector> bool_vector_values;
//  grnxx::Array<grnxx::BoolVector> bool_vector2_values;
//  grnxx::Array<grnxx::IntVector> int_vector_values;
//  grnxx::Array<grnxx::IntVector> int_vector2_values;
//  grnxx::Array<grnxx::Array<grnxx::Int>> int_vector_bodies;
//  grnxx::Array<grnxx::Array<grnxx::Int>> int_vector2_bodies;
//  grnxx::Array<grnxx::FloatVector> float_vector_values;
//  grnxx::Array<grnxx::FloatVector> float_vector2_values;
//  grnxx::Array<grnxx::Array<grnxx::Float>> float_vector_bodies;
//  grnxx::Array<grnxx::Array<grnxx::Float>> float_vector2_bodies;
//  grnxx::Array<grnxx::GeoPointVector> geo_point_vector_values;
//  grnxx::Array<grnxx::GeoPointVector> geo_point_vector2_values;
//  grnxx::Array<grnxx::Array<grnxx::GeoPoint>> geo_point_vector_bodies;
//  grnxx::Array<grnxx::Array<grnxx::GeoPoint>> geo_point_vector2_bodies;
//  grnxx::Array<grnxx::TextVector> text_vector_values;
//  grnxx::Array<grnxx::TextVector> text_vector2_values;
//  grnxx::Array<grnxx::Array<grnxx::Text>> text_vector_bodies;
//  grnxx::Array<grnxx::Array<grnxx::Text>> text_vector2_bodies;
//  grnxx::Array<grnxx::Int> ref_values;
//  grnxx::Array<grnxx::Int> ref2_values;
//  grnxx::Array<grnxx::IntVector> ref_vector_values;
//  grnxx::Array<grnxx::IntVector> ref_vector2_values;
//  grnxx::Array<grnxx::Array<grnxx::Int>> ref_vector_bodies;
//  grnxx::Array<grnxx::Array<grnxx::Int>> ref_vector2_bodies;
} test;

//void generate_text(grnxx::Int min_size, grnxx::Int max_size,
//                   std::string *str) {
//  grnxx::Int size = (mersenne_twister() % (max_size - min_size + 1)) + min_size;
//  str->resize(size);
//  for (grnxx::Int i = 0; i < size; ++i) {
//    (*str)[i] = '0' + (mersenne_twister() % 10);
//  }
//}

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

//  data_type = grnxx::TEXT_DATA;
//  auto text_column = test.table->create_column(&error, "Text", data_type);
//  auto text2_column = test.table->create_column(&error, "Text2", data_type);
//  assert(text_column);
//  assert(text2_column);

//  data_type = grnxx::BOOL_VECTOR_DATA;
//  auto bool_vector_column =
//      test.table->create_column(&error, "BoolVector", data_type);
//  auto bool_vector2_column =
//      test.table->create_column(&error, "BoolVector2", data_type);
//  assert(bool_vector_column);
//  assert(bool_vector2_column);

//  data_type = grnxx::INT_VECTOR_DATA;
//  auto int_vector_column =
//      test.table->create_column(&error, "IntVector", data_type);
//  auto int_vector2_column =
//      test.table->create_column(&error, "IntVector2", data_type);
//  assert(int_vector_column);
//  assert(int_vector2_column);

//  data_type = grnxx::FLOAT_VECTOR_DATA;
//  auto float_vector_column =
//      test.table->create_column(&error, "FloatVector", data_type);
//  auto float_vector2_column =
//      test.table->create_column(&error, "FloatVector2", data_type);
//  assert(float_vector_column);
//  assert(float_vector2_column);

//  data_type = grnxx::GEO_POINT_VECTOR_DATA;
//  auto geo_point_vector_column =
//      test.table->create_column(&error, "GeoPointVector", data_type);
//  auto geo_point_vector2_column =
//      test.table->create_column(&error, "GeoPointVector2", data_type);
//  assert(geo_point_vector_column);
//  assert(geo_point_vector2_column);

//  data_type = grnxx::TEXT_VECTOR_DATA;
//  auto text_vector_column =
//      test.table->create_column(&error, "TextVector", data_type);
//  auto text_vector2_column =
//      test.table->create_column(&error, "TextVector2", data_type);
//  assert(text_vector_column);
//  assert(text_vector2_column);

//  data_type = grnxx::INT_DATA;
//  grnxx::ColumnOptions options;
//  options.ref_table_name = "Table";
//  auto ref_column =
//      test.table->create_column(&error, "Ref", data_type, options);
//  auto ref2_column =
//      test.table->create_column(&error, "Ref2", data_type, options);
//  assert(ref_column);
//  assert(ref2_column);

//  data_type = grnxx::INT_VECTOR_DATA;
//  auto ref_vector_column =
//      test.table->create_column(&error, "RefVector", data_type, options);
//  auto ref_vector2_column =
//      test.table->create_column(&error, "RefVector2", data_type, options);
//  assert(ref_vector_column);
//  assert(ref_vector2_column);

  // Generate random values.
  // Bool: true or false.
  // Int: [0, 100).
  // Float: [0.0, 1.0].
  // GeoPoint: { [0, 100), [0, 100) }.
  // Text: byte = ['0', '9'], length = [1, 4].
  // BoolVector: value = true or false, size = [0, 58].
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
//  assert(test.text_values.resize(&error, NUM_ROWS + 1));
//  assert(test.text2_values.resize(&error, NUM_ROWS + 1));
//  assert(test.text_bodies.resize(&error, NUM_ROWS + 1));
//  assert(test.text2_bodies.resize(&error, NUM_ROWS + 1));
//  assert(test.bool_vector_values.resize(&error, NUM_ROWS + 1));
//  assert(test.bool_vector2_values.resize(&error, NUM_ROWS + 1));
//  assert(test.int_vector_values.resize(&error, NUM_ROWS + 1));
//  assert(test.int_vector2_values.resize(&error, NUM_ROWS + 1));
//  assert(test.int_vector_bodies.resize(&error, NUM_ROWS + 1));
//  assert(test.int_vector2_bodies.resize(&error, NUM_ROWS + 1));
//  assert(test.float_vector_values.resize(&error, NUM_ROWS + 1));
//  assert(test.float_vector2_values.resize(&error, NUM_ROWS + 1));
//  assert(test.float_vector_bodies.resize(&error, NUM_ROWS + 1));
//  assert(test.float_vector2_bodies.resize(&error, NUM_ROWS + 1));
//  assert(test.geo_point_vector_values.resize(&error, NUM_ROWS + 1));
//  assert(test.geo_point_vector2_values.resize(&error, NUM_ROWS + 1));
//  assert(test.geo_point_vector_bodies.resize(&error, NUM_ROWS + 1));
//  assert(test.geo_point_vector2_bodies.resize(&error, NUM_ROWS + 1));
//  assert(test.text_vector_values.resize(&error, NUM_ROWS + 1));
//  assert(test.text_vector2_values.resize(&error, NUM_ROWS + 1));
//  assert(test.text_vector_bodies.resize(&error, NUM_ROWS + 1));
//  assert(test.text_vector2_bodies.resize(&error, NUM_ROWS + 1));
//  assert(test.ref_values.resize(&error, NUM_ROWS + 1));
//  assert(test.ref2_values.resize(&error, NUM_ROWS + 1));
//  assert(test.ref_vector_values.resize(&error, NUM_ROWS + 1));
//  assert(test.ref_vector2_values.resize(&error, NUM_ROWS + 1));
//  assert(test.ref_vector_bodies.resize(&error, NUM_ROWS + 1));
//  assert(test.ref_vector2_bodies.resize(&error, NUM_ROWS + 1));

  for (std::size_t i = 0; i < NUM_ROWS; ++i) {
    test.bool_values.set(i, grnxx::Bool((mersenne_twister() & 1) != 0));
    test.bool2_values.set(i, grnxx::Bool((mersenne_twister() & 1) != 0));

    test.int_values.set(i, grnxx::Int(mersenne_twister() % 100));
    test.int2_values.set(i, grnxx::Int(mersenne_twister() % 100));

    constexpr auto MAX_VALUE = mersenne_twister.max();
    test.float_values.set(i, grnxx::Float(1.0 * mersenne_twister() / MAX_VALUE));
    test.float2_values.set(i, grnxx::Float(1.0 * mersenne_twister() / MAX_VALUE));

    test.geo_point_values.set(
        i, grnxx::GeoPoint(grnxx::Int(mersenne_twister() % 100),
                           grnxx::Int(mersenne_twister() % 100)));
    test.geo_point2_values.set(
        i, grnxx::GeoPoint(grnxx::Int(mersenne_twister() % 100),
                           grnxx::Int(mersenne_twister() % 100)));

//    std::string *text_body = &test.text_bodies[i];
//    generate_text(MIN_LENGTH, MAX_LENGTH, text_body);
//    test.text_values.set(i, grnxx::Text(text_body->data(), text_body->length()));

//    text_body = &test.text2_bodies[i];
//    generate_text(MIN_LENGTH, MAX_LENGTH, text_body);
//    test.text2_values.set(i, grnxx::Text(text_body->data(), text_body->length()));

//    grnxx::uint64_t bits = mersenne_twister();
//    grnxx::Int size = mersenne_twister() % 59;
//    test.bool_vector_values.set(i, grnxx::BoolVector(bits, size));
//    bits = mersenne_twister();
//    size = mersenne_twister() % 59;
//    test.bool_vector2_values.set(i, grnxx::BoolVector(bits, size));

//    size = mersenne_twister() % (MAX_SIZE + 1);
//    assert(test.int_vector_bodies[i].resize(&error, size));
//    for (grnxx::Int j = 0; j < size; ++j) {
//      test.int_vector_bodies[i][j] = mersenne_twister() % 100;
//    }
//    test.int_vector_values.set(
//        i, grnxx::IntVector(test.int_vector_bodies[i].data(), size));
//    size = mersenne_twister() % (MAX_SIZE + 1);
//    assert(test.int_vector2_bodies[i].resize(&error, size));
//    for (grnxx::Int j = 0; j < size; ++j) {
//      test.int_vector2_bodies[i][j] = mersenne_twister() % 100;
//    }
//    test.int_vector2_values.set(
//        i, grnxx::IntVector(test.int_vector2_bodies[i].data(), size));

//    size = mersenne_twister() % (MAX_SIZE + 1);
//    assert(test.float_vector_bodies[i].resize(&error, size));
//    for (grnxx::Int j = 0; j < size; ++j) {
//      test.float_vector_bodies[i][j] = (mersenne_twister() % 100) / 100.0;
//    }
//    test.float_vector_values.set(
//        i, grnxx::FloatVector(test.float_vector_bodies[i].data(), size));
//    size = mersenne_twister() % (MAX_SIZE + 1);
//    assert(test.float_vector2_bodies[i].resize(&error, size));
//    for (grnxx::Int j = 0; j < size; ++j) {
//      test.float_vector2_bodies[i][j] = (mersenne_twister() % 100) / 100.0;
//    }
//    test.float_vector2_values.set(
//        i, grnxx::FloatVector(test.float_vector2_bodies[i].data(), size));

//    size = mersenne_twister() % (MAX_SIZE + 1);
//    assert(test.geo_point_vector_bodies[i].resize(&error, size));
//    for (grnxx::Int j = 0; j < size; ++j) {
//      test.geo_point_vector_bodies[i][j] =
//          grnxx::GeoPoint(mersenne_twister() % 100, mersenne_twister() % 100);
//    }
//    test.geo_point_vector_values.set(
//        i, grnxx::GeoPointVector(test.geo_point_vector_bodies[i].data(), size));
//    size = mersenne_twister() % (MAX_SIZE + 1);
//    assert(test.geo_point_vector2_bodies[i].resize(&error, size));
//    for (grnxx::Int j = 0; j < size; ++j) {
//      test.geo_point_vector2_bodies[i][j] =
//          grnxx::GeoPoint(mersenne_twister() % 100, mersenne_twister() % 100);
//    }
//    test.geo_point_vector2_values.set(
//        i, grnxx::GeoPointVector(test.geo_point_vector2_bodies[i].data(), size));

//    size = mersenne_twister() % (MAX_SIZE + 1);
//    assert(test.text_vector_bodies[i].resize(&error, size));
//    for (grnxx::Int j = 0; j < size; ++j) {
//      test.text_vector_bodies[i][j] =
//          test.text_values[1 + (mersenne_twister() % NUM_ROWS)];
//    }
//    test.text_vector_values.set(
//        i, grnxx::TextVector(test.text_vector_bodies[i].data(), size));
//    size = mersenne_twister() % (MAX_SIZE + 1);
//    assert(test.text_vector2_bodies[i].resize(&error, size));
//    for (grnxx::Int j = 0; j < size; ++j) {
//      test.text_vector2_bodies[i][j] =
//          test.text_values[1 + (mersenne_twister() % NUM_ROWS)];
//    }
//    test.text_vector2_values.set(
//        i, grnxx::TextVector(test.text_vector2_bodies[i].data(), size));

//    test.ref_values.set(i, 1 + (mersenne_twister() % NUM_ROWS));
//    test.ref2_values.set(i, 1 + (mersenne_twister() % NUM_ROWS));

//    size = mersenne_twister() % (MAX_SIZE + 1);
//    assert(test.ref_vector_bodies[i].resize(&error, size));
//    for (grnxx::Int j = 0; j < size; ++j) {
//      test.ref_vector_bodies[i][j] = 1 + (mersenne_twister() % NUM_ROWS);
//    }
//    test.ref_vector_values.set(
//        i, grnxx::IntVector(test.ref_vector_bodies[i].data(), size));
//    size = mersenne_twister() % (MAX_SIZE + 1);
//    assert(test.ref_vector2_bodies[i].resize(&error, size));
//    for (grnxx::Int j = 0; j < size; ++j) {
//      test.ref_vector2_bodies[i][j] = 1 + (mersenne_twister() % NUM_ROWS);
//    }
//    test.ref_vector2_values.set(
//        i, grnxx::IntVector(test.ref_vector2_bodies[i].data(), size));
  }

  // Store generated values into columns.
  for (std::size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = test.table->insert_row();
    assert(row_id.value() == grnxx::Int(i).value());

    bool_column->set(row_id, test.bool_values[i]);
    bool2_column->set(row_id, test.bool2_values[i]);
    int_column->set(row_id, test.int_values[i]);
    int2_column->set(row_id, test.int2_values[i]);
    float_column->set(row_id, test.float_values[i]);
    float2_column->set(row_id, test.float2_values[i]);
    geo_point_column->set(row_id, test.geo_point_values[i]);
    geo_point2_column->set(row_id, test.geo_point2_values[i]);
//    assert(text_column->set(&error, row_id, test.text_values[i]));
//    assert(text2_column->set(&error, row_id, test.text2_values[i]));
//    assert(bool_vector_column->set(&error, row_id,
//                                   test.bool_vector_values[i]));
//    assert(bool_vector2_column->set(&error, row_id,
//                                    test.bool_vector2_values[i]));
//    assert(int_vector_column->set(&error, row_id,
//                                  test.int_vector_values[i]));
//    assert(int_vector2_column->set(&error, row_id,
//                                   test.int_vector2_values[i]));
//    assert(float_vector_column->set(&error, row_id,
//                                    test.float_vector_values[i]));
//    assert(float_vector2_column->set(&error, row_id,
//                                     test.float_vector2_values[i]));
//    assert(geo_point_vector_column->set(&error, row_id,
//                                        test.geo_point_vector_values[i]));
//    assert(geo_point_vector2_column->set(&error, row_id,
//                                         test.geo_point_vector2_values[i]));
//    assert(text_vector_column->set(&error, row_id,
//                                   test.text_vector_values[i]));
//    assert(text_vector2_column->set(&error, row_id,
//                                    test.text_vector2_values[i]));
  }

//  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
//    assert(ref_column->set(&error, i, test.ref_values[i]));
//    assert(ref2_column->set(&error, i, test.ref2_values[i]));
//    assert(ref_vector_column->set(&error, i, test.ref_vector_values[i]));
//    assert(ref_vector2_column->set(&error, i, test.ref_vector2_values[i]));
//  }
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
    assert(int_results[i].value() == 100);
  }

  // Test an expression (1.25).
  builder->push_constant(grnxx::Float(1.25));
  expression = builder->release();

  grnxx::Array<grnxx::Float> float_results;
  expression->evaluate(records, &float_results);
  assert(float_results.size() == test.table->num_rows());
  for (size_t i = 0; i < float_results.size(); ++i) {
    assert(float_results[i].value() == 1.25);
  }

  expression->adjust(&records);
  assert(records.size() == test.table->num_rows());
  for (size_t i = 0; i < records.size(); ++i) {
    assert(records[i].score.value() == 1.25);
  }

  // Test an expression ({ 123, 456 }).
  grnxx::GeoPoint geo_point(grnxx::Int(123), grnxx::Int(456));
  builder->push_constant(geo_point);
  expression = builder->release();

  grnxx::Array<grnxx::GeoPoint> geo_point_results;
  expression->evaluate(records, &geo_point_results);
  assert(geo_point_results.size() == test.table->num_rows());
  for (size_t i = 0; i < geo_point_results.size(); ++i) {
    assert(geo_point_results[i].latitude() == 123);
    assert(geo_point_results[i].longitude() == 456);
  }

  // Test an expression ("ABC").
  builder->push_constant(grnxx::Text("ABC"));
  expression = builder->release();

  grnxx::Array<grnxx::Text> text_results;
  expression->evaluate(records, &text_results);
  assert(text_results.size() == test.table->num_rows());
  for (size_t i = 0; i < text_results.size(); ++i) {
//    assert(text_results[i] == grnxx::Text("ABC"));
  }

//  // Test an expression ({ true, false, true }).
//  assert(builder->push_constant(
//      &error, grnxx::BoolVector({ true, false, true })));
//  expression = builder->release(&error);
//  assert(expression);

//  grnxx::Array<grnxx::BoolVector> bool_vector_results;
//  assert(expression->evaluate(&error, records, &bool_vector_results));
//  assert(bool_vector_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < bool_vector_results.size(); ++i) {
//    assert(bool_vector_results[i] ==
//           grnxx::BoolVector({ true, false, true }));
//  }

//  // Test an expression ({ 123, -456, 789 }).
//  grnxx::Int int_values[] = { 123, -456, 789 };
//  assert(builder->push_constant(&error, grnxx::IntVector(int_values, 3)));
//  expression = builder->release(&error);
//  assert(expression);

//  grnxx::Array<grnxx::IntVector> int_vector_results;
//  assert(expression->evaluate(&error, records, &int_vector_results));
//  assert(int_vector_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < int_vector_results.size(); ++i) {
//    assert(int_vector_results[i] == grnxx::IntVector(int_values, 3));
//  }

//  // Test an expression ({ 1.25, -4.5, 6.75 }).
//  grnxx::Float float_values[] = { 1.25, -4.5, 6.75 };
//  assert(builder->push_constant(&error, grnxx::FloatVector(float_values, 3)));
//  expression = builder->release(&error);
//  assert(expression);

//  grnxx::Array<grnxx::FloatVector> float_vector_results;
//  assert(expression->evaluate(&error, records, &float_vector_results));
//  assert(float_vector_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < float_vector_results.size(); ++i) {
//    assert(float_vector_results[i] == grnxx::FloatVector(float_values, 3));
//  }

//  // Test an expression ({{ 123, 456 }, { 789, 123 }, { 456, 789 }}).
//  grnxx::GeoPoint geo_point_values[] = {
//    { 123, 456 }, { 789, 123 }, { 456, 789 }
//  };
//  assert(builder->push_constant(
//      &error, grnxx::GeoPointVector(geo_point_values, 3)));
//  expression = builder->release(&error);
//  assert(expression);

//  grnxx::Array<grnxx::GeoPointVector> geo_point_vector_results;
//  assert(expression->evaluate(&error, records, &geo_point_vector_results));
//  assert(geo_point_vector_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < geo_point_vector_results.size(); ++i) {
//    assert(geo_point_vector_results[i] ==
//           grnxx::GeoPointVector(geo_point_values, 3));
//  }

//  // Test an expression ({ "abc", "DEF", "ghi" }).
//  grnxx::Text text_values[] = { "abc", "DEF", "ghi" };
//  assert(builder->push_constant(&error, grnxx::TextVector(text_values, 3)));
//  expression = builder->release(&error);
//  assert(expression);

//  grnxx::Array<grnxx::TextVector> text_vector_results;
//  assert(expression->evaluate(&error, records, &text_vector_results));
//  assert(text_vector_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < text_vector_results.size(); ++i) {
//    assert(text_vector_results[i] == grnxx::TextVector(text_values, 3));
//  }
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
    assert(id_results[i].value() == records[i].row_id.value());
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
    assert(score_results[i].value() == records[i].score.value());
  }

  expression->adjust(&records);
  assert(records.size() == test.table->num_rows());
  for (size_t i = 0; i < records.size(); ++i) {
    assert(records[i].score.value() == 0.0);
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
    size_t row_id = records[i].row_id.value();
    assert(bool_results[i].value()== test.bool_values[row_id].value());
  }

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 1; i < test.bool_values.size(); ++i) {
    if (test.bool_values[i].is_true()) {
      assert(records[count].row_id.value() == grnxx::Int(i).value());
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
    size_t row_id = records[i].row_id.value();
    assert(int_results[i].value() == test.int_values[row_id].value());
  }

  // Test an expression (Float).
  builder->push_column("Float");
  expression = builder->release();

  grnxx::Array<grnxx::Float> float_results;
  expression->evaluate(records, &float_results);
  assert(float_results.size() == test.table->num_rows());
  for (size_t i = 0; i < float_results.size(); ++i) {
    size_t row_id = records[i].row_id.value();
    assert(float_results[i].value() == test.float_values[row_id].value());
  }

  expression->adjust(&records);
  assert(records.size() == test.table->num_rows());
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.value();
    assert(records[i].score.value() == test.float_values[row_id].value());
  }

  // Test an expression (GeoPoint).
  builder->push_column("GeoPoint");
  expression = builder->release();

  grnxx::Array<grnxx::GeoPoint> geo_point_results;
  expression->evaluate(records, &geo_point_results);
  assert(geo_point_results.size() == test.table->num_rows());
  for (size_t i = 0; i < geo_point_results.size(); ++i) {
    size_t row_id = records[i].row_id.value();
    assert(geo_point_results[i].latitude() ==
           test.geo_point_values[row_id].latitude());
    assert(geo_point_results[i].longitude() ==
           test.geo_point_values[row_id].longitude());
  }

//  // Test an expression (Text).
//  assert(builder->push_column(&error, "Text"));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::Text> text_results;
//  assert(expression->evaluate(&error, records, &text_results));
//  assert(text_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < text_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(text_results[i] == test.text_values[row_id]);
//  }

//  // Test an expression (BoolVector).
//  assert(builder->push_column(&error, "BoolVector"));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::BoolVector> bool_vector_results;
//  assert(expression->evaluate(&error, records, &bool_vector_results));
//  assert(bool_vector_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < bool_vector_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(bool_vector_results[i] == test.bool_vector_values[row_id]);
//  }

//  // Test an expression (IntVector).
//  assert(builder->push_column(&error, "IntVector"));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::IntVector> int_vector_results;
//  assert(expression->evaluate(&error, records, &int_vector_results));
//  assert(int_vector_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < int_vector_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(int_vector_results[i] == test.int_vector_values[row_id]);
//  }

//  // Test an expression (FloatVector).
//  assert(builder->push_column(&error, "FloatVector"));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::FloatVector> float_vector_results;
//  assert(expression->evaluate(&error, records, &float_vector_results));
//  assert(float_vector_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < float_vector_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(float_vector_results[i] == test.float_vector_values[row_id]);
//  }

//  // Test an expression (GeoPointVector).
//  assert(builder->push_column(&error, "GeoPointVector"));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::GeoPointVector> geo_point_vector_results;
//  assert(expression->evaluate(&error, records, &geo_point_vector_results));
//  assert(geo_point_vector_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < geo_point_vector_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(geo_point_vector_results[i] == test.geo_point_vector_values[row_id]);
//  }

//  // Test an expression (TextVector).
//  assert(builder->push_column(&error, "TextVector"));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::TextVector> text_vector_results;
//  assert(expression->evaluate(&error, records, &text_vector_results));
//  assert(text_vector_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < text_vector_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(text_vector_results[i] == test.text_vector_values[row_id]);
//  }

//  // Test an expression (Ref).
//  assert(builder->push_column(&error, "Ref"));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::Int> ref_results;
//  assert(expression->evaluate(&error, records, &ref_results));
//  assert(ref_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < ref_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(ref_results[i] == test.ref_values[row_id]);
//  }

//  // Test an expression (RefVector).
//  assert(builder->push_column(&error, "RefVector"));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::IntVector> ref_vector_results;
//  assert(expression->evaluate(&error, records, &ref_vector_results));
//  assert(ref_vector_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < ref_vector_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(ref_vector_results[i] == test.ref_vector_values[row_id]);
//  }
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
    size_t row_id = records[i].row_id.value();
    assert(bool_results[i].value() ==
           (!test.bool_values[row_id]).value());
  }

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 0; i < test.bool_values.size(); ++i) {
    if ((!test.bool_values[i]).is_true()) {
      assert(records[count].row_id.value() == grnxx::Int(i).value());
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
    size_t row_id = records[i].row_id.value();
    assert(bool_results[i].value() == (~test.bool_values[row_id]).value());
  }

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 0; i < test.bool_values.size(); ++i) {
    if ((~test.bool_values[i]).is_true()) {
      assert(records[count].row_id.value() == grnxx::Int(i).value());
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
    size_t row_id = records[i].row_id.value();
    assert(int_results[i].value() == (~test.int_values[row_id]).value());
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
    size_t row_id = records[i].row_id.value();
    assert(int_results[i].value() == test.int_values[row_id].value());
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
    size_t row_id = records[i].row_id.value();
    assert(float_results[i].value() == test.float_values[row_id].value());
  }

  expression->adjust(&records);
  assert(records.size() == test.table->num_rows());
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.value();
    assert(records[i].score.value() == test.float_values[row_id].value());
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
    size_t row_id = records[i].row_id.value();
    assert(int_results[i].value() == (-test.int_values[row_id]).value());
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
    size_t row_id = records[i].row_id.value();
    assert(float_results[i].value() == (-test.float_values[row_id]).value());
  }

  expression->adjust(&records);
  assert(records.size() == test.table->num_rows());
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.value();
    assert(records[i].score.value() == -test.float_values[row_id].value());
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
    size_t row_id = records[i].row_id.value();
    assert(int_results[i].value() ==
           test.float_values[row_id].to_int().value());
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
    size_t row_id = records[i].row_id.value();
    assert(float_results[i].value() ==
           test.int_values[row_id].to_float().value());
  }

  expression->adjust(&records);
  assert(records.size() == test.table->num_rows());
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.value();
    assert(records[i].score.value() ==
           test.int_values[row_id].to_float().value());
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
    size_t row_id = records[i].row_id.value();
    assert(bool_results[i].value() ==
           (test.bool_values[row_id] & test.bool2_values[row_id]).value());
  }

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 0; i < test.bool_values.size(); ++i) {
    if ((test.bool_values[i] & test.bool2_values[i]).is_true()) {
      assert(records[count].row_id.value() == grnxx::Int(i).value());
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
    size_t row_id = records[i].row_id.value();
    assert(bool_results[i].value() ==
           (test.bool_values[row_id] | test.bool2_values[row_id]).value());
  }

  expression->filter(&records);
  size_t count = 0;
  for (size_t i = 0; i < test.bool_values.size(); ++i) {
    if ((test.bool_values[i] | test.bool2_values[i]).is_true()) {
      assert(records[count].row_id.value() == grnxx::Int(i).value());
      ++count;
    }
  }
  assert(records.size() == count);
}

//void test_equal() {
//  grnxx::Error error;

//  // Create an object for building expressions.
//  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
//  assert(builder);

//  // Test an expression (Bool == Bool2).
//  assert(builder->push_column(&error, "Bool"));
//  assert(builder->push_column(&error, "Bool2"));
//  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
//  auto expression = builder->release(&error);
//  assert(expression);

//  auto records = create_input_records();

//  grnxx::Array<grnxx::Bool> results;
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.bool_values[row_id] == test.bool2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  grnxx::Int count = 0;
//  for (grnxx::Int i = 1; i < test.bool_values.size(); ++i) {
//    if (test.bool_values[i] == test.bool2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (Int == Int2).
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Int2"));
//  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.int_values[row_id] == test.int2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.int_values.size(); ++i) {
//    if (test.int_values[i] == test.int2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (Float == Float2).
//  assert(builder->push_column(&error, "Float"));
//  assert(builder->push_column(&error, "Float2"));
//  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.float_values[row_id] == test.float2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.float_values.size(); ++i) {
//    if (test.float_values[i] == test.float2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (GeoPoint == GeoPoint2).
//  assert(builder->push_column(&error, "GeoPoint"));
//  assert(builder->push_column(&error, "GeoPoint2"));
//  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.geo_point_values[row_id] == test.geo_point2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.geo_point_values.size(); ++i) {
//    if (test.geo_point_values[i] == test.geo_point2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (Text == Text2).
//  assert(builder->push_column(&error, "Text"));
//  assert(builder->push_column(&error, "Text2"));
//  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.text_values[row_id] == test.text2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.text_values.size(); ++i) {
//    if (test.text_values[i] == test.text2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (BoolVector == BoolVector2).
//  assert(builder->push_column(&error, "BoolVector"));
//  assert(builder->push_column(&error, "BoolVector2"));
//  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] == (test.bool_vector_values[row_id] ==
//                          test.bool_vector2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.bool_vector_values.size(); ++i) {
//    if (test.bool_vector_values[i] == test.bool_vector2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (IntVector == IntVector2).
//  assert(builder->push_column(&error, "IntVector"));
//  assert(builder->push_column(&error, "IntVector2"));
//  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] == (test.int_vector_values[row_id] ==
//                          test.int_vector2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.int_vector_values.size(); ++i) {
//    if (test.int_vector_values[i] == test.int_vector2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (FloatVector == FloatVector2).
//  assert(builder->push_column(&error, "FloatVector"));
//  assert(builder->push_column(&error, "FloatVector2"));
//  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] == (test.float_vector_values[row_id] ==
//                          test.float_vector2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.float_vector_values.size(); ++i) {
//    if (test.float_vector_values[i] == test.float_vector2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (GeoPointVector == GeoPointVector2).
//  assert(builder->push_column(&error, "GeoPointVector"));
//  assert(builder->push_column(&error, "GeoPointVector2"));
//  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] == (test.geo_point_vector_values[row_id] ==
//                          test.geo_point_vector2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.geo_point_vector_values.size(); ++i) {
//    if (test.geo_point_vector_values[i] == test.geo_point_vector2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (TextVector == TextVector2).
//  assert(builder->push_column(&error, "TextVector"));
//  assert(builder->push_column(&error, "TextVector2"));
//  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] == (test.text_vector_values[row_id] ==
//                          test.text_vector2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.text_vector_values.size(); ++i) {
//    if (test.text_vector_values[i] == test.text_vector2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);
//}

//void test_not_equal() {
//  grnxx::Error error;

//  // Create an object for building expressions.
//  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
//  assert(builder);

//  // Test an expression (Bool != Bool2).
//  assert(builder->push_column(&error, "Bool"));
//  assert(builder->push_column(&error, "Bool2"));
//  assert(builder->push_operator(&error, grnxx::NOT_EQUAL_OPERATOR));
//  auto expression = builder->release(&error);
//  assert(expression);

//  auto records = create_input_records();

//  grnxx::Array<grnxx::Bool> results;
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.bool_values[row_id] != test.bool2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  grnxx::Int count = 0;
//  for (grnxx::Int i = 1; i < test.bool_values.size(); ++i) {
//    if (test.bool_values[i] != test.bool2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (Int != Int2).
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Int2"));
//  assert(builder->push_operator(&error, grnxx::NOT_EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.int_values[row_id] != test.int2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.int_values.size(); ++i) {
//    if (test.int_values[i] != test.int2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (Float != Float2).
//  assert(builder->push_column(&error, "Float"));
//  assert(builder->push_column(&error, "Float2"));
//  assert(builder->push_operator(&error, grnxx::NOT_EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.float_values[row_id] != test.float2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.float_values.size(); ++i) {
//    if (test.float_values[i] != test.float2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (GeoPoint != GeoPoint2).
//  assert(builder->push_column(&error, "GeoPoint"));
//  assert(builder->push_column(&error, "GeoPoint2"));
//  assert(builder->push_operator(&error, grnxx::NOT_EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] == (test.geo_point_values[row_id] !=
//                          test.geo_point2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.geo_point_values.size(); ++i) {
//    if (test.geo_point_values[i] != test.geo_point2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (Text != Text2).
//  assert(builder->push_column(&error, "Text"));
//  assert(builder->push_column(&error, "Text2"));
//  assert(builder->push_operator(&error, grnxx::NOT_EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.text_values[row_id] != test.text2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.text_values.size(); ++i) {
//    if (test.text_values[i] != test.text2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (BoolVector != BoolVector2).
//  assert(builder->push_column(&error, "BoolVector"));
//  assert(builder->push_column(&error, "BoolVector2"));
//  assert(builder->push_operator(&error, grnxx::NOT_EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] == (test.bool_vector_values[row_id] !=
//                          test.bool_vector2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.bool_vector_values.size(); ++i) {
//    if (test.bool_vector_values[i] != test.bool_vector2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (IntVector != IntVector2).
//  assert(builder->push_column(&error, "IntVector"));
//  assert(builder->push_column(&error, "IntVector2"));
//  assert(builder->push_operator(&error, grnxx::NOT_EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] == (test.int_vector_values[row_id] !=
//                          test.int_vector2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.int_vector_values.size(); ++i) {
//    if (test.int_vector_values[i] != test.int_vector2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (FloatVector != FloatVector2).
//  assert(builder->push_column(&error, "FloatVector"));
//  assert(builder->push_column(&error, "FloatVector2"));
//  assert(builder->push_operator(&error, grnxx::NOT_EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] == (test.float_vector_values[row_id] !=
//                          test.float_vector2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.float_vector_values.size(); ++i) {
//    if (test.float_vector_values[i] != test.float_vector2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (GeoPointVector != GeoPointVector2).
//  assert(builder->push_column(&error, "GeoPointVector"));
//  assert(builder->push_column(&error, "GeoPointVector2"));
//  assert(builder->push_operator(&error, grnxx::NOT_EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] == (test.geo_point_vector_values[row_id] !=
//                          test.geo_point_vector2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.geo_point_vector_values.size(); ++i) {
//    if (test.geo_point_vector_values[i] != test.geo_point_vector2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (TextVector != TextVector2).
//  assert(builder->push_column(&error, "TextVector"));
//  assert(builder->push_column(&error, "TextVector2"));
//  assert(builder->push_operator(&error, grnxx::NOT_EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] == (test.text_vector_values[row_id] !=
//                          test.text_vector2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.text_vector_values.size(); ++i) {
//    if (test.text_vector_values[i] != test.text_vector2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);
//}

//void test_less() {
//  grnxx::Error error;

//  // Create an object for building expressions.
//  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
//  assert(builder);

//  // Test an expression (Int < Int2).
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Int2"));
//  assert(builder->push_operator(&error, grnxx::LESS_OPERATOR));
//  auto expression = builder->release(&error);
//  assert(expression);

//  auto records = create_input_records();

//  grnxx::Array<grnxx::Bool> results;
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.int_values[row_id] < test.int2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  grnxx::Int count = 0;
//  for (grnxx::Int i = 1; i < test.int_values.size(); ++i) {
//    if (test.int_values[i] < test.int2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (Float < Float2).
//  assert(builder->push_column(&error, "Float"));
//  assert(builder->push_column(&error, "Float2"));
//  assert(builder->push_operator(&error, grnxx::LESS_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.float_values[row_id] < test.float2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.float_values.size(); ++i) {
//    if (test.float_values[i] < test.float2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (Text < Text2).
//  assert(builder->push_column(&error, "Text"));
//  assert(builder->push_column(&error, "Text2"));
//  assert(builder->push_operator(&error, grnxx::LESS_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.text_values[row_id] < test.text2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.text_values.size(); ++i) {
//    if (test.text_values[i] < test.text2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);
//}

//void test_less_equal() {
//  grnxx::Error error;

//  // Create an object for building expressions.
//  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
//  assert(builder);

//  // Test an expression (Int <= Int2).
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Int2"));
//  assert(builder->push_operator(&error, grnxx::LESS_EQUAL_OPERATOR));
//  auto expression = builder->release(&error);
//  assert(expression);

//  auto records = create_input_records();

//  grnxx::Array<grnxx::Bool> results;
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.int_values[row_id] <= test.int2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  grnxx::Int count = 0;
//  for (grnxx::Int i = 1; i < test.int_values.size(); ++i) {
//    if (test.int_values[i] <= test.int2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (Int <= Int2).
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Int2"));
//  assert(builder->push_operator(&error, grnxx::LESS_EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.int_values[row_id] <= test.int2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.int_values.size(); ++i) {
//    if (test.int_values[i] <= test.int2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (Float <= Float2).
//  assert(builder->push_column(&error, "Float"));
//  assert(builder->push_column(&error, "Float2"));
//  assert(builder->push_operator(&error, grnxx::LESS_EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.float_values[row_id] <= test.float2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.float_values.size(); ++i) {
//    if (test.float_values[i] <= test.float2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (Text <= Text2).
//  assert(builder->push_column(&error, "Text"));
//  assert(builder->push_column(&error, "Text2"));
//  assert(builder->push_operator(&error, grnxx::LESS_EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.text_values[row_id] <= test.text2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.text_values.size(); ++i) {
//    if (test.text_values[i] <= test.text2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);
//}

//void test_greater() {
//  grnxx::Error error;

//  // Create an object for building expressions.
//  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
//  assert(builder);

//  // Test an expression (Int > Int2).
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Int2"));
//  assert(builder->push_operator(&error, grnxx::GREATER_OPERATOR));
//  auto expression = builder->release(&error);
//  assert(expression);

//  auto records = create_input_records();

//  grnxx::Array<grnxx::Bool> results;
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.int_values[row_id] > test.int2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  grnxx::Int count = 0;
//  for (grnxx::Int i = 1; i < test.int_values.size(); ++i) {
//    if (test.int_values[i] > test.int2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (Float > Float2).
//  assert(builder->push_column(&error, "Float"));
//  assert(builder->push_column(&error, "Float2"));
//  assert(builder->push_operator(&error, grnxx::GREATER_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.float_values[row_id] > test.float2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.float_values.size(); ++i) {
//    if (test.float_values[i] > test.float2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (Text > Text2).
//  assert(builder->push_column(&error, "Text"));
//  assert(builder->push_column(&error, "Text2"));
//  assert(builder->push_operator(&error, grnxx::GREATER_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.text_values[row_id] > test.text2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.text_values.size(); ++i) {
//    if (test.text_values[i] > test.text2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);
//}

//void test_greater_equal() {
//  grnxx::Error error;

//  // Create an object for building expressions.
//  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
//  assert(builder);

//  // Test an expression (Int >= Int2).
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Int2"));
//  assert(builder->push_operator(&error, grnxx::GREATER_EQUAL_OPERATOR));
//  auto expression = builder->release(&error);
//  assert(expression);

//  auto records = create_input_records();

//  grnxx::Array<grnxx::Bool> results;
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.int_values[row_id] >= test.int2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  grnxx::Int count = 0;
//  for (grnxx::Int i = 1; i < test.int_values.size(); ++i) {
//    if (test.int_values[i] >= test.int2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (Int >= Int2).
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Int2"));
//  assert(builder->push_operator(&error, grnxx::GREATER_EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.int_values[row_id] >= test.int2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.int_values.size(); ++i) {
//    if (test.int_values[i] >= test.int2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (Float >= Float2).
//  assert(builder->push_column(&error, "Float"));
//  assert(builder->push_column(&error, "Float2"));
//  assert(builder->push_operator(&error, grnxx::GREATER_EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.float_values[row_id] >= test.float2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.float_values.size(); ++i) {
//    if (test.float_values[i] >= test.float2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (Text >= Text2).
//  assert(builder->push_column(&error, "Text"));
//  assert(builder->push_column(&error, "Text2"));
//  assert(builder->push_operator(&error, grnxx::GREATER_EQUAL_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  results.clear();
//  assert(expression->evaluate(&error, records, &results));
//  assert(results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.text_values[row_id] >= test.text2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  count = 0;
//  for (grnxx::Int i = 1; i < test.text_values.size(); ++i) {
//    if (test.text_values[i] >= test.text2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);
//}

//void test_bitwise_and() {
//  grnxx::Error error;

//  // Create an object for building expressions.
//  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
//  assert(builder);

//  // Test an expression (Bool & Bool2).
//  assert(builder->push_column(&error, "Bool"));
//  assert(builder->push_column(&error, "Bool2"));
//  assert(builder->push_operator(&error, grnxx::BITWISE_AND_OPERATOR));
//  auto expression = builder->release(&error);
//  assert(expression);

//  auto records = create_input_records();

//  grnxx::Array<grnxx::Bool> bool_results;
//  assert(expression->evaluate(&error, records, &bool_results));
//  assert(bool_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < bool_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(bool_results[i] ==
//           (test.bool_values[row_id] & test.bool2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  grnxx::Int count = 0;
//  for (grnxx::Int i = 1; i < test.bool_values.size(); ++i) {
//    if (test.bool_values[i] & test.bool2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (Int & Int2).
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Int2"));
//  assert(builder->push_operator(&error, grnxx::BITWISE_AND_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::Int> int_results;
//  assert(expression->evaluate(&error, records, &int_results));
//  assert(int_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < int_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(int_results[i] ==
//           (test.int_values[row_id] & test.int2_values[row_id]));
//  }
//}

//void test_bitwise_or() {
//  grnxx::Error error;

//  // Create an object for building expressions.
//  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
//  assert(builder);

//  // Test an expression (Bool | Bool2).
//  assert(builder->push_column(&error, "Bool"));
//  assert(builder->push_column(&error, "Bool2"));
//  assert(builder->push_operator(&error, grnxx::BITWISE_OR_OPERATOR));
//  auto expression = builder->release(&error);
//  assert(expression);

//  auto records = create_input_records();

//  grnxx::Array<grnxx::Bool> bool_results;
//  assert(expression->evaluate(&error, records, &bool_results));
//  assert(bool_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < bool_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(bool_results[i] ==
//           (test.bool_values[row_id] | test.bool2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  grnxx::Int count = 0;
//  for (grnxx::Int i = 1; i < test.bool_values.size(); ++i) {
//    if (test.bool_values[i] | test.bool2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (Int | Int2).
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Int2"));
//  assert(builder->push_operator(&error, grnxx::BITWISE_OR_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::Int> int_results;
//  assert(expression->evaluate(&error, records, &int_results));
//  assert(int_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < int_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(int_results[i] ==
//           (test.int_values[row_id] | test.int2_values[row_id]));
//  }
//}

//void test_bitwise_xor() {
//  grnxx::Error error;

//  // Create an object for building expressions.
//  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
//  assert(builder);

//  // Test an expression (Bool ^ Bool2).
//  assert(builder->push_column(&error, "Bool"));
//  assert(builder->push_column(&error, "Bool2"));
//  assert(builder->push_operator(&error, grnxx::BITWISE_XOR_OPERATOR));
//  auto expression = builder->release(&error);
//  assert(expression);

//  auto records = create_input_records();

//  grnxx::Array<grnxx::Bool> bool_results;
//  assert(expression->evaluate(&error, records, &bool_results));
//  assert(bool_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < bool_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(bool_results[i] ==
//           (test.bool_values[row_id] ^ test.bool2_values[row_id]));
//  }

//  assert(expression->filter(&error, &records));
//  grnxx::Int count = 0;
//  for (grnxx::Int i = 1; i < test.bool_values.size(); ++i) {
//    if (test.bool_values[i] ^ test.bool2_values[i]) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (Int ^ Int2).
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Int2"));
//  assert(builder->push_operator(&error, grnxx::BITWISE_XOR_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::Int> int_results;
//  assert(expression->evaluate(&error, records, &int_results));
//  assert(int_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < int_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(int_results[i] ==
//           (test.int_values[row_id] ^ test.int2_values[row_id]));
//  }
//}

//void test_plus() {
//  grnxx::Error error;

//  // Create an object for building expressions.
//  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
//  assert(builder);

//  // Test an expression (Int + Int2).
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Int2"));
//  assert(builder->push_operator(&error, grnxx::PLUS_OPERATOR));
//  auto expression = builder->release(&error);
//  assert(expression);

//  auto records = create_input_records();

//  grnxx::Array<grnxx::Int> int_results;
//  assert(expression->evaluate(&error, records, &int_results));
//  assert(int_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < int_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(int_results[i] ==
//           (test.int_values[row_id] + test.int2_values[row_id]));
//  }

//  // Test an expression (Float + Float2).
//  assert(builder->push_column(&error, "Float"));
//  assert(builder->push_column(&error, "Float2"));
//  assert(builder->push_operator(&error, grnxx::PLUS_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::Float> float_results;
//  assert(expression->evaluate(&error, records, &float_results));
//  assert(float_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < float_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(float_results[i] ==
//           (test.float_values[row_id] + test.float2_values[row_id]));
//  }

//  assert(expression->adjust(&error, &records));
//  assert(records.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < records.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(records.get_score(i) ==
//           (test.float_values[row_id] + test.float2_values[row_id]));
//  }
//}

//void test_minus() {
//  grnxx::Error error;

//  // Create an object for building expressions.
//  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
//  assert(builder);

//  // Test an expression (Int - Int2).
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Int2"));
//  assert(builder->push_operator(&error, grnxx::MINUS_OPERATOR));
//  auto expression = builder->release(&error);
//  assert(expression);

//  auto records = create_input_records();

//  grnxx::Array<grnxx::Int> int_results;
//  assert(expression->evaluate(&error, records, &int_results));
//  assert(int_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < int_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(int_results[i] ==
//           (test.int_values[row_id] - test.int2_values[row_id]));
//  }

//  // Test an expression (Float - Float2).
//  assert(builder->push_column(&error, "Float"));
//  assert(builder->push_column(&error, "Float2"));
//  assert(builder->push_operator(&error, grnxx::MINUS_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::Float> float_results;
//  assert(expression->evaluate(&error, records, &float_results));
//  assert(float_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < float_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(float_results[i] ==
//           (test.float_values[row_id] - test.float2_values[row_id]));
//  }

//  assert(expression->adjust(&error, &records));
//  assert(records.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < records.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(records.get_score(i) ==
//           (test.float_values[row_id] - test.float2_values[row_id]));
//  }
//}

//void test_multiplication() {
//  grnxx::Error error;

//  // Create an object for building expressions.
//  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
//  assert(builder);

//  // Test an expression (Int * Int2).
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Int2"));
//  assert(builder->push_operator(&error, grnxx::MULTIPLICATION_OPERATOR));
//  auto expression = builder->release(&error);
//  assert(expression);

//  auto records = create_input_records();

//  grnxx::Array<grnxx::Int> int_results;
//  assert(expression->evaluate(&error, records, &int_results));
//  assert(int_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < int_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(int_results[i] ==
//           (test.int_values[row_id] * test.int2_values[row_id]));
//  }

//  // Test an expression (Float * Float2).
//  assert(builder->push_column(&error, "Float"));
//  assert(builder->push_column(&error, "Float2"));
//  assert(builder->push_operator(&error, grnxx::MULTIPLICATION_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::Float> float_results;
//  assert(expression->evaluate(&error, records, &float_results));
//  assert(float_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < float_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(float_results[i] ==
//           (test.float_values[row_id] * test.float2_values[row_id]));
//  }

//  assert(expression->adjust(&error, &records));
//  assert(records.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < records.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(records.get_score(i) ==
//           (test.float_values[row_id] * test.float2_values[row_id]));
//  }
//}

//void test_division() {
//  grnxx::Error error;

//  // Create an object for building expressions.
//  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
//  assert(builder);

//  // Test an expression (Int / Int2).
//  // An error occurs because of division by zero.
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Int2"));
//  assert(builder->push_operator(&error, grnxx::DIVISION_OPERATOR));
//  auto expression = builder->release(&error);
//  assert(expression);

//  auto records = create_input_records();

//  grnxx::Array<grnxx::Int> int_results;
//  assert(!expression->evaluate(&error, records, &int_results));

//  // Test an expression (Int / (Int2 + 1)).
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Int2"));
//  assert(builder->push_constant(&error, grnxx::Int(1)));
//  assert(builder->push_operator(&error, grnxx::PLUS_OPERATOR));
//  assert(builder->push_operator(&error, grnxx::DIVISION_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  int_results.clear();
//  assert(expression->evaluate(&error, records, &int_results));
//  assert(int_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < int_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(int_results[i] ==
//           (test.int_values[row_id] / (test.int2_values[row_id] + 1)));
//  }

//  // Test an expression (Float / Float2).
//  assert(builder->push_column(&error, "Float"));
//  assert(builder->push_column(&error, "Float2"));
//  assert(builder->push_operator(&error, grnxx::DIVISION_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::Float> float_results;
//  assert(expression->evaluate(&error, records, &float_results));
//  assert(float_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < float_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(float_results[i] ==
//           (test.float_values[row_id] / test.float2_values[row_id]));
//  }

//  assert(expression->adjust(&error, &records));
//  assert(records.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < records.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(records.get_score(i) ==
//           (test.float_values[row_id] / test.float2_values[row_id]));
//  }
//}

//void test_modulus() {
//  grnxx::Error error;

//  // Create an object for building expressions.
//  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
//  assert(builder);

//  // Test an expression (Int % Int2).
//  // An error occurs because of division by zero.
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Int2"));
//  assert(builder->push_operator(&error, grnxx::MODULUS_OPERATOR));
//  auto expression = builder->release(&error);
//  assert(expression);

//  auto records = create_input_records();

//  grnxx::Array<grnxx::Int> int_results;
//  assert(!expression->evaluate(&error, records, &int_results));

//  // Test an expression (Int % (Int2 + 1)).
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Int2"));
//  assert(builder->push_constant(&error, grnxx::Int(1)));
//  assert(builder->push_operator(&error, grnxx::PLUS_OPERATOR));
//  assert(builder->push_operator(&error, grnxx::MODULUS_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  int_results.clear();
//  assert(expression->evaluate(&error, records, &int_results));
//  assert(int_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < int_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(int_results[i] ==
//           (test.int_values[row_id] % (test.int2_values[row_id] + 1)));
//  }
//}

//void test_subscript() {
//  grnxx::Error error;

//  // Create an object for building expressions.
//  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
//  assert(builder);

//  // Test an expression (BoolVector[Int]).
//  assert(builder->push_column(&error, "BoolVector"));
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_operator(&error, grnxx::SUBSCRIPT_OPERATOR));
//  auto expression = builder->release(&error);
//  assert(expression);

//  auto records = create_input_records();

//  grnxx::Array<grnxx::Bool> bool_results;
//  assert(expression->evaluate(&error, records, &bool_results));
//  assert(bool_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < bool_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    const auto int_value = test.int_values[row_id];
//    const auto &bool_vector_value = test.bool_vector_values[row_id];
//    if (int_value < bool_vector_value.size()) {
//      assert(bool_results[i] == bool_vector_value[int_value]);
//    } else {
//      assert(bool_results[i] == 0);
//    }
//  }

//  assert(expression->filter(&error, &records));
//  grnxx::Int count = 0;
//  for (grnxx::Int i = 1; i < test.int_values.size(); ++i) {
//    const auto int_value = test.int_values[i];
//    const auto &bool_vector_value = test.bool_vector_values[i];
//    if (int_value < bool_vector_value.size()) {
//      if (bool_vector_value[int_value]) {
//        assert(records.get_row_id(count) == i);
//        ++count;
//      }
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (IntVector[Int]).
//  assert(builder->push_column(&error, "IntVector"));
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_operator(&error, grnxx::SUBSCRIPT_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::Int> int_results;
//  assert(expression->evaluate(&error, records, &int_results));
//  assert(int_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < int_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    const auto int_value = test.int_values[row_id];
//    const auto &int_vector_value = test.int_vector_values[row_id];
//    if (int_value < int_vector_value.size()) {
//      assert(int_results[i] == int_vector_value[int_value]);
//    } else {
//      assert(int_results[i] == 0);
//    }
//  }

//  // Test an expression (FloatVector[Int]).
//  assert(builder->push_column(&error, "FloatVector"));
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_operator(&error, grnxx::SUBSCRIPT_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::Float> float_results;
//  assert(expression->evaluate(&error, records, &float_results));
//  assert(float_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < float_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    const auto int_value = test.int_values[row_id];
//    const auto &float_vector_value = test.float_vector_values[row_id];
//    if (int_value < float_vector_value.size()) {
//      assert(float_results[i] == float_vector_value[int_value]);
//    } else {
//      assert(float_results[i] == 0);
//    }
//  }

//  assert(expression->adjust(&error, &records));
//  assert(records.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < records.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    const auto int_value = test.int_values[row_id];
//    const auto &float_vector_value = test.float_vector_values[row_id];
//    if (int_value < float_vector_value.size()) {
//      assert(records.get_score(i) == float_vector_value[int_value]);
//    } else {
//      assert(records.get_score(i) == 0.0);
//    }
//  }

//  // Test an expression (GeoPointVector[Int]).
//  assert(builder->push_column(&error, "GeoPointVector"));
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_operator(&error, grnxx::SUBSCRIPT_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::GeoPoint> geo_point_results;
//  assert(expression->evaluate(&error, records, &geo_point_results));
//  assert(geo_point_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < geo_point_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    const auto int_value = test.int_values[row_id];
//    const auto &geo_point_vector_value = test.geo_point_vector_values[row_id];
//    if (int_value < geo_point_vector_value.size()) {
//      assert(geo_point_results[i] == geo_point_vector_value[int_value]);
//    } else {
//      assert(geo_point_results[i] == grnxx::GeoPoint(0, 0));
//    }
//  }

//  // Test an expression (TextVector[Int]).
//  assert(builder->push_column(&error, "TextVector"));
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_operator(&error, grnxx::SUBSCRIPT_OPERATOR));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::Text> text_results;
//  assert(expression->evaluate(&error, records, &text_results));
//  assert(text_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < text_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    const auto int_value = test.int_values[row_id];
//    const auto &text_vector_value = test.text_vector_values[row_id];
//    if (int_value < text_vector_value.size()) {
//      assert(text_results[i] == text_vector_value[int_value]);
//    } else {
//      assert(text_results[i] == grnxx::StringCRef(""));
//    }
//  }
//}

//void test_subexpression() {
//  grnxx::Error error;

//  // Create an object for building expressions.
//  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
//  assert(builder);

//  // Test an expression (Ref.Bool).
//  assert(builder->push_column(&error, "Ref"));
//  assert(builder->begin_subexpression(&error));
//  assert(builder->push_column(&error, "Bool"));
//  assert(builder->end_subexpression(&error));
//  auto expression = builder->release(&error);
//  assert(expression);

//  auto records = create_input_records();

//  grnxx::Array<grnxx::Bool> bool_results;
//  assert(expression->evaluate(&error, records, &bool_results));
//  assert(bool_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < bool_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    const auto ref_value = test.ref_values[row_id];
//    const auto bool_value = test.bool_values[ref_value];
//    assert(bool_results[i] == bool_value);
//  }

//  assert(expression->filter(&error, &records));
//  grnxx::Int count = 0;
//  for (grnxx::Int i = 1; i < test.ref_values.size(); ++i) {
//    const auto ref_value = test.ref_values[i];
//    const auto bool_value = test.bool_values[ref_value];
//    if (bool_value) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);

//  // Test an expression (Ref.Float).
//  assert(builder->push_column(&error, "Ref"));
//  assert(builder->begin_subexpression(&error));
//  assert(builder->push_column(&error, "Float"));
//  assert(builder->end_subexpression(&error));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::Float> float_results;
//  assert(expression->evaluate(&error, records, &float_results));
//  assert(float_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < float_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    const auto ref_value = test.ref_values[row_id];
//    const auto float_value = test.float_values[ref_value];
//    assert(float_results[i] == float_value);
//  }

//  assert(expression->adjust(&error, &records));
//  for (grnxx::Int i = 0; i < float_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    const auto ref_value = test.ref_values[row_id];
//    const auto float_value = test.float_values[ref_value];
//    assert(records.get_score(i) == float_value);
//  }

//  // Test an expression (Ref.(Ref.Text)).
//  assert(builder->push_column(&error, "Ref"));
//  assert(builder->begin_subexpression(&error));
//  assert(builder->push_column(&error, "Ref"));
//  assert(builder->begin_subexpression(&error));
//  assert(builder->push_column(&error, "Text"));
//  assert(builder->end_subexpression(&error));
//  assert(builder->end_subexpression(&error));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::Text> text_results;
//  assert(expression->evaluate(&error, records, &text_results));
//  assert(text_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < text_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    const auto ref_value = test.ref_values[row_id];
//    const auto ref_ref_value = test.ref_values[ref_value];
//    const auto text_value = test.text_values[ref_ref_value];
//    assert(text_results[i] == text_value);
//  }

//  // Test an expression ((Ref.Ref).Int).
//  assert(builder->push_column(&error, "Ref"));
//  assert(builder->begin_subexpression(&error));
//  assert(builder->push_column(&error, "Ref"));
//  assert(builder->end_subexpression(&error));
//  assert(builder->begin_subexpression(&error));
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->end_subexpression(&error));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::Int> int_results;
//  assert(expression->evaluate(&error, records, &int_results));
//  assert(int_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < int_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    const auto ref_value = test.ref_values[row_id];
//    const auto ref_ref_value = test.ref_values[ref_value];
//    const auto int_value = test.int_values[ref_ref_value];
//    assert(int_results[i] == int_value);
//  }

//  // Test an expression (RefVector.Int).
//  assert(builder->push_column(&error, "RefVector"));
//  assert(builder->begin_subexpression(&error));
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->end_subexpression(&error));
//  expression = builder->release(&error);
//  assert(expression);

//  records = create_input_records();

//  grnxx::Array<grnxx::IntVector> int_vector_results;
//  assert(expression->evaluate(&error, records, &int_vector_results));
//  assert(int_vector_results.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < int_vector_results.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    const auto ref_vector_value = test.ref_vector_values[row_id];
//    assert(int_vector_results[i].size() == ref_vector_value.size());
//    for (grnxx::Int j = 0; j < ref_vector_value.size(); ++j) {
//      grnxx::Int ref_value = ref_vector_value[j];
//      const auto int_value = test.int_values[ref_value];
//      assert(int_vector_results[i][j] == int_value);
//    }
//  }
//}

//void test_sequential_filter() {
//  grnxx::Error error;

//  // Create an object for building expressions.
//  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
//  assert(builder);

//  // Test an expression ((Int + Int2) < 100).
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Int2"));
//  assert(builder->push_operator(&error, grnxx::PLUS_OPERATOR));
//  assert(builder->push_constant(&error, grnxx::Int(100)));
//  assert(builder->push_operator(&error, grnxx::LESS_OPERATOR));
//  auto expression = builder->release(&error);
//  assert(expression);

//  auto cursor = test.table->create_cursor(&error);
//  assert(cursor);

//  // Read and filter records block by block.
//  grnxx::Array<grnxx::Record> records;
//  grnxx::Int offset = 0;
//  for ( ; ; ) {
//    auto result = cursor->read(&error, 1024, &records);
//    assert(result.is_ok);
//    assert((offset + result.count) == records.size());
//    if (result.count == 0) {
//      break;
//    }
//    assert(expression->filter(&error, &records, offset));
//    offset = records.size();
//  }

//  grnxx::Int count = 0;
//  for (grnxx::Int i = 1; i < test.bool_values.size(); ++i) {
//    if ((test.int_values[i] + test.int2_values[i]) < 100) {
//      assert(records.get_row_id(count) == i);
//      ++count;
//    }
//  }
//  assert(records.size() == count);
//}

//void test_sequential_adjust() {
//  grnxx::Error error;

//  // Create an object for building expressions.
//  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
//  assert(builder);

//  // Test an expression (Float(Int) + Float).
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_operator(&error, grnxx::TO_FLOAT_OPERATOR));
//  assert(builder->push_column(&error, "Float"));
//  assert(builder->push_operator(&error, grnxx::PLUS_OPERATOR));
//  auto expression = builder->release(&error);
//  assert(expression);

//  auto cursor = test.table->create_cursor(&error);
//  assert(cursor);

//  // Read and adjust records block by block.
//  grnxx::Array<grnxx::Record> records;
//  grnxx::Int offset = 0;
//  for ( ; ; ) {
//    auto result = cursor->read(&error, 1024, &records);
//    assert(result.is_ok);
//    assert((offset + result.count) == records.size());
//    if (result.count == 0) {
//      break;
//    }
//    assert(expression->adjust(&error, &records, offset));
//    offset += result.count;
//  }

//  assert(records.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < records.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(records.get_score(i) ==
//           (test.int_values[row_id] + test.float_values[row_id]));
//  }
//}

//void test_sequential_evaluate() {
//  grnxx::Error error;

//  // Create an object for building expressions.
//  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
//  assert(builder);

//  // Test an expression (Int + Int(Float * 100.0)).
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Float"));
//  assert(builder->push_constant(&error, 100.0));
//  assert(builder->push_operator(&error, grnxx::MULTIPLICATION_OPERATOR));
//  assert(builder->push_operator(&error, grnxx::TO_INT_OPERATOR));
//  assert(builder->push_operator(&error, grnxx::PLUS_OPERATOR));
//  auto expression = builder->release(&error);
//  assert(expression);

//  auto cursor = test.table->create_cursor(&error);
//  assert(cursor);

//  // Read and evaluate records block by block.
//  grnxx::Array<grnxx::Record> records;
//  grnxx::Array<grnxx::Int> results;
//  grnxx::Int offset = 0;
//  for ( ; ; ) {
//    auto result = cursor->read(&error, 1024, &records);
//    assert(result.is_ok);
//    assert((offset + result.count) == records.size());
//    if (result.count == 0) {
//      break;
//    }
//    assert(results.resize(&error, offset + result.count));
//    assert(expression->evaluate(&error, records.ref(offset),
//                                results.ref(offset)));
//    offset += result.count;
//  }

//  assert(records.size() == test.table->num_rows());
//  for (grnxx::Int i = 0; i < records.size(); ++i) {
//    grnxx::Int row_id = records.get_row_id(i);
//    assert(results[i] ==
//           (test.int_values[row_id] +
//            static_cast<grnxx::Int>(test.float_values[row_id] * 100.0)));
//  }
//}

//void test_partial_filter() {
//  grnxx::Error error;

//  // Create an object for building expressions.
//  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
//  assert(builder);

//  // Test an expression ((Float * Float2) > 0.25).
//  assert(builder->push_column(&error, "Float"));
//  assert(builder->push_column(&error, "Float2"));
//  assert(builder->push_operator(&error, grnxx::MULTIPLICATION_OPERATOR));
//  assert(builder->push_constant(&error, grnxx::Float(0.25)));
//  assert(builder->push_operator(&error, grnxx::GREATER_OPERATOR));
//  auto expression = builder->release(&error);
//  assert(expression);

//  // Read all records.
//  auto records = create_input_records();

//  // Extract a part of true records.
//  constexpr grnxx::Int OFFSET = 12345;
//  constexpr grnxx::Int LIMIT = 5000;
//  assert(expression->filter(&error, &records, 0, OFFSET, LIMIT));
//  assert(records.size() == 5000);
//  grnxx::Int count = 0;
//  for (grnxx::Int i = 1; i < test.bool_values.size(); ++i) {
//    if ((test.float_values[i] * test.float2_values[i]) > 0.25) {
//      if ((count >= OFFSET) && (count < (OFFSET + LIMIT))) {
//        assert(records.get_row_id(count - OFFSET) == i);
//      }
//      ++count;
//    }
//  }
//}

//void test_error() {
//  grnxx::Error error;

//  // Create an object for building expressions.
//  auto builder = grnxx::ExpressionBuilder::create(&error, test.table);
//  assert(builder);

//  // Test an invalid expression (Int * Text).
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Text"));
//  assert(!builder->push_operator(&error, grnxx::MULTIPLICATION_OPERATOR));

//  // Additional operations must fail.
//  assert(!builder->push_constant(&error, grnxx::Text("ABC")));
//  assert(!builder->push_column(&error, "Float"));

//  // Clear the broken status.
//  builder->clear();

//  // Test a valid expression (Int + Int).
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_column(&error, "Int"));
//  assert(builder->push_operator(&error, grnxx::PLUS_OPERATOR));
//}

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
//  test_equal();
//  test_not_equal();
//  test_less();
//  test_less_equal();
//  test_greater();
//  test_greater_equal();
//  test_bitwise_and();
//  test_bitwise_or();
//  test_bitwise_xor();
//  test_plus();
//  test_minus();
//  test_multiplication();
//  test_division();
//  test_modulus();
//  test_subscript();

//  // Subexpression.
//  test_subexpression();

//  // Test sequential operations.
//  test_sequential_filter();
//  test_sequential_adjust();
//  test_sequential_evaluate();

//  // Test partial filtering.
//  test_partial_filter();

//  // Test error.
//  test_error();

  return 0;
}
