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
#include "grnxx/index.hpp"
#include "grnxx/table.hpp"

std::mt19937_64 mersenne_twister;

void test_index() {
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

  // Create a column named "Column".
  auto column = table->create_column(&error, "Column", grnxx::INT_DATA);
  assert(column);

  // Create an index named "Index".
  auto index = column->create_index(&error, "Index", grnxx::TREE_INDEX);
  assert(index);
  assert(index->column() == column);
  assert(index->name() == "Index");
  assert(index->type() == grnxx::TREE_INDEX);
}

void test_set_and_index() {
  constexpr grnxx::Int NUM_ROWS = 1 << 16;

  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table with the default options.
  auto table = db->create_table(&error, "Table");
  assert(table);

  // Create a column.
  auto column = table->create_column(&error, "Int", grnxx::INT_DATA);
  assert(column);

  // Generate random values.
  // Int: [0, 100).
  grnxx::Array<grnxx::Int> values;
  assert(values.resize(&error, NUM_ROWS + 1));
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    values.set(i, mersenne_twister() % 100);
  }

  // Store generated values into columns.
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    assert(row_id == i);
    assert(column->set(&error, row_id, values[i]));
  }

  // Create an index.
  auto index = column->create_index(&error, "Index", grnxx::TREE_INDEX);
  assert(index);

  // Create a cursor.
  auto cursor = index->find_in_range(&error);
  assert(cursor);

  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) == NUM_ROWS);
  for (grnxx::Int i = 1; i < NUM_ROWS; ++i) {
    assert(values[records.get_row_id(i - 1)] <= values[records.get_row_id(i)]);
  }
}

void test_index_and_set() {
  constexpr grnxx::Int NUM_ROWS = 1 << 16;

  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table with the default options.
  auto table = db->create_table(&error, "Table");
  assert(table);

  // Create a column.
  auto column = table->create_column(&error, "Int", grnxx::INT_DATA);
  assert(column);

  // Create an index.
  auto index = column->create_index(&error, "Index", grnxx::TREE_INDEX);
  assert(index);

  // Generate random values.
  // Int: [0, 100).
  grnxx::Array<grnxx::Int> values;
  assert(values.resize(&error, NUM_ROWS + 1));
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    values.set(i, mersenne_twister() % 100);
  }

  // Store generated values into columns.
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    assert(row_id == i);
    assert(column->set(&error, row_id, values[i]));
  }

  // Create a cursor.
  auto cursor = index->find_in_range(&error);
  assert(cursor);

  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) == NUM_ROWS);
  for (grnxx::Int i = 1; i < NUM_ROWS; ++i) {
    assert(values[records.get_row_id(i - 1)] <= values[records.get_row_id(i)]);
  }
}

void test_remove() {
  constexpr grnxx::Int NUM_ROWS = 1 << 16;

  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table with the default options.
  auto table = db->create_table(&error, "Table");
  assert(table);

  // Create a column.
  auto column = table->create_column(&error, "Int", grnxx::INT_DATA);
  assert(column);

  // Generate random values.
  // Int: [0, 100).
  grnxx::Array<grnxx::Int> values;
  assert(values.resize(&error, NUM_ROWS + 1));
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    values.set(i, mersenne_twister() % 100);
  }

  // Store generated values into columns.
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    assert(row_id == i);
    assert(column->set(&error, row_id, values[i]));
  }

  // Create an index.
  auto index = column->create_index(&error, "Index", grnxx::TREE_INDEX);
  assert(index);

  // Remove odd rows.
  for (grnxx::Int i = 1; i <= NUM_ROWS; i += 2) {
    assert(table->remove_row(&error, i));
    assert(!table->test_row(&error, i));
  }

  // Create a cursor.
  auto cursor = index->find_in_range(&error);
  assert(cursor);

  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) == (NUM_ROWS / 2));
  for (grnxx::Int i = 1; i < (NUM_ROWS / 2); ++i) {
    assert(values[records.get_row_id(i - 1)] <= values[records.get_row_id(i)]);
  }
}

void test_bool_exact_match() {
  constexpr grnxx::Int NUM_ROWS = 1 << 16;

  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table with the default options.
  auto table = db->create_table(&error, "Table");
  assert(table);

  // Create a column.
  auto column = table->create_column(&error, "Bool", grnxx::BOOL_DATA);
  assert(column);

  // Create an index.
  auto index = column->create_index(&error, "Index", grnxx::TREE_INDEX);
  assert(index);

  // Generate random values.
  // Bool: [false, true].
  grnxx::Array<grnxx::Bool> values;
  assert(values.resize(&error, NUM_ROWS + 1));
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    values.set(i, (mersenne_twister() & 1) == 1);
  }

  // Store generated values into columns.
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    assert(row_id == i);
    assert(column->set(&error, row_id, values[i]));
  }

  // Test cursors for each value.
  grnxx::Bool possible_values[2] = { false, true };
  for (int possible_value_id = 0; possible_value_id < 2; ++possible_value_id) {
    grnxx::Bool value = possible_values[possible_value_id];

    auto cursor = index->find(&error, value);
    assert(cursor);

    grnxx::Array<grnxx::Record> records;
    assert(cursor->read_all(&error, &records) != -1);
    for (grnxx::Int i = 1; i < records.size(); ++i) {
      assert(values[records.get_row_id(i)] == value);
    }

    grnxx::Int count = 0;
    for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
      if (values[i] == value) {
        ++count;
      }
    }
    assert(count == records.size());
  }
}

void test_int_exact_match() {
  constexpr grnxx::Int NUM_ROWS = 1 << 16;

  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table with the default options.
  auto table = db->create_table(&error, "Table");
  assert(table);

  // Create a column.
  auto column = table->create_column(&error, "Int", grnxx::INT_DATA);
  assert(column);

  // Create an index.
  auto index = column->create_index(&error, "Index", grnxx::TREE_INDEX);
  assert(index);

  // Generate random values.
  // Int: [0, 100).
  grnxx::Array<grnxx::Int> values;
  assert(values.resize(&error, NUM_ROWS + 1));
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    values.set(i, mersenne_twister() % 100);
  }

  // Store generated values into columns.
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    assert(row_id == i);
    assert(column->set(&error, row_id, values[i]));
  }

  // Test cursors for each value.
  for (grnxx::Int value = 0; value < 100; ++value) {
    auto cursor = index->find(&error, value);
    assert(cursor);

    grnxx::Array<grnxx::Record> records;
    assert(cursor->read_all(&error, &records) != -1);
    for (grnxx::Int i = 1; i < records.size(); ++i) {
      assert(values[records.get_row_id(i)] == value);
    }

    grnxx::Int count = 0;
    for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
      if (values[i] == value) {
        ++count;
      }
    }
    assert(count == records.size());
  }
}

void test_float_exact_match() {
  constexpr grnxx::Int NUM_ROWS = 1 << 16;

  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table with the default options.
  auto table = db->create_table(&error, "Table");
  assert(table);

  // Create a column.
  auto column = table->create_column(&error, "Float", grnxx::FLOAT_DATA);
  assert(column);

  // Create an index.
  auto index = column->create_index(&error, "Index", grnxx::TREE_INDEX);
  assert(index);

  // Generate random values.
  // Float: [0.0, 1.0).
  grnxx::Array<grnxx::Float> values;
  assert(values.resize(&error, NUM_ROWS + 1));
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    values.set(i, (mersenne_twister() % 256) / 256.0);
  }

  // Store generated values into columns.
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    assert(row_id == i);
    assert(column->set(&error, row_id, values[i]));
  }

  // Test cursors for each value.
  for (grnxx::Int int_value = 0; int_value < 256; ++int_value) {
    grnxx::Float value = int_value / 256.0;

    auto cursor = index->find(&error, value);
    assert(cursor);

    grnxx::Array<grnxx::Record> records;
    assert(cursor->read_all(&error, &records) != -1);
    for (grnxx::Int i = 1; i < records.size(); ++i) {
      assert(values[records.get_row_id(i)] == value);
    }

    grnxx::Int count = 0;
    for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
      if (values[i] == value) {
        ++count;
      }
    }
    assert(count == records.size());
  }
}

void test_text_exact_match() {
  constexpr grnxx::Int NUM_ROWS = 1 << 16;

  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table with the default options.
  auto table = db->create_table(&error, "Table");
  assert(table);

  // Create a column.
  auto column = table->create_column(&error, "Text", grnxx::TEXT_DATA);
  assert(column);

  // Create an index.
  auto index = column->create_index(&error, "Index", grnxx::TREE_INDEX);
  assert(index);

  // Generate random values.
  // Text: ["0", "99"].
  grnxx::Array<grnxx::Text> values;
  char bodies[100][3];
  assert(values.resize(&error, NUM_ROWS + 1));
  for (int i = 0; i < 100; ++i) {
    std::sprintf(bodies[i], "%d", i);
  }
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    values.set(i, bodies[mersenne_twister() % 100]);
  }

  // Store generated values into columns.
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    assert(row_id == i);
    assert(column->set(&error, row_id, values[i]));
  }

  // Test cursors for each value.
  for (int int_value = 0; int_value < 100; ++int_value) {
    grnxx::Text value = bodies[int_value];

    auto cursor = index->find(&error, value);
    assert(cursor);

    grnxx::Array<grnxx::Record> records;
    assert(cursor->read_all(&error, &records) != -1);
    for (grnxx::Int i = 1; i < records.size(); ++i) {
      assert(values[records.get_row_id(i)] == value);
    }

    grnxx::Int count = 0;
    for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
      if (values[i] == value) {
        ++count;
      }
    }
    assert(count == records.size());
  }
}

void test_int_range() {
  constexpr grnxx::Int NUM_ROWS = 1 << 16;

  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table with the default options.
  auto table = db->create_table(&error, "Table");
  assert(table);

  // Create a column.
  auto column = table->create_column(&error, "Int", grnxx::INT_DATA);
  assert(column);

  // Create an index.
  auto index = column->create_index(&error, "Index", grnxx::TREE_INDEX);
  assert(index);

  // Generate random values.
  // Int: [0, 100).
  grnxx::Array<grnxx::Int> values;
  assert(values.resize(&error, NUM_ROWS + 1));
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    values.set(i, mersenne_twister() % 100);
  }

  // Store generated values into columns.
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    assert(row_id == i);
    assert(column->set(&error, row_id, values[i]));
  }

  // Create a cursor.
  grnxx::IndexRange range;
  range.set_lower_bound(grnxx::Int(10), grnxx::INCLUSIVE_END_POINT);
  range.set_upper_bound(grnxx::Int(90), grnxx::EXCLUSIVE_END_POINT);
  auto cursor = index->find_in_range(&error, range);
  assert(cursor);

  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) != -1);
  for (grnxx::Int i = 1; i < records.size(); ++i) {
    assert(values[records.get_row_id(i - 1)] <= values[records.get_row_id(i)]);
  }

  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    if ((values[i] >= 10) && (values[i] < 90)) {
      ++count;
    }
  }
  assert(count == records.size());
}

void test_float_range() {
  constexpr grnxx::Int NUM_ROWS = 1 << 16;

  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table with the default options.
  auto table = db->create_table(&error, "Table");
  assert(table);

  // Create a column.
  auto column = table->create_column(&error, "Float", grnxx::FLOAT_DATA);
  assert(column);

  // Create an index.
  auto index = column->create_index(&error, "Index", grnxx::TREE_INDEX);
  assert(index);

  // Generate random values.
  // Float: [0.0, 1.0).
  grnxx::Array<grnxx::Float> values;
  assert(values.resize(&error, NUM_ROWS + 1));
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    values.set(i, (mersenne_twister() % 256) / 256.0);
  }

  // Store generated values into columns.
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    assert(row_id == i);
    assert(column->set(&error, row_id, values[i]));
  }

  // Create a cursor.
  grnxx::IndexRange range;
  range.set_lower_bound(grnxx::Float(64 / 256.0), grnxx::INCLUSIVE_END_POINT);
  range.set_upper_bound(grnxx::Float(192 / 256.0), grnxx::EXCLUSIVE_END_POINT);
  auto cursor = index->find_in_range(&error, range);
  assert(cursor);

  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) != -1);
  for (grnxx::Int i = 1; i < records.size(); ++i) {
    assert(values[records.get_row_id(i - 1)] <= values[records.get_row_id(i)]);
  }

  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    if ((values[i] >= (64 / 256.0)) && (values[i] < (192 / 256.0))) {
      ++count;
    }
  }
  assert(count == records.size());
}

void test_text_range() {
  constexpr grnxx::Int NUM_ROWS = 1 << 16;

  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table with the default options.
  auto table = db->create_table(&error, "Table");
  assert(table);

  // Create a column.
  auto column = table->create_column(&error, "Text", grnxx::TEXT_DATA);
  assert(column);

  // Create an index.
  auto index = column->create_index(&error, "Index", grnxx::TREE_INDEX);
  assert(index);

  // Generate random values.
  // Text: ["0", "99"].
  grnxx::Array<grnxx::Text> values;
  char bodies[100][3];
  assert(values.resize(&error, NUM_ROWS + 1));
  for (int i = 0; i < 100; ++i) {
    std::sprintf(bodies[i], "%d", i);
  }
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    values.set(i, bodies[mersenne_twister() % 100]);
  }

  // Store generated values into columns.
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    assert(row_id == i);
    assert(column->set(&error, row_id, values[i]));
  }

  // Create a cursor.
  grnxx::IndexRange range;
  range.set_lower_bound(grnxx::Text("25"), grnxx::EXCLUSIVE_END_POINT);
  range.set_upper_bound(grnxx::Text("75"), grnxx::INCLUSIVE_END_POINT);
  auto cursor = index->find_in_range(&error, range);
  assert(cursor);

  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) != -1);
  for (grnxx::Int i = 1; i < records.size(); ++i) {
    assert(values[records.get_row_id(i - 1)] <= values[records.get_row_id(i)]);
  }

  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    if ((values[i] > "25") && (values[i] <= "75")) {
      ++count;
    }
  }
  assert(count == records.size());
}

void test_reverse() {
  constexpr grnxx::Int NUM_ROWS = 1 << 16;

  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table with the default options.
  auto table = db->create_table(&error, "Table");
  assert(table);

  // Create a column.
  auto column = table->create_column(&error, "Int", grnxx::INT_DATA);
  assert(column);

  // Create an index.
  auto index = column->create_index(&error, "Index", grnxx::TREE_INDEX);
  assert(index);

  // Generate random values.
  // Int: [0, 100).
  grnxx::Array<grnxx::Int> values;
  assert(values.resize(&error, NUM_ROWS + 1));
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    values.set(i, mersenne_twister() % 100);
  }

  // Store generated values into columns.
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    assert(row_id == i);
    assert(column->set(&error, row_id, values[i]));
  }

  // Create a cursor.
  grnxx::IndexRange range;
  range.set_lower_bound(grnxx::Int(10), grnxx::INCLUSIVE_END_POINT);
  range.set_upper_bound(grnxx::Int(90), grnxx::EXCLUSIVE_END_POINT);
  grnxx::CursorOptions options;
  options.order_type = grnxx::REVERSE_ORDER;
  auto cursor = index->find_in_range(&error, range, options);
  assert(cursor);

  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) != -1);
  for (grnxx::Int i = 1; i < records.size(); ++i) {
    assert(values[records.get_row_id(i - 1)] >= values[records.get_row_id(i)]);
  }

  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    if ((values[i] >= 10) && (values[i] < 90)) {
      ++count;
    }
  }
  assert(count == records.size());
}

void test_offset_and_limit() {
  constexpr grnxx::Int NUM_ROWS = 1 << 16;

  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table with the default options.
  auto table = db->create_table(&error, "Table");
  assert(table);

  // Create a column.
  auto column = table->create_column(&error, "Int", grnxx::INT_DATA);
  assert(column);

  // Generate random values.
  // Int: [0, 100).
  grnxx::Array<grnxx::Int> values;
  assert(values.resize(&error, NUM_ROWS + 1));
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    values.set(i, mersenne_twister() % 100);
  }

  // Store generated values into columns.
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    assert(row_id == i);
    assert(column->set(&error, row_id, values[i]));
  }

  // Create an index.
  auto index = column->create_index(&error, "Index", grnxx::TREE_INDEX);
  assert(index);

  // Create a cursor.
  auto cursor = index->find_in_range(&error);
  assert(cursor);

  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) == NUM_ROWS);

  constexpr grnxx::Int OFFSET = 1000;

  // Create a cursor with an offset.
  grnxx::CursorOptions options;
  options.offset = OFFSET;
  cursor = index->find_in_range(&error, grnxx::IndexRange(), options);

  grnxx::Array<grnxx::Record> records_with_offset;
  assert(cursor->read_all(&error, &records_with_offset) == (NUM_ROWS - OFFSET));

  for (grnxx::Int i = 0; i < records_with_offset.size(); ++i) {
    assert(records.get_row_id(OFFSET + i) ==
           records_with_offset.get_row_id(i));
  }

  constexpr grnxx::Int LIMIT = 100;

  // Create a cursor with an offset and a limit.
  options.limit = LIMIT;
  cursor = index->find_in_range(&error, grnxx::IndexRange(), options);

  grnxx::Array<grnxx::Record> records_with_offset_and_limit;
  assert(cursor->read_all(&error, &records_with_offset_and_limit) == LIMIT);

  for (grnxx::Int i = 0; i < records_with_offset_and_limit.size(); ++i) {
    assert(records.get_row_id(OFFSET + i) ==
           records_with_offset_and_limit.get_row_id(i));
  }
}

int main() {
  test_index();

  test_set_and_index();
  test_index_and_set();
  test_remove();

  test_bool_exact_match();
  test_int_exact_match();
  test_float_exact_match();
  test_text_exact_match();

  test_int_range();
  test_float_range();
  test_text_range();

  test_reverse();
  test_offset_and_limit();

  return 0;
}
