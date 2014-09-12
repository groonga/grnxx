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
  auto cursor = index->create_cursor(&error);
  assert(cursor);

  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) == NUM_ROWS);
  for (grnxx::Int i = 1; i < NUM_ROWS; ++i) {
    assert(values[records.get_row_id(i)] <= values[records.get_row_id(i)]);
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
  auto cursor = index->create_cursor(&error);
  assert(cursor);

  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) == NUM_ROWS);
  for (grnxx::Int i = 1; i < NUM_ROWS; ++i) {
    assert(values[records.get_row_id(i)] <= values[records.get_row_id(i)]);
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
  auto cursor = index->create_cursor(&error);
  assert(cursor);

  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) == (NUM_ROWS / 2));
  for (grnxx::Int i = 1; i < (NUM_ROWS / 2); ++i) {
    assert(values[records.get_row_id(i)] <= values[records.get_row_id(i)]);
  }
}

void test_range() {
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
  auto cursor = index->create_cursor(&error, range);
  assert(cursor);

  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) != -1);
  for (grnxx::Int i = 1; i < records.size(); ++i) {
    assert(values[records.get_row_id(i)] <= values[records.get_row_id(i)]);
  }

  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    if ((values[i] >= 10) && (values[i] < 90)) {
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
  auto cursor = index->create_cursor(&error, range, options);
  assert(cursor);

  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&error, &records) != -1);
  for (grnxx::Int i = 1; i < records.size(); ++i) {
    assert(values[records.get_row_id(i)] >= values[records.get_row_id(i)]);
  }

  grnxx::Int count = 0;
  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
    if ((values[i] >= 10) && (values[i] < 90)) {
      ++count;
    }
  }
  assert(count == records.size());
}

int main() {
  test_index();

  test_set_and_index();
  test_index_and_set();
  test_remove();

  test_range();
  test_reverse();

  return 0;
}
