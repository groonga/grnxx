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
  // Create a database with the default options.
  auto db = grnxx::open_db("");

  // Create a table with the default options.
  auto table = db->create_table("Table");

  // Append the first row.
  grnxx::Int row_id = table->insert_row();

  // Create a column named "Column".
  auto column = table->create_column("Column", grnxx::INT_DATA);

  // Create an index named "Index".
  auto index = column->create_index("Index", grnxx::TREE_INDEX);
  assert(index->column() == column);
  assert(index->name() == "Index");
  assert(index->type() == grnxx::TREE_INDEX);
}

void test_set_and_index() {
  constexpr size_t NUM_ROWS = 1 << 16;

  // Create a database with the default options.
  auto db = grnxx::open_db("");

  // Create a table with the default options.
  auto table = db->create_table("Table");

  // Create a column.
  auto column = table->create_column("Int", grnxx::INT_DATA);

  // Generate random values.
  // Int: [0, 100) or N/A.
  grnxx::Array<grnxx::Int> values;
  values.resize(NUM_ROWS);
  size_t total_count = 0;
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    if ((mersenne_twister() % 128) != 0) {
      values[i] = grnxx::Int(mersenne_twister() % 100);
      ++total_count;
    } else {
      values[i] = grnxx::Int::na();
    }
  }

  // Store generated values into columns.
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = table->insert_row();
    assert(row_id.match(grnxx::Int(i)));
    column->set(row_id, values[i]);
  }

  // Create an index.
  auto index = column->create_index("Index", grnxx::TREE_INDEX);

//  // Create a cursor.
//  auto cursor = index->find_in_range();

//  grnxx::Array<grnxx::Record> records;
//  size_t count = cursor->read_all(&records);
//  assert(count == total_count);
//  for (size_t i = 1; i < NUM_ROWS; ++i) {
//    size_t lhs_row_id = records[i - 1].row_id.raw();
//    size_t rhs_row_id = records[i].row_id.raw();
//    grnxx::Int lhs_value = values[lhs_row_id];
//    grnxx::Int rhs_value = values[rhs_row_id];
//    assert(!lhs_value.is_na());
//    assert(!rhs_value.is_na());
//    assert(lhs_value.raw() <= rhs_value.raw());
//    if (lhs_value.match(rhs_value)) {
//      assert(lhs_row_id < rhs_row_id);
//    }
//  }
}

void test_index_and_set() {
  constexpr size_t NUM_ROWS = 1 << 16;

  // Create a database with the default options.
  auto db = grnxx::open_db("");

  // Create a table with the default options.
  auto table = db->create_table("Table");

  // Create a column.
  auto column = table->create_column("Int", grnxx::INT_DATA);

  // Create an index.
  auto index = column->create_index("Index", grnxx::TREE_INDEX);

  // Generate random values.
  // Int: [0, 100) or N/A.
  grnxx::Array<grnxx::Int> values;
  values.resize(NUM_ROWS);
  size_t total_count = 0;
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    if ((mersenne_twister() % 128) != 0) {
      values[i] = grnxx::Int(mersenne_twister() % 100);
      ++total_count;
    } else {
      values[i] = grnxx::Int::na();
    }
  }

  // Store generated values into columns.
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = table->insert_row();
    assert(row_id.match(grnxx::Int(i)));
    column->set(row_id, values[i]);
  }

//  // Create a cursor.
//  auto cursor = index->find_in_range();

//  grnxx::Array<grnxx::Record> records;
//  size_t count = cursor->read_all(&records);
//  assert(count == NUM_ROWS);
//  for (size_t i = 1; i < NUM_ROWS; ++i) {
//    size_t lhs_row_id = records[i - 1].row_id.raw();
//    size_t rhs_row_id = records[i].row_id.raw();
//    grnxx::Int lhs_value = values[lhs_row_id];
//    grnxx::Int rhs_value = values[rhs_row_id];
//    assert(!lhs_value.is_na());
//    assert(!rhs_value.is_na());
//    assert(lhs_value.raw() <= rhs_value.raw());
//    if (lhs_value.match(rhs_value)) {
//      assert(lhs_row_id < rhs_row_id);
//    }
//  }
}

void test_remove() {
  constexpr size_t NUM_ROWS = 1 << 16;

  // Create a database with the default options.
  auto db = grnxx::open_db("");

  // Create a table with the default options.
  auto table = db->create_table("Table");

  // Create a column.
  auto column = table->create_column("Int", grnxx::INT_DATA);

  // Generate random values.
  // Int: [0, 100) or N/A.
  grnxx::Array<grnxx::Int> values;
  values.resize(NUM_ROWS);
  size_t total_count = 0;
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    if ((mersenne_twister() % 128) != 0) {
      values[i] = grnxx::Int(mersenne_twister() % 100);
      ++total_count;
    } else {
      values[i] = grnxx::Int::na();
    }
  }

  // Store generated values into columns.
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = table->insert_row();
    assert(row_id.match(grnxx::Int(i)));
    column->set(row_id, values[i]);
  }

  // Create an index.
  auto index = column->create_index("Index", grnxx::TREE_INDEX);

  // Remove even rows.
  size_t odd_count = total_count;
  for (size_t i = 0; i < NUM_ROWS; i += 2) {
    grnxx::Int row_id(i);
    grnxx::Datum datum;
    column->get(row_id, &datum);
    if (datum.as_int().is_na()) {
      --odd_count;
    }
    table->remove_row(row_id);
    assert(!table->test_row(row_id));
  }

  // Create a cursor.
//  auto cursor = index->find_in_range();

//  grnxx::Array<grnxx::Record> records;
//  size_t count = cursor->read_all(&records);
//  assert(count == odd_count);
//  for (size_t i = 0; i < count; ++i) {
//    size_t lhs_row_id = records[i - 1].row_id.raw();
//    size_t rhs_row_id = records[i].row_id.raw();
//    grnxx::Int lhs_value = values[lhs_row_id];
//    grnxx::Int rhs_value = values[rhs_row_id];
//    assert(!lhs_value.is_na());
//    assert(!rhs_value.is_na());
//    assert(lhs_value.raw() <= rhs_value.raw());
//    if (lhs_value.match(rhs_value)) {
//      assert(lhs_row_id < rhs_row_id);
//    }
//  }
}

//void test_bool_exact_match() {
//  constexpr grnxx::Int NUM_ROWS = 1 << 16;

//  grnxx::Error error;

//  // Create a database with the default options.
//  auto db = grnxx::open_db(&error, "");
//  assert(db);

//  // Create a table with the default options.
//  auto table = db->create_table(&error, "Table");
//  assert(table);

//  // Create a column.
//  auto column = table->create_column(&error, "Bool", grnxx::BOOL_DATA);
//  assert(column);

//  // Create an index.
//  auto index = column->create_index(&error, "Index", grnxx::TREE_INDEX);
//  assert(index);

//  // Generate random values.
//  // Bool: [false, true].
//  grnxx::Array<grnxx::Bool> values;
//  assert(values.resize(&error, NUM_ROWS + 1));
//  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
//    values.set(i, (mersenne_twister() & 1) == 1);
//  }

//  // Store generated values into columns.
//  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
//    grnxx::Int row_id;
//    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
//                             grnxx::Datum(), &row_id));
//    assert(row_id == i);
//    assert(column->set(&error, row_id, values[i]));
//  }

//  // Test cursors for each value.
//  grnxx::Bool possible_values[2] = { false, true };
//  for (int possible_value_id = 0; possible_value_id < 2; ++possible_value_id) {
//    grnxx::Bool value = possible_values[possible_value_id];

//    auto cursor = index->find(&error, value);
//    assert(cursor);

//    grnxx::Array<grnxx::Record> records;
//    auto result = cursor->read_all(&error, &records);
//    assert(result.is_ok);
//    for (grnxx::Int i = 1; i < records.size(); ++i) {
//      assert(values[records.get_row_id(i)] == value);
//    }

//    grnxx::Int count = 0;
//    for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
//      if (values[i] == value) {
//        ++count;
//      }
//    }
//    assert(count == records.size());
//  }
//}

void test_int_exact_match() {
  constexpr size_t NUM_ROWS = 1 << 16;

  // Create a database with the default options.
  auto db = grnxx::open_db("");

  // Create a table with the default options.
  auto table = db->create_table("Table");

  // Create a column.
  auto column = table->create_column("Int", grnxx::INT_DATA);

  // Create an index.
  auto index = column->create_index("Index", grnxx::TREE_INDEX);

  // Generate random values.
  // Int: [0, 100) or N/A.
  grnxx::Array<grnxx::Int> values;
  values.resize(NUM_ROWS);
  size_t total_count = 0;
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    if ((mersenne_twister() % 128) != 0) {
      values[i] = grnxx::Int(mersenne_twister() % 100);
      ++total_count;
    } else {
      values[i] = grnxx::Int::na();
    }
  }

  // Store generated values into columns.
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = table->insert_row();
    assert(row_id.match(grnxx::Int(i)));
    column->set(row_id, values[i]);
  }

  // Test cursors for each value.
  for (int raw = 0; raw < 100; ++raw) {
    grnxx::Int value(raw);
    auto cursor = index->find(value);

    grnxx::Array<grnxx::Record> records;
    size_t count = cursor->read_all(&records);
    for (size_t i = 0; i < records.size(); ++i) {
      assert(values[records[i].row_id.raw()].match(value));
    }

    count = 0;
    for (size_t i = 0; i < NUM_ROWS; ++i) {
      if (values[i].match(value)) {
        ++count;
      }
    }
    assert(count == records.size());
  }
}

void test_float_exact_match() {
  constexpr size_t NUM_ROWS = 1 << 16;

  // Create a database with the default options.
  auto db = grnxx::open_db("");

  // Create a table with the default options.
  auto table = db->create_table("Table");

  // Create a column.
  auto column = table->create_column("Float", grnxx::FLOAT_DATA);

  // Create an index.
  auto index = column->create_index("Index", grnxx::TREE_INDEX);

  // Generate random values.
  // Float: [0, 1.0) or N/A.
  grnxx::Array<grnxx::Float> values;
  values.resize(NUM_ROWS);
  size_t total_count = 0;
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    if ((mersenne_twister() % 256) != 0) {
      values[i] = grnxx::Float((mersenne_twister() % 256) / 256.0);
      ++total_count;
    } else {
      values[i] = grnxx::Float::na();
    }
  }

  // Store generated values into columns.
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = table->insert_row();
    assert(row_id.match(grnxx::Int(i)));
    column->set(row_id, values[i]);
  }

  // Test cursors for each value.
  for (int raw = 0; raw < 256; ++raw) {
    grnxx::Float value(raw / 256.0);
    auto cursor = index->find(value);

    grnxx::Array<grnxx::Record> records;
    size_t count = cursor->read_all(&records);
    for (size_t i = 0; i < records.size(); ++i) {
      assert(values[records[i].row_id.raw()].match(value));
    }

    count = 0;
    for (size_t i = 0; i < NUM_ROWS; ++i) {
      if (values[i].match(value)) {
        ++count;
      }
    }
    assert(count == records.size());
  }
}

void test_text_exact_match() {
  constexpr size_t NUM_ROWS = 1 << 16;

  // Create a database with the default options.
  auto db = grnxx::open_db("");

  // Create a table with the default options.
  auto table = db->create_table("Table");

  // Create a column.
  auto column = table->create_column("Text", grnxx::TEXT_DATA);

  // Create an index.
  auto index = column->create_index("Index", grnxx::TREE_INDEX);

  // Generate random values.
  // Text: ["0", "256") or N/A.
  char bodies[256][4];
  for (int i = 0; i < 256; ++i) {
    std::sprintf(bodies[i], "%d", i);
  }
  grnxx::Array<grnxx::Text> values;
  values.resize(NUM_ROWS);
  size_t total_count = 0;
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    if ((mersenne_twister() % 256) != 0) {
      values[i] = grnxx::Text(bodies[mersenne_twister() % 256]);
      ++total_count;
    } else {
      values[i] = grnxx::Text::na();
    }
  }

  // Store generated values into columns.
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = table->insert_row();
    assert(row_id.match(grnxx::Int(i)));
    column->set(row_id, values[i]);
  }

  // Test cursors for each value.
  for (int raw = 0; raw < 256; ++raw) {
    grnxx::Text value(bodies[raw]);
    auto cursor = index->find(value);

    grnxx::Array<grnxx::Record> records;
    size_t count = cursor->read_all(&records);
    for (size_t i = 0; i < records.size(); ++i) {
      assert(values[records[i].row_id.raw()].match(value));
    }

    count = 0;
    for (size_t i = 0; i < NUM_ROWS; ++i) {
      if (values[i].match(value)) {
        ++count;
      }
    }
    assert(count == records.size());
  }
}

//void test_text_exact_match() {
//  constexpr grnxx::Int NUM_ROWS = 1 << 16;

//  grnxx::Error error;

//  // Create a database with the default options.
//  auto db = grnxx::open_db(&error, "");
//  assert(db);

//  // Create a table with the default options.
//  auto table = db->create_table(&error, "Table");
//  assert(table);

//  // Create a column.
//  auto column = table->create_column(&error, "Text", grnxx::TEXT_DATA);
//  assert(column);

//  // Create an index.
//  auto index = column->create_index(&error, "Index", grnxx::TREE_INDEX);
//  assert(index);

//  // Generate random values.
//  // Text: ["0", "99"].
//  grnxx::Array<grnxx::Text> values;
//  char bodies[100][3];
//  assert(values.resize(&error, NUM_ROWS + 1));
//  for (int i = 0; i < 100; ++i) {
//    std::sprintf(bodies[i], "%d", i);
//  }
//  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
//    values.set(i, bodies[mersenne_twister() % 100]);
//  }

//  // Store generated values into columns.
//  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
//    grnxx::Int row_id;
//    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
//                             grnxx::Datum(), &row_id));
//    assert(row_id == i);
//    assert(column->set(&error, row_id, values[i]));
//  }

//  // Test cursors for each value.
//  for (int int_value = 0; int_value < 100; ++int_value) {
//    grnxx::Text value = bodies[int_value];

//    auto cursor = index->find(&error, value);
//    assert(cursor);

//    grnxx::Array<grnxx::Record> records;
//    auto result = cursor->read_all(&error, &records);
//    assert(result.is_ok);
//    for (grnxx::Int i = 1; i < records.size(); ++i) {
//      assert(values[records.get_row_id(i)] == value);
//    }

//    grnxx::Int count = 0;
//    for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
//      if (values[i] == value) {
//        ++count;
//      }
//    }
//    assert(count == records.size());
//  }
//}

void test_int_range() {
  constexpr size_t NUM_ROWS = 1 << 16;

  // Create a database with the default options.
  auto db = grnxx::open_db("");

  // Create a table with the default options.
  auto table = db->create_table("Table");

  // Create a column.
  auto column = table->create_column("Int", grnxx::INT_DATA);

  // Create an index.
  auto index = column->create_index("Index", grnxx::TREE_INDEX);

  // Generate random values.
  // Int: [0, 100).
  grnxx::Array<grnxx::Int> values;
  values.resize(NUM_ROWS);
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    if ((mersenne_twister() % 100) == 0) {
      values[i] = grnxx::Int::na();
    } else {
      values[i] = grnxx::Int(mersenne_twister() % 100);
    }
  }

  // Store generated values into columns.
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = table->insert_row();
    column->set(row_id, values[i]);
  }

  // Create a cursor.
  grnxx::IndexRange range;
  range.set_lower_bound(grnxx::Int(10), grnxx::INCLUSIVE_END_POINT);
  range.set_upper_bound(grnxx::Int(90), grnxx::EXCLUSIVE_END_POINT);
  auto cursor = index->find_in_range(range);

  grnxx::Array<grnxx::Record> records;
  size_t count = cursor->read_all(&records);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    assert(!values[lhs_row_id].is_na());
    assert(!values[rhs_row_id].is_na());
    assert((values[lhs_row_id] <= values[rhs_row_id]).is_true());
  }

  count = 0;
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    if ((values[i] >= grnxx::Int(10)).is_true() &&
        (values[i] < grnxx::Int(90)).is_true()) {
      ++count;
    }
  }
  assert(count == records.size());
}

void test_float_range() {
  constexpr size_t NUM_ROWS = 1 << 16;

  // Create a database with the default options.
  auto db = grnxx::open_db("");

  // Create a table with the default options.
  auto table = db->create_table("Table");

  // Create a column.
  auto column = table->create_column("Float", grnxx::FLOAT_DATA);

  // Create an index.
  auto index = column->create_index("Index", grnxx::TREE_INDEX);

  // Generate random values.
  // Float: [0.0, 1.0).
  grnxx::Array<grnxx::Float> values;
  values.resize(NUM_ROWS);
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    if ((mersenne_twister() % 100) == 0) {
      values[i] = grnxx::Float::na();
    } else {
      values[i] = grnxx::Float((mersenne_twister() % 256) / 256.0);
    }
  }

  // Store generated values into columns.
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = table->insert_row();
    column->set(row_id, values[i]);
  }

  // Create a cursor.
  grnxx::IndexRange range;
  range.set_lower_bound(grnxx::Float(0.25), grnxx::INCLUSIVE_END_POINT);
  range.set_upper_bound(grnxx::Float(0.75), grnxx::EXCLUSIVE_END_POINT);
  auto cursor = index->find_in_range(range);

  grnxx::Array<grnxx::Record> records;
  size_t count = cursor->read_all(&records);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    assert(!values[lhs_row_id].is_na());
    assert(!values[rhs_row_id].is_na());
    assert((values[lhs_row_id] <= values[rhs_row_id]).is_true());
  }

  count = 0;
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    if ((values[i] >= grnxx::Float(0.25)).is_true() &&
        (values[i] < grnxx::Float(0.75)).is_true()) {
      ++count;
    }
  }
  assert(count == records.size());
}

void test_text_range() {
  constexpr size_t NUM_ROWS = 1 << 16;

  // Create a database with the default options.
  auto db = grnxx::open_db("");

  // Create a table with the default options.
  auto table = db->create_table("Table");

  // Create a column.
  auto column = table->create_column("Text", grnxx::TEXT_DATA);

  // Create an index.
  auto index = column->create_index("Index", grnxx::TREE_INDEX);

  // Generate random values.
  // Text: ["0", "99"].
  grnxx::Array<grnxx::Text> values;
  char bodies[100][3];
  values.resize(NUM_ROWS);
  for (int i = 0; i < 100; ++i) {
    std::sprintf(bodies[i], "%d", i);
  }
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    if ((mersenne_twister() % 100) == 0) {
      values[i] = grnxx::Text::na();
    } else {
      values[i] = grnxx::Text(bodies[mersenne_twister() % 100]);
    }
  }

  // Store generated values into columns.
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = table->insert_row();
    column->set(row_id, values[i]);
  }

  // Create a cursor.
  grnxx::IndexRange range;
  range.set_lower_bound(grnxx::Text("25"), grnxx::EXCLUSIVE_END_POINT);
  range.set_upper_bound(grnxx::Text("75"), grnxx::INCLUSIVE_END_POINT);
  auto cursor = index->find_in_range(range);

  grnxx::Array<grnxx::Record> records;
  size_t count = cursor->read_all(&records);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    assert(!values[lhs_row_id].is_na());
    assert(!values[rhs_row_id].is_na());
    assert((values[lhs_row_id] <= values[rhs_row_id]).is_true());
  }

  count = 0;
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    if ((values[i] > grnxx::Text("25")).is_true() &&
        (values[i] <= grnxx::Text("75")).is_true()) {
      ++count;
    }
  }
  assert(count == records.size());
}

void test_text_find_starts_with() {
  constexpr size_t NUM_ROWS = 1 << 16;

  // Create a database with the default options.
  auto db = grnxx::open_db("");

  // Create a table with the default options.
  auto table = db->create_table("Table");

  // Create a column.
  auto column = table->create_column("Text", grnxx::TEXT_DATA);

  // Create an index.
  auto index = column->create_index("Index", grnxx::TREE_INDEX);

  // Generate random values.
  // Text: ["0", "99"].
  grnxx::Array<grnxx::Text> values;
  char bodies[100][3];
  values.resize(NUM_ROWS);
  for (int i = 0; i < 100; ++i) {
    std::sprintf(bodies[i], "%d", i);
  }
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    if ((mersenne_twister() % 100) == 0) {
      values[i] = grnxx::Text::na();
    } else {
      values[i] = grnxx::Text(bodies[mersenne_twister() % 100]);
    }
  }

  // Store generated values into columns.
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = table->insert_row();
    column->set(row_id, values[i]);
  }

  // Test cursors for each value.
  for (int int_value = 0; int_value < 100; ++int_value) {
    grnxx::Text value = grnxx::Text(bodies[int_value]);

    grnxx::EndPoint prefix;
    prefix.value = value;
    prefix.type = grnxx::INCLUSIVE_END_POINT;
    auto cursor = index->find_starts_with(prefix);

    grnxx::Array<grnxx::Record> records;
    size_t count = cursor->read_all(&records);
    for (size_t i = 0; i < records.size(); ++i) {
      size_t row_id = records[i].row_id.raw();
      assert(values[row_id].starts_with(value).is_true());
    }

    count = 0;
    for (size_t i = 0; i < NUM_ROWS; ++i) {
      if (values[i].starts_with(value).is_true()) {
        ++count;
      }
    }
    assert(count == records.size());
  }

  // Test cursors for each value.
  for (int int_value = 0; int_value < 100; ++int_value) {
    grnxx::Text value = grnxx::Text(bodies[int_value]);

    grnxx::EndPoint prefix;
    prefix.value = value;
    prefix.type = grnxx::EXCLUSIVE_END_POINT;
    auto cursor = index->find_starts_with(prefix);

    grnxx::Array<grnxx::Record> records;
    size_t count = cursor->read_all(&records);
    for (size_t i = 0; i < records.size(); ++i) {
      size_t row_id = records[i].row_id.raw();
      assert(values[row_id].unmatch(value) &&
             values[row_id].starts_with(value).is_true());
    }

    count = 0;
    for (size_t i = 0; i < NUM_ROWS; ++i) {
      if (values[i].unmatch(value) &&
          values[i].starts_with(value).is_true()) {
        ++count;
      }
    }
    assert(count == records.size());
  }
}

void test_text_find_prefixes() {
  constexpr size_t NUM_ROWS = 1 << 16;

  // Create a database with the default options.
  auto db = grnxx::open_db("");

  // Create a table with the default options.
  auto table = db->create_table("Table");

  // Create a column.
  auto column = table->create_column("Text", grnxx::TEXT_DATA);

  // Create an index.
  auto index = column->create_index("Index", grnxx::TREE_INDEX);

  // Generate random values.
  // Text: ["0", "99"].
  grnxx::Array<grnxx::Text> values;
  char bodies[100][3];
  values.resize(NUM_ROWS);
  for (int i = 0; i < 100; ++i) {
    std::sprintf(bodies[i], "%d", i);
  }
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    if ((mersenne_twister() % 100) == 0) {
      values[i] = grnxx::Text::na();
    } else {
      values[i] = grnxx::Text(bodies[mersenne_twister() % 100]);
    }
  }

  // Store generated values into columns.
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = table->insert_row();
    column->set(row_id, values[i]);
  }

  // Test cursors for each value.
  for (int int_value = 0; int_value < 100; ++int_value) {
    grnxx::Text value(bodies[int_value]);
    auto cursor = index->find_prefixes(value);

    grnxx::Array<grnxx::Record> records;
    size_t count = cursor->read_all(&records);
    for (size_t i = 0; i < records.size(); ++i) {
      size_t row_id = records[i].row_id.raw();
      assert(value.starts_with(values[row_id]).is_true());
    }

    count = 0;
    for (size_t i = 0; i < NUM_ROWS; ++i) {
      if (value.starts_with(values[i]).is_true()) {
        ++count;
      }
    }
    assert(count == records.size());
  }
}

//void test_reverse() {
//  constexpr grnxx::Int NUM_ROWS = 1 << 16;

//  grnxx::Error error;

//  // Create a database with the default options.
//  auto db = grnxx::open_db(&error, "");
//  assert(db);

//  // Create a table with the default options.
//  auto table = db->create_table(&error, "Table");
//  assert(table);

//  // Create a column.
//  auto column = table->create_column(&error, "Int", grnxx::INT_DATA);
//  assert(column);

//  // Create an index.
//  auto index = column->create_index(&error, "Index", grnxx::TREE_INDEX);
//  assert(index);

//  // Generate random values.
//  // Int: [0, 100).
//  grnxx::Array<grnxx::Int> values;
//  assert(values.resize(&error, NUM_ROWS + 1));
//  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
//    values.set(i, mersenne_twister() % 100);
//  }

//  // Store generated values into columns.
//  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
//    grnxx::Int row_id;
//    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
//                             grnxx::Datum(), &row_id));
//    assert(row_id == i);
//    assert(column->set(&error, row_id, values[i]));
//  }

//  // Create a cursor.
//  grnxx::IndexRange range;
//  range.set_lower_bound(grnxx::Int(10), grnxx::INCLUSIVE_END_POINT);
//  range.set_upper_bound(grnxx::Int(90), grnxx::EXCLUSIVE_END_POINT);
//  grnxx::CursorOptions options;
//  options.order_type = grnxx::REVERSE_ORDER;
//  auto cursor = index->find_in_range(&error, range, options);
//  assert(cursor);

//  grnxx::Array<grnxx::Record> records;
//  auto result = cursor->read_all(&error, &records);
//  assert(result.is_ok);
//  for (grnxx::Int i = 1; i < records.size(); ++i) {
//    assert(values[records.get_row_id(i - 1)] >= values[records.get_row_id(i)]);
//  }

//  grnxx::Int count = 0;
//  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
//    if ((values[i] >= 10) && (values[i] < 90)) {
//      ++count;
//    }
//  }
//  assert(count == records.size());
//}

//void test_offset_and_limit() {
//  constexpr grnxx::Int NUM_ROWS = 1 << 16;

//  grnxx::Error error;

//  // Create a database with the default options.
//  auto db = grnxx::open_db(&error, "");
//  assert(db);

//  // Create a table with the default options.
//  auto table = db->create_table(&error, "Table");
//  assert(table);

//  // Create a column.
//  auto column = table->create_column(&error, "Int", grnxx::INT_DATA);
//  assert(column);

//  // Generate random values.
//  // Int: [0, 100).
//  grnxx::Array<grnxx::Int> values;
//  assert(values.resize(&error, NUM_ROWS + 1));
//  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
//    values.set(i, mersenne_twister() % 100);
//  }

//  // Store generated values into columns.
//  for (grnxx::Int i = 1; i <= NUM_ROWS; ++i) {
//    grnxx::Int row_id;
//    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
//                             grnxx::Datum(), &row_id));
//    assert(row_id == i);
//    assert(column->set(&error, row_id, values[i]));
//  }

//  // Create an index.
//  auto index = column->create_index(&error, "Index", grnxx::TREE_INDEX);
//  assert(index);

//  // Create a cursor.
//  auto cursor = index->find_in_range(&error);
//  assert(cursor);

//  grnxx::Array<grnxx::Record> records;
//  auto result = cursor->read_all(&error, &records);
//  assert(result.is_ok);
//  assert(result.count == NUM_ROWS);

//  constexpr grnxx::Int OFFSET = 1000;

//  // Create a cursor with an offset.
//  grnxx::CursorOptions options;
//  options.offset = OFFSET;
//  cursor = index->find_in_range(&error, grnxx::IndexRange(), options);

//  grnxx::Array<grnxx::Record> records_with_offset;
//  result = cursor->read_all(&error, &records_with_offset);
//  assert(result.is_ok);
//  assert(result.count == (NUM_ROWS - OFFSET));

//  for (grnxx::Int i = 0; i < records_with_offset.size(); ++i) {
//    assert(records.get_row_id(OFFSET + i) ==
//           records_with_offset.get_row_id(i));
//  }

//  constexpr grnxx::Int LIMIT = 100;

//  // Create a cursor with an offset and a limit.
//  options.limit = LIMIT;
//  cursor = index->find_in_range(&error, grnxx::IndexRange(), options);

//  grnxx::Array<grnxx::Record> records_with_offset_and_limit;
//  result = cursor->read_all(&error, &records_with_offset_and_limit);
//  assert(result.is_ok);
//  assert(result.count == LIMIT);

//  for (grnxx::Int i = 0; i < records_with_offset_and_limit.size(); ++i) {
//    assert(records.get_row_id(OFFSET + i) ==
//           records_with_offset_and_limit.get_row_id(i));
//  }
//}

int main() {
  test_index();

  test_set_and_index();
  test_index_and_set();
  test_remove();

//  test_bool_exact_match();
  test_int_exact_match();
  test_float_exact_match();
  test_text_exact_match();

  test_int_range();
  test_float_range();
  test_text_range();

  test_text_find_starts_with();
  test_text_find_prefixes();

//  test_reverse();
//  test_offset_and_limit();

  return 0;
}
