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
#include <vector>

#include "grnxx/column.hpp"
#include "grnxx/cursor.hpp"
#include "grnxx/db.hpp"
#include "grnxx/table.hpp"

void test_table() {
  // Create a database with the default options.
  auto db = grnxx::open_db("");

  // Create a table named "Table".
  auto table = db->create_table("Table");
  assert(table->db() == db.get());
  assert(table->name() == "Table");
  assert(table->num_columns() == 0);
  assert(!table->key_column());
  assert(table->num_rows() == 0);
  assert(table->max_row_id().is_na());
  assert(table->is_empty());
  assert(table->is_full());

  // Create a column named "Column_1".
  auto column = table->create_column("Column_1", grnxx::BOOL_DATA);
  assert(column->name() == "Column_1");
  assert(table->num_columns() == 1);

  assert(table->get_column(0) == column);
  assert(table->find_column("Column_1") == column);

  // The following create_column() must fail because "Column_1" already exists.
  try {
    table->create_column("Column_1", grnxx::BOOL_DATA);
    assert(false);
  } catch (...) {
  }

  // Create columns named "Column_2" and Column_3".
  table->create_column("Column_2", grnxx::BOOL_DATA);
  table->create_column("Column_3", grnxx::BOOL_DATA);
  assert(table->num_columns() == 3);

  // Remove "Column_2".
  table->remove_column("Column_2");
  assert(table->num_columns() == 2);

  assert(table->get_column(0)->name() == "Column_1");
  assert(table->get_column(1)->name() == "Column_3");

  // Recreate "Column_2".
  table->create_column("Column_2", grnxx::BOOL_DATA);

  // Move "Column_3" to the next to "Column_2".
  table->reorder_column("Column_3", "Column_2");
  assert(table->get_column(0)->name() == "Column_1");
  assert(table->get_column(1)->name() == "Column_2");
  assert(table->get_column(2)->name() == "Column_3");

  // Move "Column_3" to the head.
  table->reorder_column("Column_3", "");
  assert(table->get_column(0)->name() == "Column_3");
  assert(table->get_column(1)->name() == "Column_1");
  assert(table->get_column(2)->name() == "Column_2");

  // Move "Column_2" to the next to "Column3".
  table->reorder_column("Column_2", "Column_3");
  assert(table->get_column(0)->name() == "Column_3");
  assert(table->get_column(1)->name() == "Column_2");
  assert(table->get_column(2)->name() == "Column_1");
}

void test_rows() {
  // Create a table named "Table".
  auto db = grnxx::open_db("");
  auto table = db->create_table("Table");

  // Append the first row.
  grnxx::Int row_id = table->insert_row();
  assert(row_id.raw() == 0);
  assert(table->num_rows() == 1);
  assert(table->max_row_id().match(row_id));
  assert(!table->test_row(grnxx::Int(-1)));
  assert(table->test_row(grnxx::Int(0)));
  assert(!table->test_row(grnxx::Int(1)));

  // Append two more rows.
  assert(table->insert_row().raw() == 1);
  assert(table->insert_row().raw() == 2);
  assert(table->num_rows() == 3);
  assert(table->max_row_id().raw() == 2);
  assert(table->test_row(grnxx::Int(0)));
  assert(table->test_row(grnxx::Int(1)));
  assert(table->test_row(grnxx::Int(2)));
  assert(!table->test_row(grnxx::Int(3)));

  // Remove the second row.
  table->remove_row(grnxx::Int(1));
  assert(table->num_rows() == 2);
  assert(table->max_row_id().raw() == 2);
  assert(table->test_row(grnxx::Int(0)));
  assert(!table->test_row(grnxx::Int(1)));
  assert(table->test_row(grnxx::Int(2)));
  assert(!table->test_row(grnxx::Int(3)));

  // Remove the first row.
  table->remove_row(grnxx::Int(0));
  assert(table->num_rows() == 1);
  assert(table->max_row_id().raw() == 2);
  assert(!table->test_row(grnxx::Int(0)));
  assert(!table->test_row(grnxx::Int(1)));
  assert(table->test_row(grnxx::Int(2)));
  assert(!table->test_row(grnxx::Int(3)));

  // Remove the third row.
  table->remove_row(grnxx::Int(2));
  assert(table->num_rows() == 0);
  assert(table->max_row_id().is_na());
  assert(!table->test_row(grnxx::Int(0)));
  assert(!table->test_row(grnxx::Int(1)));
  assert(!table->test_row(grnxx::Int(2)));
  assert(!table->test_row(grnxx::Int(3)));
}

void test_bitmap() {
  constexpr size_t NUM_ROWS = 1 << 16;

  // Create a table named "Table".
  auto db = grnxx::open_db("");
  auto table = db->create_table("Table");

  // Insert rows.
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int expected_row_id(i);
    assert(table->insert_row().match(expected_row_id));
  }
  assert(table->num_rows() == NUM_ROWS);
  assert(table->max_row_id().raw() == (NUM_ROWS - 1));

  // Remove all rows.
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id(i);
    table->remove_row(row_id);
  }
  assert(table->num_rows() == 0);
  assert(table->max_row_id().is_na());

  // Insert rows again.
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int exptected_row_id(i);
    assert(table->insert_row().match(exptected_row_id));
  }
  assert(table->num_rows() == NUM_ROWS);
  assert(table->max_row_id().raw() == (NUM_ROWS - 1));

  // Remove rows with even IDs.
  for (size_t i = 0; i < NUM_ROWS; i += 2) {
    grnxx::Int row_id(i);
    table->remove_row(row_id);
  }
  assert(table->num_rows() == (NUM_ROWS / 2));
  assert(table->max_row_id().raw() == (NUM_ROWS - 1));

  // Insert rows again.
  for (size_t i = 0; i < NUM_ROWS; i += 2) {
    grnxx::Int exptected_row_id(i);
    assert(table->insert_row().match(exptected_row_id));
  }
  assert(table->num_rows() == NUM_ROWS);
  assert(table->max_row_id().raw() == (NUM_ROWS - 1));

  // Remove rows in reverse order.
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id(NUM_ROWS - i - 1);
    assert(table->max_row_id().match(row_id));
    table->remove_row(row_id);
  }
  assert(table->max_row_id().is_na());

  // Insert rows again.
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int exptected_row_id(i);
    assert(table->insert_row().match(exptected_row_id));
  }
  assert(table->num_rows() == NUM_ROWS);
  assert(table->max_row_id().raw() == (NUM_ROWS - 1));
}

void test_int_key() {
  // Create a table named "Table".
  auto db = grnxx::open_db("");
  auto table = db->create_table("Table");

  // Create a column named "Column".
  auto column = table->create_column("Column", grnxx::INT_DATA);

  // Append three rows.
  grnxx::Int row_id = table->insert_row();
  column->set(row_id, grnxx::Int(1));
  row_id = table->insert_row();
  column->set(row_id, grnxx::Int(10));
  row_id = table->insert_row();
  column->set(row_id, grnxx::Int(100));

  // Set key column.
  table->set_key_column("Column");
  assert(table->key_column() == column);

  // Duplicate keys must be rejected.
  bool inserted = true;
  row_id = table->find_or_insert_row(grnxx::Int(1), &inserted);
  assert(row_id.match(grnxx::Int(0)));
  assert(!inserted);
  row_id = table->find_or_insert_row(grnxx::Int(10), &inserted);
  assert(row_id.match(grnxx::Int(1)));
  assert(!inserted);
  row_id = table->find_or_insert_row(grnxx::Int(100), &inserted);
  assert(row_id.match(grnxx::Int(2)));
  assert(!inserted);

  // Append new keys.
  grnxx::Datum datum;
  row_id = table->find_or_insert_row(grnxx::Int(2), &inserted);
  assert(inserted);
  column->get(row_id, &datum);
  assert(datum.as_int().raw() == 2);
  row_id = table->find_or_insert_row(grnxx::Int(20), &inserted);
  column->get(row_id, &datum);
  assert(datum.as_int().raw() == 20);
  row_id = table->find_or_insert_row(grnxx::Int(200), &inserted);
  column->get(row_id, &datum);
  assert(datum.as_int().raw() == 200);
  row_id = table->find_or_insert_row(grnxx::Int(200000), &inserted);
  column->get(row_id, &datum);
  assert(datum.as_int().raw() == 200000);
  row_id = table->find_or_insert_row(grnxx::Int(20000000000L), &inserted);
  column->get(row_id, &datum);
  assert(datum.as_int().raw() == 20000000000L);

  // Find rows by key.
  assert(table->find_row(grnxx::Int(1)).raw() == 0);
  assert(table->find_row(grnxx::Int(10)).raw() == 1);
  assert(table->find_row(grnxx::Int(100)).raw() == 2);
  assert(table->find_row(grnxx::Int(2)).raw() == 3);
  assert(table->find_row(grnxx::Int(20)).raw() == 4);
  assert(table->find_row(grnxx::Int(200)).raw() == 5);
  assert(table->find_row(grnxx::Int(200000)).raw() == 6);
  assert(table->find_row(grnxx::Int(20000000000L)).raw() == 7);
  assert(table->find_row(grnxx::Int::na()).is_na());

  // Unset key column.
  table->unset_key_column();
  assert(!table->key_column());
}

void test_text_key() {
  // Create a table named "Table".
  auto db = grnxx::open_db("");
  auto table = db->create_table("Table");

  // Create a column named "Column".
  auto column = table->create_column("Column", grnxx::TEXT_DATA);

  // Append three rows.
  grnxx::Int row_id = table->insert_row();
  column->set(row_id, grnxx::Text("1"));
  row_id = table->insert_row();
  column->set(row_id, grnxx::Text("12"));
  row_id = table->insert_row();
  column->set(row_id, grnxx::Text("123"));

  // Set key column.
  table->set_key_column("Column");
  assert(table->key_column() == column);

  // Duplicate keys must be rejected.
  bool inserted = true;
  row_id = table->find_or_insert_row(grnxx::Text("1"), &inserted);
  assert(row_id.raw() == 0);
  assert(!inserted);
  row_id = table->find_or_insert_row(grnxx::Text("12"), &inserted);
  assert(row_id.raw() == 1);
  assert(!inserted);
  row_id = table->find_or_insert_row(grnxx::Text("123"), &inserted);
  assert(row_id.raw() == 2);
  assert(!inserted);

  // Append new keys.
  grnxx::Datum datum;
  row_id = table->find_or_insert_row(grnxx::Text("A"), &inserted);
  assert(row_id.raw() == 3);
  assert(inserted);
  row_id = table->find_or_insert_row(grnxx::Text("AB"), &inserted);
  assert(row_id.raw() == 4);
  assert(inserted);
  row_id = table->find_or_insert_row(grnxx::Text("ABC"), &inserted);
  assert(row_id.raw() == 5);
  assert(inserted);

  // Find rows by key.
  assert(table->find_row(grnxx::Text("1")).raw() == 0);
  assert(table->find_row(grnxx::Text("12")).raw() == 1);
  assert(table->find_row(grnxx::Text("123")).raw() == 2);
  assert(table->find_row(grnxx::Text("A")).raw() == 3);
  assert(table->find_row(grnxx::Text("AB")).raw() == 4);
  assert(table->find_row(grnxx::Text("ABC")).raw() == 5);
  assert(table->find_row(grnxx::Text::na()).is_na());

  // Unset key column.
  table->unset_key_column();
  assert(!table->key_column());
}

void test_cursor() {
  // Create a table named "Table".
  auto db = grnxx::open_db("");
  auto table = db->create_table("Table");

  // Insert rows.
  constexpr size_t NUM_ROWS = 1 << 16;
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    table->insert_row();
  }

  // Test a cursor with the default options.
  auto cursor = table->create_cursor();
  grnxx::Array<grnxx::Record> records;
  assert(cursor->read(0, &records) == 0);
  assert(records.is_empty());
  assert(cursor->read(NUM_ROWS / 2, &records) == (NUM_ROWS / 2));
  assert(records.size() == (NUM_ROWS / 2));
  assert(cursor->read_all(&records) == (NUM_ROWS / 2));
  assert(records.size() == NUM_ROWS);
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int exptected_row_id(i);
    assert(records[i].row_id.match(exptected_row_id));
    assert(records[i].score.raw() == 0.0);
  }
  records.clear();

  // Test a cursor that scans a table in reverse order.
  grnxx::CursorOptions cursor_options;
  cursor_options.order_type = grnxx::CURSOR_REVERSE_ORDER;
  cursor = table->create_cursor(cursor_options);
  assert(cursor->read_all(&records) == NUM_ROWS);
  assert(records.size() == NUM_ROWS);
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int exptected_row_id(NUM_ROWS - i - 1);
    assert(records[i].row_id.match(exptected_row_id));
    assert(records[i].score.raw() == 0.0);
  }
  records.clear();

  // Remove 98.4375% rows.
  std::mt19937 rng;
  std::vector<grnxx::Int> row_ids;
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id(i);
    if ((rng() % 64) != 0) {
      table->remove_row(row_id);
    } else {
      row_ids.push_back(row_id);
    }
  }

  // Test a cursor with the default options.
  cursor = table->create_cursor();
  assert(cursor->read_all(&records) == row_ids.size());
  for (size_t i = 0; i < row_ids.size(); ++i) {
    assert(records[i].row_id.match(row_ids[i]));
  }
  records.clear();

  // Test a cursor that scans a table in reverse order.
  cursor = table->create_cursor(cursor_options);
  assert(cursor->read_all(&records) == row_ids.size());
  for (size_t i = 0; i < row_ids.size(); ++i) {
    assert(records[i].row_id.match(row_ids[row_ids.size() - i - 1]));
  }
  records.clear();
}

void test_reference() {
  // Create tables.
  auto db = grnxx::open_db("");
  auto to_table = db->create_table("To");
  auto from_table = db->create_table("From");

  // Create a column named "Ref".
  grnxx::ColumnOptions options;
  options.reference_table_name = "To";
  auto ref_column = from_table->create_column("Ref", grnxx::INT_DATA, options);

  // Append rows.
  to_table->insert_row();
  to_table->insert_row();
  to_table->insert_row();
  from_table->insert_row();
  from_table->insert_row();
  from_table->insert_row();

  ref_column->set(grnxx::Int(0), grnxx::Int(0));
  ref_column->set(grnxx::Int(1), grnxx::Int(1));
  ref_column->set(grnxx::Int(2), grnxx::Int(1));

  // TODO: "from_table" may be updated in "to_table->remove_row()".

  to_table->remove_row(grnxx::Int(0));

  grnxx::Datum datum;
  ref_column->get(grnxx::Int(0), &datum);
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.as_int().raw() == 0);
  ref_column->get(grnxx::Int(1), &datum);
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.as_int().raw() == 1);
  ref_column->get(grnxx::Int(2), &datum);
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.as_int().raw() == 1);

  to_table->remove_row(grnxx::Int(1));

  ref_column->get(grnxx::Int(0), &datum);
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.as_int().raw() == 0);
  ref_column->get(grnxx::Int(1), &datum);
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.as_int().raw() == 1);
  ref_column->get(grnxx::Int(2), &datum);
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.as_int().raw() == 1);
}

int main() {
  test_table();
  test_rows();
  test_bitmap();
  test_int_key();
  test_text_key();
  test_cursor();
  test_reference();
  return 0;
}
