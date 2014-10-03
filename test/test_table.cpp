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
#include "grnxx/db.hpp"
#include "grnxx/table.hpp"

void test_table() {
  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table named "Table".
  auto table = db->create_table(&error, "Table");
  assert(table);
  assert(table->db() == db.get());
  assert(table->name() == "Table");
  assert(table->num_columns() == 0);
  assert(!table->key_column());
  assert(table->num_rows() == 0);
  assert(table->max_row_id() == 0);

  // Create a column named "Column_1".
  auto column = table->create_column(&error, "Column_1", grnxx::BOOL_DATA);
  assert(column);
  assert(column->name() == "Column_1");
  assert(table->num_columns() == 1);

  assert(table->get_column(0) == column);
  assert(table->find_column(&error, "Column_1") == column);

  // The following create_column() must fail because "Column_1" already exists.
  assert(!table->create_column(&error, "Column_1", grnxx::BOOL_DATA));

  // Create columns named "Column_2" and Column_3".
  assert(table->create_column(&error, "Column_2", grnxx::BOOL_DATA));
  assert(table->create_column(&error, "Column_3", grnxx::BOOL_DATA));
  assert(table->num_columns() == 3);

  // Remove "Column_2".
  assert(table->remove_column(&error, "Column_2"));
  assert(table->num_columns() == 2);

  assert(table->get_column(0)->name() == "Column_1");
  assert(table->get_column(1)->name() == "Column_3");

  // Recreate "Column_2".
  assert(table->create_column(&error, "Column_2", grnxx::BOOL_DATA));

  // Move "Column_3" to the next to "Column_2".
  assert(table->reorder_column(&error, "Column_3", "Column_2"));
  assert(table->get_column(0)->name() == "Column_1");
  assert(table->get_column(1)->name() == "Column_2");
  assert(table->get_column(2)->name() == "Column_3");

  // Move "Column_3" to the head.
  assert(table->reorder_column(&error, "Column_3", ""));
  assert(table->get_column(0)->name() == "Column_3");
  assert(table->get_column(1)->name() == "Column_1");
  assert(table->get_column(2)->name() == "Column_2");

  // Move "Column_2" to the next to "Column3".
  assert(table->reorder_column(&error, "Column_2", "Column_3"));
  assert(table->get_column(0)->name() == "Column_3");
  assert(table->get_column(1)->name() == "Column_2");
  assert(table->get_column(2)->name() == "Column_1");
}

void test_rows() {
  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table named "Table".
  auto table = db->create_table(&error, "Table");
  assert(table);

  // Append the first row.
  grnxx::Int row_id;
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));
  assert(row_id == 1);
  assert(table->num_rows() == 1);
  assert(table->max_row_id() == 1);
  assert(!table->test_row(&error, 0));
  assert(table->test_row(&error, 1));
  assert(!table->test_row(&error, 2));

  // Append two more rows.
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));
  assert(row_id == 3);
  assert(table->num_rows() == 3);
  assert(table->max_row_id() == 3);
  assert(!table->test_row(&error, 0));
  assert(table->test_row(&error, 1));
  assert(table->test_row(&error, 2));
  assert(table->test_row(&error, 3));
  assert(!table->test_row(&error, 4));

  // Remove the 2nd row.
  assert(table->remove_row(&error, 2));
  assert(table->num_rows() == 2);
  assert(table->max_row_id() == 3);
  assert(!table->test_row(&error, 0));
  assert(table->test_row(&error, 1));
  assert(!table->test_row(&error, 2));
  assert(table->test_row(&error, 3));
  assert(!table->test_row(&error, 4));
}

void test_bitmap() {
  constexpr int NUM_ROWS = 1 << 16;

  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table named "Table".
  auto table = db->create_table(&error, "Table");
  assert(table);

  // Create rows.
  for (int i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    assert(row_id == (i + 1));
  }
  assert(table->num_rows() == NUM_ROWS);
  assert(table->max_row_id() == NUM_ROWS);

  // Remove all rows.
  for (int i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = i + 1;
    assert(table->remove_row(&error, row_id));
  }
  assert(table->num_rows() == 0);
  assert(table->max_row_id() == (grnxx::MIN_ROW_ID - 1));

  // Recreate rows.
  for (int i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    assert(row_id == (i + 1));
  }
  assert(table->num_rows() == NUM_ROWS);
  assert(table->max_row_id() == NUM_ROWS);

  // Remove rows with odd IDs.
  for (int i = 0; i < NUM_ROWS; i += 2) {
    grnxx::Int row_id = i + 1;
    assert(table->remove_row(&error, row_id));
  }
  assert(table->num_rows() == (NUM_ROWS / 2));
  assert(table->max_row_id() == NUM_ROWS);

  // Recreate rows.
  for (int i = 0; i < NUM_ROWS; i += 2) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    assert(row_id == (i + 1));
  }
  assert(table->num_rows() == NUM_ROWS);
  assert(table->max_row_id() == NUM_ROWS);

  // Remove rows in reverse order.
  for (int i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = NUM_ROWS - i;
    assert(table->remove_row(&error, row_id));
    assert(table->max_row_id() == (row_id - 1));
  }

  // Recreate rows.
  for (int i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    assert(row_id == (i + 1));
  }
  assert(table->num_rows() == NUM_ROWS);
  assert(table->max_row_id() == NUM_ROWS);
}

void test_int_key() {
  // TODO: find_row() is not supported yet.
  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table named "Table".
  auto table = db->create_table(&error, "Table");
  assert(table);

  // Create a column named "Column".
  auto column = table->create_column(&error, "Column", grnxx::INT_DATA);
  assert(column);

  // Append three rows.
  grnxx::Int row_id;
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));
  assert(column->set(&error, row_id, grnxx::Int(1)));
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));
  assert(column->set(&error, row_id, grnxx::Int(10)));
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));
  assert(column->set(&error, row_id, grnxx::Int(100)));

  // Set key column.
  assert(table->set_key_column(&error, "Column"));
  assert(table->key_column() == column);

  // Duplicate keys must be rejected.
  assert(!table->insert_row(&error, grnxx::NULL_ROW_ID,
                            grnxx::Int(1), &row_id));
  assert(row_id == 1);
  assert(!table->insert_row(&error, grnxx::NULL_ROW_ID,
                            grnxx::Int(10), &row_id));
  assert(row_id == 2);
  assert(!table->insert_row(&error, grnxx::NULL_ROW_ID,
                            grnxx::Int(100), &row_id));
  assert(row_id == 3);

  // Append new keys.
  grnxx::Datum datum;
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Int(2), &row_id));
  assert(column->get(&error, row_id, &datum));
  assert(datum.force_int() == 2);
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Int(20), &row_id));
  assert(column->get(&error, row_id, &datum));
  assert(datum.force_int() == 20);
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Int(200), &row_id));
  assert(column->get(&error, row_id, &datum));
  assert(datum.force_int() == 200);

  // Find rows by key.
  assert(table->find_row(&error, grnxx::Int(1)) == 1);
  assert(table->find_row(&error, grnxx::Int(10)) == 2);
  assert(table->find_row(&error, grnxx::Int(100)) == 3);
  assert(table->find_row(&error, grnxx::Int(2)) == 4);
  assert(table->find_row(&error, grnxx::Int(20)) == 5);
  assert(table->find_row(&error, grnxx::Int(200)) == 6);

  // Unset key column.
  assert(table->unset_key_column(&error));
  assert(!table->key_column());
}

void test_text_key() {
  // TODO: find_row() is not supported yet.
  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table named "Table".
  auto table = db->create_table(&error, "Table");
  assert(table);

  // Create a column named "Column".
  auto column = table->create_column(&error, "Column", grnxx::TEXT_DATA);
  assert(column);

  // Append three rows.
  grnxx::Int row_id;
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));
  assert(column->set(&error, row_id, grnxx::Text("1")));
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));
  assert(column->set(&error, row_id, grnxx::Text("12")));
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));
  assert(column->set(&error, row_id, grnxx::Text("123")));

  // Set key column.
  assert(table->set_key_column(&error, "Column"));
  assert(table->key_column() == column);

  // Duplicate keys must be rejected.
  assert(!table->insert_row(&error, grnxx::NULL_ROW_ID,
                            grnxx::Text("1"), &row_id));
  assert(row_id == 1);
  assert(!table->insert_row(&error, grnxx::NULL_ROW_ID,
                            grnxx::Text("12"), &row_id));
  assert(row_id == 2);
  assert(!table->insert_row(&error, grnxx::NULL_ROW_ID,
                            grnxx::Text("123"), &row_id));
  assert(row_id == 3);

  // Append new keys.
  grnxx::Datum datum;
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Text("A"), &row_id));
  assert(column->get(&error, row_id, &datum));
  assert(datum.force_text() == "A");
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Text("AB"), &row_id));
  assert(column->get(&error, row_id, &datum));
  assert(datum.force_text() == "AB");
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Text("ABC"), &row_id));
  assert(column->get(&error, row_id, &datum));
  assert(datum.force_text() == "ABC");

  // Find rows by key.
  assert(table->find_row(&error, grnxx::Text("1")) == 1);
  assert(table->find_row(&error, grnxx::Text("12")) == 2);
  assert(table->find_row(&error, grnxx::Text("123")) == 3);
  assert(table->find_row(&error, grnxx::Text("A")) == 4);
  assert(table->find_row(&error, grnxx::Text("AB")) == 5);
  assert(table->find_row(&error, grnxx::Text("ABC")) == 6);

  // Unset key column.
  assert(table->unset_key_column(&error));
  assert(!table->key_column());
}

void test_cursor() {
  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table named "Table".
  auto table = db->create_table(&error, "Table");
  assert(table);

  // Create a column named "Column".
  assert(table->create_column(&error, "Column", grnxx::BOOL_DATA));

  // Append three rows and remove the 2nd row.
  grnxx::Int row_id;
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));
  assert(table->remove_row(&error, 2));

  // Create a cursor with the default options.
  auto cursor = table->create_cursor(&error);
  assert(cursor);

  // Read records from the cursor.
  grnxx::Array<grnxx::Record> records;
  auto result = cursor->read(&error, 0, &records);
  assert(result.is_ok);
  assert(result.count == 0);

  result = cursor->read(&error, 1, &records);
  assert(result.is_ok);
  assert(result.count == 1);
  assert(records.size() == 1);
  assert(records.get(0).row_id == 1);

  result = cursor->read(&error, 2, &records);
  assert(result.is_ok);
  assert(result.count == 1);
  assert(records.size() == 2);
  assert(records.get(0).row_id == 1);
  assert(records.get(1).row_id == 3);

  records.clear();

  // Create a cursor that scans a table in reverse order.
  grnxx::CursorOptions cursor_options;
  cursor_options.order_type = grnxx::REVERSE_ORDER;
  cursor = table->create_cursor(&error, cursor_options);
  assert(cursor);

  result = cursor->read_all(&error, &records);
  assert(result.is_ok);
  assert(result.count == 2);
  assert(records.size() == 2);
  assert(records.get(0).row_id == 3);
  assert(records.get(1).row_id == 1);

  records.clear();

  cursor = table->create_cursor(&error, cursor_options);
  assert(cursor);

  result = cursor->read(&error, 1, &records);
  assert(result.is_ok);
  assert(result.count == 1);
  assert(records.size() == 1);
  assert(records.get(0).row_id == 3);

  result = cursor->read(&error, 2, &records);
  assert(result.is_ok);
  assert(result.count == 1);
  assert(records.size() == 2);
  assert(records.get(0).row_id == 3);
  assert(records.get(1).row_id == 1);
}

void test_reference() {
  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table named "Table".
  auto to_table = db->create_table(&error, "To");
  assert(to_table);
  auto from_table = db->create_table(&error, "From");
  assert(from_table);

  // Create a column named "Ref".
  grnxx::ColumnOptions options;
  options.ref_table_name = "To";
  auto ref_column = from_table->create_column(&error, "Ref", grnxx::INT_DATA,
                                              options);
  assert(ref_column);

  // Append rows.
  grnxx::Int row_id;
  assert(to_table->insert_row(&error, grnxx::NULL_ROW_ID,
                              grnxx::Datum(), &row_id));
  assert(to_table->insert_row(&error, grnxx::NULL_ROW_ID,
                              grnxx::Datum(), &row_id));
  assert(to_table->insert_row(&error, grnxx::NULL_ROW_ID,
                              grnxx::Datum(), &row_id));
  assert(from_table->insert_row(&error, grnxx::NULL_ROW_ID,
                                grnxx::Datum(), &row_id));
  assert(from_table->insert_row(&error, grnxx::NULL_ROW_ID,
                                grnxx::Datum(), &row_id));
  assert(from_table->insert_row(&error, grnxx::NULL_ROW_ID,
                                grnxx::Datum(), &row_id));

  assert(ref_column->set(&error, 1, grnxx::Int(1)));
  assert(ref_column->set(&error, 2, grnxx::Int(2)));
  assert(ref_column->set(&error, 3, grnxx::Int(2)));

  assert(to_table->remove_row(&error, 1));

  grnxx::Datum datum;
  assert(ref_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.force_int() == grnxx::NULL_ROW_ID);
  assert(ref_column->get(&error, 2, &datum));
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.force_int() == 2);
  assert(ref_column->get(&error, 3, &datum));
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.force_int() == 2);

  assert(to_table->remove_row(&error, 2));

  assert(ref_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.force_int() == grnxx::NULL_ROW_ID);
  assert(ref_column->get(&error, 2, &datum));
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.force_int() == grnxx::NULL_ROW_ID);
  assert(ref_column->get(&error, 3, &datum));
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.force_int() == grnxx::NULL_ROW_ID);
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
