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

//  // Create a column named "Column_1".
//  auto column = table->create_column(&error, "Column_1", grnxx::BOOL_DATA);
//  assert(column);
//  assert(column->name() == "Column_1");
//  assert(table->num_columns() == 1);

//  assert(table->get_column(0) == column);
//  assert(table->find_column(&error, "Column_1") == column);

//  // The following create_column() must fail because "Column_1" already exists.
//  assert(!table->create_column(&error, "Column_1", grnxx::BOOL_DATA));

//  // Create columns named "Column_2" and Column_3".
//  assert(table->create_column(&error, "Column_2", grnxx::BOOL_DATA));
//  assert(table->create_column(&error, "Column_3", grnxx::BOOL_DATA));
//  assert(table->num_columns() == 3);

//  // Remove "Column_2".
//  assert(table->remove_column(&error, "Column_2"));
//  assert(table->num_columns() == 2);

//  assert(table->get_column(0)->name() == "Column_1");
//  assert(table->get_column(1)->name() == "Column_3");

//  // Recreate "Column_2".
//  assert(table->create_column(&error, "Column_2", grnxx::BOOL_DATA));

//  // Move "Column_3" to the next to "Column_2".
//  assert(table->reorder_column(&error, "Column_3", "Column_2"));
//  assert(table->get_column(0)->name() == "Column_1");
//  assert(table->get_column(1)->name() == "Column_2");
//  assert(table->get_column(2)->name() == "Column_3");

//  // Move "Column_3" to the head.
//  assert(table->reorder_column(&error, "Column_3", ""));
//  assert(table->get_column(0)->name() == "Column_3");
//  assert(table->get_column(1)->name() == "Column_1");
//  assert(table->get_column(2)->name() == "Column_2");

//  // Move "Column_2" to the next to "Column3".
//  assert(table->reorder_column(&error, "Column_2", "Column_3"));
//  assert(table->get_column(0)->name() == "Column_3");
//  assert(table->get_column(1)->name() == "Column_2");
//  assert(table->get_column(2)->name() == "Column_1");
}

void test_rows() {
  // Create a database with the default options.
  auto db = grnxx::open_db("");

  // Create a table named "Table".
  auto table = db->create_table("Table");

  // Append the first row.
  grnxx::Int row_id;
  row_id = table->insert_row();
  assert(row_id == grnxx::Int(0));
  assert(table->num_rows() == 1);
  assert(table->max_row_id() == row_id);
  assert(!table->test_row(grnxx::Int(-1)));
  assert(table->test_row(grnxx::Int(0)));
  assert(!table->test_row(grnxx::Int(1)));

  // Append two more rows.
  assert(table->insert_row() == grnxx::Int(1));
  assert(table->insert_row() == grnxx::Int(2));
  assert(table->num_rows() == 3);
  assert(table->max_row_id() == grnxx::Int(2));
  assert(table->test_row(grnxx::Int(0)));
  assert(table->test_row(grnxx::Int(1)));
  assert(table->test_row(grnxx::Int(2)));
  assert(!table->test_row(grnxx::Int(3)));

  // Remove the second row.
  table->remove_row(grnxx::Int(1));
  assert(table->num_rows() == 2);
  assert(table->max_row_id() == grnxx::Int(2));
  assert(table->test_row(grnxx::Int(0)));
  assert(!table->test_row(grnxx::Int(1)));
  assert(table->test_row(grnxx::Int(2)));
  assert(!table->test_row(grnxx::Int(3)));

  // Remove the first row.
  table->remove_row(grnxx::Int(0));
  assert(table->num_rows() == 1);
  assert(table->max_row_id() == grnxx::Int(2));
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

//void test_bitmap() {
//  constexpr int NUM_ROWS = 1 << 16;

//  grnxx::Error error;

//  // Create a database with the default options.
//  auto db = grnxx::open_db(&error, "");
//  assert(db);

//  // Create a table named "Table".
//  auto table = db->create_table(&error, "Table");
//  assert(table);

//  // Create rows.
//  for (int i = 0; i < NUM_ROWS; ++i) {
//    grnxx::Int row_id;
//    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
//                             grnxx::Datum(), &row_id));
//    assert(row_id == (i + 1));
//  }
//  assert(table->num_rows() == NUM_ROWS);
//  assert(table->max_row_id() == NUM_ROWS);

//  // Remove all rows.
//  for (int i = 0; i < NUM_ROWS; ++i) {
//    grnxx::Int row_id = i + 1;
//    assert(table->remove_row(&error, row_id));
//  }
//  assert(table->num_rows() == 0);
//  assert(table->max_row_id() == (grnxx::MIN_ROW_ID - 1));

//  // Recreate rows.
//  for (int i = 0; i < NUM_ROWS; ++i) {
//    grnxx::Int row_id;
//    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
//                             grnxx::Datum(), &row_id));
//    assert(row_id == (i + 1));
//  }
//  assert(table->num_rows() == NUM_ROWS);
//  assert(table->max_row_id() == NUM_ROWS);

//  // Remove rows with odd IDs.
//  for (int i = 0; i < NUM_ROWS; i += 2) {
//    grnxx::Int row_id = i + 1;
//    assert(table->remove_row(&error, row_id));
//  }
//  assert(table->num_rows() == (NUM_ROWS / 2));
//  assert(table->max_row_id() == NUM_ROWS);

//  // Recreate rows.
//  for (int i = 0; i < NUM_ROWS; i += 2) {
//    grnxx::Int row_id;
//    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
//                             grnxx::Datum(), &row_id));
//    assert(row_id == (i + 1));
//  }
//  assert(table->num_rows() == NUM_ROWS);
//  assert(table->max_row_id() == NUM_ROWS);

//  // Remove rows in reverse order.
//  for (int i = 0; i < NUM_ROWS; ++i) {
//    grnxx::Int row_id = NUM_ROWS - i;
//    assert(table->remove_row(&error, row_id));
//    assert(table->max_row_id() == (row_id - 1));
//  }

//  // Recreate rows.
//  for (int i = 0; i < NUM_ROWS; ++i) {
//    grnxx::Int row_id;
//    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
//                             grnxx::Datum(), &row_id));
//    assert(row_id == (i + 1));
//  }
//  assert(table->num_rows() == NUM_ROWS);
//  assert(table->max_row_id() == NUM_ROWS);
//}

//void test_int_key() {
//  // TODO: find_row() is not supported yet.
//  grnxx::Error error;

//  // Create a database with the default options.
//  auto db = grnxx::open_db(&error, "");
//  assert(db);

//  // Create a table named "Table".
//  auto table = db->create_table(&error, "Table");
//  assert(table);

//  // Create a column named "Column".
//  auto column = table->create_column(&error, "Column", grnxx::INT_DATA);
//  assert(column);

//  // Append three rows.
//  grnxx::Int row_id;
//  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
//                           grnxx::Datum(), &row_id));
//  assert(column->set(&error, row_id, grnxx::Int(1)));
//  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
//                           grnxx::Datum(), &row_id));
//  assert(column->set(&error, row_id, grnxx::Int(10)));
//  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
//                           grnxx::Datum(), &row_id));
//  assert(column->set(&error, row_id, grnxx::Int(100)));

//  // Set key column.
//  assert(table->set_key_column(&error, "Column"));
//  assert(table->key_column() == column);

//  // Duplicate keys must be rejected.
//  assert(!table->insert_row(&error, grnxx::NULL_ROW_ID,
//                            grnxx::Int(1), &row_id));
//  assert(row_id == 1);
//  assert(!table->insert_row(&error, grnxx::NULL_ROW_ID,
//                            grnxx::Int(10), &row_id));
//  assert(row_id == 2);
//  assert(!table->insert_row(&error, grnxx::NULL_ROW_ID,
//                            grnxx::Int(100), &row_id));
//  assert(row_id == 3);

//  // Append new keys.
//  grnxx::Datum datum;
//  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
//                           grnxx::Int(2), &row_id));
//  assert(column->get(&error, row_id, &datum));
//  assert(datum.force_int() == 2);
//  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
//                           grnxx::Int(20), &row_id));
//  assert(column->get(&error, row_id, &datum));
//  assert(datum.force_int() == 20);
//  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
//                           grnxx::Int(200), &row_id));
//  assert(column->get(&error, row_id, &datum));
//  assert(datum.force_int() == 200);

//  // Find rows by key.
//  assert(table->find_row(&error, grnxx::Int(1)) == 1);
//  assert(table->find_row(&error, grnxx::Int(10)) == 2);
//  assert(table->find_row(&error, grnxx::Int(100)) == 3);
//  assert(table->find_row(&error, grnxx::Int(2)) == 4);
//  assert(table->find_row(&error, grnxx::Int(20)) == 5);
//  assert(table->find_row(&error, grnxx::Int(200)) == 6);

//  // Unset key column.
//  assert(table->unset_key_column(&error));
//  assert(!table->key_column());
//}

//void test_text_key() {
//  // TODO: find_row() is not supported yet.
//  grnxx::Error error;

//  // Create a database with the default options.
//  auto db = grnxx::open_db(&error, "");
//  assert(db);

//  // Create a table named "Table".
//  auto table = db->create_table(&error, "Table");
//  assert(table);

//  // Create a column named "Column".
//  auto column = table->create_column(&error, "Column", grnxx::TEXT_DATA);
//  assert(column);

//  // Append three rows.
//  grnxx::Int row_id;
//  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
//                           grnxx::Datum(), &row_id));
//  assert(column->set(&error, row_id, grnxx::Text("1")));
//  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
//                           grnxx::Datum(), &row_id));
//  assert(column->set(&error, row_id, grnxx::Text("12")));
//  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
//                           grnxx::Datum(), &row_id));
//  assert(column->set(&error, row_id, grnxx::Text("123")));

//  // Set key column.
//  assert(table->set_key_column(&error, "Column"));
//  assert(table->key_column() == column);

//  // Duplicate keys must be rejected.
//  assert(!table->insert_row(&error, grnxx::NULL_ROW_ID,
//                            grnxx::Text("1"), &row_id));
//  assert(row_id == 1);
//  assert(!table->insert_row(&error, grnxx::NULL_ROW_ID,
//                            grnxx::Text("12"), &row_id));
//  assert(row_id == 2);
//  assert(!table->insert_row(&error, grnxx::NULL_ROW_ID,
//                            grnxx::Text("123"), &row_id));
//  assert(row_id == 3);

//  // Append new keys.
//  grnxx::Datum datum;
//  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
//                           grnxx::Text("A"), &row_id));
//  assert(column->get(&error, row_id, &datum));
//  assert(datum.force_text() == "A");
//  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
//                           grnxx::Text("AB"), &row_id));
//  assert(column->get(&error, row_id, &datum));
//  assert(datum.force_text() == "AB");
//  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
//                           grnxx::Text("ABC"), &row_id));
//  assert(column->get(&error, row_id, &datum));
//  assert(datum.force_text() == "ABC");

//  // Find rows by key.
//  assert(table->find_row(&error, grnxx::Text("1")) == 1);
//  assert(table->find_row(&error, grnxx::Text("12")) == 2);
//  assert(table->find_row(&error, grnxx::Text("123")) == 3);
//  assert(table->find_row(&error, grnxx::Text("A")) == 4);
//  assert(table->find_row(&error, grnxx::Text("AB")) == 5);
//  assert(table->find_row(&error, grnxx::Text("ABC")) == 6);

//  // Unset key column.
//  assert(table->unset_key_column(&error));
//  assert(!table->key_column());
//}

void test_cursor() {
  // Create a database with the default options.
  auto db = grnxx::open_db("");

  // Create a table named "Table".
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
    assert(records[i].row_id == grnxx::Int(i));
    assert(records[i].score == grnxx::Float(0.0));
  }
  records.clear();

  // Test a cursor that scans a table in reverse order.
  grnxx::CursorOptions cursor_options;
  cursor_options.order_type = grnxx::CURSOR_REVERSE_ORDER;
  cursor = table->create_cursor(cursor_options);
  assert(cursor->read_all(&records) == NUM_ROWS);
  assert(records.size() == NUM_ROWS);
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    assert(records[i].row_id == grnxx::Int(NUM_ROWS - i - 1));
    assert(records[i].score == grnxx::Float(0.0));
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
    assert(records[i].row_id == row_ids[i]);
  }
  records.clear();

  // Test a cursor that scans a table in reverse order.
  cursor = table->create_cursor(cursor_options);
  assert(cursor->read_all(&records) == row_ids.size());
  for (size_t i = 0; i < row_ids.size(); ++i) {
    assert(records[i].row_id == row_ids[row_ids.size() - i - 1]);
  }
  records.clear();
}

//void test_reference() {
//  grnxx::Error error;

//  // Create a database with the default options.
//  auto db = grnxx::open_db(&error, "");
//  assert(db);

//  // Create a table named "Table".
//  auto to_table = db->create_table(&error, "To");
//  assert(to_table);
//  auto from_table = db->create_table(&error, "From");
//  assert(from_table);

//  // Create a column named "Ref".
//  grnxx::ColumnOptions options;
//  options.ref_table_name = "To";
//  auto ref_column = from_table->create_column(&error, "Ref", grnxx::INT_DATA,
//                                              options);
//  assert(ref_column);

//  // Append rows.
//  grnxx::Int row_id;
//  assert(to_table->insert_row(&error, grnxx::NULL_ROW_ID,
//                              grnxx::Datum(), &row_id));
//  assert(to_table->insert_row(&error, grnxx::NULL_ROW_ID,
//                              grnxx::Datum(), &row_id));
//  assert(to_table->insert_row(&error, grnxx::NULL_ROW_ID,
//                              grnxx::Datum(), &row_id));
//  assert(from_table->insert_row(&error, grnxx::NULL_ROW_ID,
//                                grnxx::Datum(), &row_id));
//  assert(from_table->insert_row(&error, grnxx::NULL_ROW_ID,
//                                grnxx::Datum(), &row_id));
//  assert(from_table->insert_row(&error, grnxx::NULL_ROW_ID,
//                                grnxx::Datum(), &row_id));

//  assert(ref_column->set(&error, 1, grnxx::Int(1)));
//  assert(ref_column->set(&error, 2, grnxx::Int(2)));
//  assert(ref_column->set(&error, 3, grnxx::Int(2)));

//  assert(to_table->remove_row(&error, 1));

//  grnxx::Datum datum;
//  assert(ref_column->get(&error, 1, &datum));
//  assert(datum.type() == grnxx::INT_DATA);
//  assert(datum.force_int() == grnxx::NULL_ROW_ID);
//  assert(ref_column->get(&error, 2, &datum));
//  assert(datum.type() == grnxx::INT_DATA);
//  assert(datum.force_int() == 2);
//  assert(ref_column->get(&error, 3, &datum));
//  assert(datum.type() == grnxx::INT_DATA);
//  assert(datum.force_int() == 2);

//  assert(to_table->remove_row(&error, 2));

//  assert(ref_column->get(&error, 1, &datum));
//  assert(datum.type() == grnxx::INT_DATA);
//  assert(datum.force_int() == grnxx::NULL_ROW_ID);
//  assert(ref_column->get(&error, 2, &datum));
//  assert(datum.type() == grnxx::INT_DATA);
//  assert(datum.force_int() == grnxx::NULL_ROW_ID);
//  assert(ref_column->get(&error, 3, &datum));
//  assert(datum.type() == grnxx::INT_DATA);
//  assert(datum.force_int() == grnxx::NULL_ROW_ID);
//}

int main() {
  test_table();
  test_rows();
//  test_bitmap();
//  test_int_key();
//  test_text_key();
  test_cursor();
//  test_reference();
  return 0;
}
