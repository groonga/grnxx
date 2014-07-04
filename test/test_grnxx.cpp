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

#include "grnxx/column.hpp"
#include "grnxx/datum.hpp"
#include "grnxx/db.hpp"
#include "grnxx/error.hpp"
#include "grnxx/table.hpp"

namespace {

void test_db() {
  grnxx::Error error;

  auto db = grnxx::open_db(&error, "", grnxx::DBOptions());
  assert(db);
  assert(db->num_tables() == 0);

  auto table = db->create_table(&error, "Table_1", grnxx::TableOptions());
  assert(table);
  assert(table->name() == "Table_1");
  assert(db->num_tables() == 1);

  assert(db->get_table(0) == table);
  assert(db->find_table(&error, "Table_1") == table);

  assert(!db->create_table(&error, "Table_1", grnxx::TableOptions()));

  assert(db->create_table(&error, "Table_2", grnxx::TableOptions()));
  assert(db->create_table(&error, "Table_3", grnxx::TableOptions()));
  assert(db->num_tables() == 3);

  assert(db->remove_table(&error, "Table_2"));
  assert(db->num_tables() == 2);

  assert(db->get_table(0)->name() == "Table_1");
  assert(db->get_table(1)->name() == "Table_3");

  assert(db->create_table(&error, "Table_2", grnxx::TableOptions()));

  assert(db->reorder_table(&error, "Table_3", "Table_2"));
  assert(db->get_table(0)->name() == "Table_1");
  assert(db->get_table(1)->name() == "Table_2");
  assert(db->get_table(2)->name() == "Table_3");

  assert(db->reorder_table(&error, "Table_3", ""));
  assert(db->get_table(0)->name() == "Table_3");
  assert(db->get_table(1)->name() == "Table_1");
  assert(db->get_table(2)->name() == "Table_2");

  assert(db->reorder_table(&error, "Table_2", "Table_3"));
  assert(db->get_table(0)->name() == "Table_3");
  assert(db->get_table(1)->name() == "Table_2");
  assert(db->get_table(2)->name() == "Table_1");
}

void test_table() {
  grnxx::Error error;

  auto db = grnxx::open_db(&error, "", grnxx::DBOptions());
  assert(db);

  auto table = db->create_table(&error, "Table", grnxx::TableOptions());
  assert(table);
  assert(table->db() == db.get());
  assert(table->name() == "Table");
  assert(table->num_columns() == 0);
  assert(!table->key_column());
  assert(table->max_row_id() == 0);

  auto column = table->create_column(&error, "Column_1", grnxx::BOOL_DATA,
                                     grnxx::ColumnOptions());
  assert(column);
  assert(column->name() == "Column_1");
  assert(table->num_columns() == 1);

  assert(table->get_column(0) == column);
  assert(table->find_column(&error, "Column_1") == column);

  assert(!table->create_column(&error, "Column_1", grnxx::BOOL_DATA,
                               grnxx::ColumnOptions()));

  assert(table->create_column(&error, "Column_2", grnxx::BOOL_DATA,
                              grnxx::ColumnOptions()));
  assert(table->create_column(&error, "Column_3", grnxx::BOOL_DATA,
                              grnxx::ColumnOptions()));
  assert(table->num_columns() == 3);

  assert(table->remove_column(&error, "Column_2"));
  assert(table->num_columns() == 2);

  assert(table->get_column(0)->name() == "Column_1");
  assert(table->get_column(1)->name() == "Column_3");

  assert(table->create_column(&error, "Column_2", grnxx::BOOL_DATA,
                              grnxx::ColumnOptions()));

  assert(table->reorder_column(&error, "Column_3", "Column_2"));
  assert(table->get_column(0)->name() == "Column_1");
  assert(table->get_column(1)->name() == "Column_2");
  assert(table->get_column(2)->name() == "Column_3");

  assert(table->reorder_column(&error, "Column_3", ""));
  assert(table->get_column(0)->name() == "Column_3");
  assert(table->get_column(1)->name() == "Column_1");
  assert(table->get_column(2)->name() == "Column_2");

  assert(table->reorder_column(&error, "Column_2", "Column_3"));
  assert(table->get_column(0)->name() == "Column_3");
  assert(table->get_column(1)->name() == "Column_2");
  assert(table->get_column(2)->name() == "Column_1");

  // TODO: set_key_column(), unset_key_column().

  grnxx::Int row_id;
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));
  assert(row_id == 1);
  assert(table->max_row_id() == 1);
  assert(!table->test_row(&error, 0));
  assert(table->test_row(&error, 1));
  assert(!table->test_row(&error, 2));

  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));
  assert(row_id == 3);
  assert(table->max_row_id() == 3);
  assert(!table->test_row(&error, 0));
  assert(table->test_row(&error, 1));
  assert(table->test_row(&error, 2));
  assert(table->test_row(&error, 3));
  assert(!table->test_row(&error, 4));

  assert(table->remove_row(&error, 2));
  assert(table->max_row_id() == 3);
  assert(!table->test_row(&error, 0));
  assert(table->test_row(&error, 1));
  assert(!table->test_row(&error, 2));
  assert(table->test_row(&error, 3));
  assert(!table->test_row(&error, 4));

  // TODO: find_row().

  // TODO: create_cursor().
}

void test_column() {
  grnxx::Error error;

  auto db = grnxx::open_db(&error, "", grnxx::DBOptions());
  assert(db);

  auto table = db->create_table(&error, "Table", grnxx::TableOptions());
  assert(table);

  grnxx::Int row_id;
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));

  auto bool_column = table->create_column(&error, "BoolColumn",
                                          grnxx::BOOL_DATA,
                                          grnxx::ColumnOptions());
  assert(bool_column);
  assert(bool_column->table() == table);
  assert(bool_column->name() == "BoolColumn");
  assert(bool_column->data_type() == grnxx::BOOL_DATA);
  assert(!bool_column->has_key_attribute());
  assert(bool_column->num_indexes() == 0);

  auto int_column = table->create_column(&error, "IntColumn",
                                         grnxx::INT_DATA,
                                         grnxx::ColumnOptions());
  assert(int_column);
  assert(int_column->table() == table);
  assert(int_column->name() == "IntColumn");
  assert(int_column->data_type() == grnxx::INT_DATA);
  assert(!int_column->has_key_attribute());
  assert(int_column->num_indexes() == 0);

  grnxx::Datum datum;

  assert(bool_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::BOOL_DATA);
  assert(!static_cast<grnxx::Bool>(datum));

  assert(int_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::INT_DATA);
  assert(static_cast<grnxx::Int>(datum) == 0);

  assert(bool_column->set(&error, 1, grnxx::Bool(true)));
  assert(int_column->set(&error, 1, grnxx::Int(123)));

  assert(bool_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::BOOL_DATA);
  assert(static_cast<grnxx::Bool>(datum));

  assert(int_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::INT_DATA);
  assert(static_cast<grnxx::Int>(datum) == 123);
}

}  // namespace

int main() {
  test_db();
  test_table();
  test_column();

  return 0;
}
