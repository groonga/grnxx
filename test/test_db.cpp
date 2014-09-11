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

#include "grnxx/db.hpp"
#include "grnxx/table.hpp"

void test_db() {
  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);
  assert(db->num_tables() == 0);

  // Create a table named "Table_1".
  auto table = db->create_table(&error, "Table_1");
  assert(table);
  assert(table->name() == "Table_1");
  assert(db->num_tables() == 1);

  assert(db->get_table(0) == table);
  assert(db->find_table(&error, "Table_1") == table);

  // The following create_table() must fail because "Table_1" already exists.
  assert(!db->create_table(&error, "Table_1"));

  // Create tables named "Table_2" and "Table_3".
  assert(db->create_table(&error, "Table_2"));
  assert(db->create_table(&error, "Table_3"));
  assert(db->num_tables() == 3);

  // Remove "Table_2".
  assert(db->remove_table(&error, "Table_2"));
  assert(db->num_tables() == 2);

  assert(db->get_table(0)->name() == "Table_1");
  assert(db->get_table(1)->name() == "Table_3");

  // Recreate "Table_2".
  assert(db->create_table(&error, "Table_2"));

  // Move "Table_3" to the next to "Table_2".
  assert(db->reorder_table(&error, "Table_3", "Table_2"));
  assert(db->get_table(0)->name() == "Table_1");
  assert(db->get_table(1)->name() == "Table_2");
  assert(db->get_table(2)->name() == "Table_3");

  // Move "Table_3" to the head.
  assert(db->reorder_table(&error, "Table_3", ""));
  assert(db->get_table(0)->name() == "Table_3");
  assert(db->get_table(1)->name() == "Table_1");
  assert(db->get_table(2)->name() == "Table_2");

  // Move "Table_2" to the next to "Table_3".
  assert(db->reorder_table(&error, "Table_2", "Table_3"));
  assert(db->get_table(0)->name() == "Table_3");
  assert(db->get_table(1)->name() == "Table_2");
  assert(db->get_table(2)->name() == "Table_1");
}

int main() {
  test_db();
  return 0;
}
