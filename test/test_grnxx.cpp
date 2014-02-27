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

#include "grnxx/library.hpp"
#include <cassert>
#include <iostream>
#include <random>
#include <vector>

#include "grnxx/calc.hpp"
#include "grnxx/column.hpp"
#include "grnxx/column_impl.hpp"
#include "grnxx/database.hpp"
#include "grnxx/index.hpp"
#include "grnxx/sorter.hpp"
#include "grnxx/table.hpp"

namespace {

void test_database() {
  grnxx::Database database;

  assert(database.min_table_id() == grnxx::MIN_TABLE_ID);
  assert(database.max_table_id() == (grnxx::MIN_TABLE_ID - 1));

  grnxx::Table *table = database.create_table("Table_1");
  assert(table);

  grnxx::TableID table_id = table->id();
  assert(table_id == grnxx::MIN_TABLE_ID);
  grnxx::String table_name = table->name();
  assert(table_name == "Table_1");

  table = database.get_table_by_id(table_id);
  assert(table->id() == table_id);
  assert(table->name() == table_name);

  table = database.get_table_by_name(table_name);
  assert(table->id() == table_id);
  assert(table->name() == table_name);

  assert(!database.create_table("Table_1"));

  assert(database.create_table("Table_2"));
  assert(database.create_table("Table_3"));
  assert(database.drop_table("Table_2"));

  std::vector<grnxx::Table *> tables;
  assert(database.get_tables(&tables) == 2);

  assert(tables[0]->name() == "Table_1");
  assert(tables[1]->name() == "Table_3");

  assert(database.min_table_id() == tables[0]->id());
  assert(database.max_table_id() == tables[1]->id());
}

void test_table() {
  grnxx::Database database;

  grnxx::Table *table = database.create_table("Table");
  assert(table);

  assert(table->min_column_id() == grnxx::MIN_COLUMN_ID);
  assert(table->max_column_id() == (grnxx::MIN_COLUMN_ID - 1));

  grnxx::Column *column = table->create_column("Column_1", grnxx::INTEGER);
  assert(column);

  grnxx::ColumnID column_id = column->id();
  assert(column_id == grnxx::MIN_COLUMN_ID);
  grnxx::String column_name = column->name();
  assert(column_name == "Column_1");

  column = table->get_column_by_id(column_id);
  assert(column->id() == column_id);
  assert(column->name() == column_name);

  column = table->get_column_by_name(column_name);
  assert(column->id() == column_id);
  assert(column->name() == column_name);

  grnxx::Index *index =
      table->create_index("Index_1", "Column_1", grnxx::TREE_MAP);
  assert(index);

  grnxx::IndexID index_id = index->id();
  assert(index_id == grnxx::MIN_INDEX_ID);
  grnxx::String index_name = index->name();
  assert(index_name == "Index_1");

  index = table->get_index_by_id(index_id);
  assert(index->id() == index_id);
  assert(index->name() == index_name);

  index = table->get_index_by_name(index_name);
  assert(index->id() == index_id);
  assert(index->name() == index_name);

  assert(!table->create_index("Index_1", "Column_1", grnxx::TREE_MAP));

  assert(table->create_column("Column_2", grnxx::FLOAT));
  assert(table->create_column("Column_3", grnxx::STRING));
  assert(table->create_index("Index_2", "Column_2", grnxx::TREE_MAP));
  assert(table->create_index("Index_3", "Column_3", grnxx::TREE_MAP));
  assert(table->drop_column("Column_2"));
  assert(table->drop_index("Index_3"));

  std::vector<grnxx::Column *> columns;
  assert(table->get_columns(&columns) == 2);

  assert(columns[0]->name() == "Column_1");
  assert(columns[1]->name() == "Column_3");

  assert(table->min_column_id() == columns[0]->id());
  assert(table->max_column_id() == columns[1]->id());

  std::vector<grnxx::Index *> indexes;
  assert(table->get_indexes(&indexes) == 1);

  assert(indexes[0]->name() == "Index_1");

  for (grnxx::Int64 i = 0; i < 100; ++i) {
    assert(table->insert_row() == (grnxx::MIN_ROW_ID + i));
    assert(table->min_row_id() == grnxx::MIN_ROW_ID);
    assert(table->max_row_id() == (grnxx::MIN_ROW_ID + i));
  }

  std::unique_ptr<grnxx::RowIDCursor> cursor(table->create_cursor());
  assert(cursor);

  std::vector<grnxx::RowID> row_ids;
  assert(cursor->get_next(10, &row_ids) == 10);
  assert(cursor->get_next(100, &row_ids) == 90);
  for (grnxx::Int64 i = 0; i < 100; ++i) {
    assert(row_ids[i] == (grnxx::MIN_ROW_ID + i));
  }
}

void test_column() {
  grnxx::Database database;

  grnxx::Table *table = database.create_table("Table");
  assert(table);

  auto boolean_column = dynamic_cast<grnxx::ColumnImpl<grnxx::Boolean> *>(
      table->create_column("Boolean", grnxx::BOOLEAN));
  assert(boolean_column);
  assert(boolean_column->name() == "Boolean");
  assert(boolean_column->data_type() == grnxx::BOOLEAN);

  auto integer_column = dynamic_cast<grnxx::ColumnImpl<grnxx::Int64> *>(
      table->create_column("Integer", grnxx::INTEGER));
  assert(integer_column);
  assert(integer_column->name() == "Integer");
  assert(integer_column->data_type() == grnxx::INTEGER);

  auto float_column = dynamic_cast<grnxx::ColumnImpl<grnxx::Float> *>(
      table->create_column("Float", grnxx::FLOAT));
  assert(float_column);
  assert(float_column->name() == "Float");
  assert(float_column->data_type() == grnxx::FLOAT);

  auto string_column = dynamic_cast<grnxx::ColumnImpl<grnxx::String> *>(
      table->create_column("String", grnxx::STRING));
  assert(string_column);
  assert(string_column->name() == "String");
  assert(string_column->data_type() == grnxx::STRING);

  for (grnxx::Int64 row_id = grnxx::MIN_ROW_ID; row_id <= 1000; ++row_id) {
    assert(table->insert_row() == row_id);
    boolean_column->set(row_id, row_id & 1);
    integer_column->set(row_id, row_id);
    float_column->set(row_id, 1.0 / row_id);
    std::string str = std::to_string(row_id);
    string_column->set(row_id, str);
  }

  for (grnxx::Int64 row_id = table->min_row_id();
       row_id <= table->max_row_id(); ++row_id) {
    assert(boolean_column->get(row_id) == grnxx::Boolean(row_id & 1));
    assert(integer_column->get(row_id) == grnxx::Int64(row_id));
    assert(float_column->get(row_id) == grnxx::Float(1.0 / row_id));
    std::string str = std::to_string(row_id);
    assert(string_column->get(row_id) == str);
  }

  std::vector<grnxx::RowID> row_ids = { 1, 5, 10, 50, 100, 500 };
  table->write_to(std::cout, &*row_ids.begin(), row_ids.size(),
                  "_id,Integer,Float,String") << std::endl;
  table->write_to(std::cout, &*row_ids.begin(), row_ids.size(),
                  "*") << std::endl;

  std::vector<grnxx::Int64> boundaries = { 2, 4, 6 };
  table->write_to(std::cout, &*row_ids.begin(), row_ids.size(), boundaries,
                  "*") << std::endl;
}

void test_calc() {
  grnxx::Database database;

  grnxx::Table *table = database.create_table("Table");
  assert(table);

  auto boolean_column = dynamic_cast<grnxx::ColumnImpl<grnxx::Boolean> *>(
      table->create_column("Boolean", grnxx::BOOLEAN));
  assert(boolean_column);
  auto integer_column = dynamic_cast<grnxx::ColumnImpl<grnxx::Int64> *>(
      table->create_column("Integer", grnxx::INTEGER));
  assert(integer_column);
  auto float_column = dynamic_cast<grnxx::ColumnImpl<grnxx::Float> *>(
      table->create_column("Float", grnxx::FLOAT));
  assert(float_column);
  auto string_column = dynamic_cast<grnxx::ColumnImpl<grnxx::String> *>(
      table->create_column("String", grnxx::STRING));
  assert(string_column);

  std::mt19937_64 random;
  std::vector<grnxx::Boolean> boolean_data;
  std::vector<grnxx::Int64> integer_data;
  std::vector<grnxx::Float> float_data;
  std::vector<std::string> string_data;
  for (grnxx::Int64 i = 0; i < 1000; ++i) {
    boolean_data.push_back(random() & 1);
    integer_data.push_back(random() % 100);
    float_data.push_back(1.0 * random() / random.max());
    std::string str(1 + (random() % 10), 'A');
    for (std::size_t i = 0; i < str.size(); ++i) {
      str[i] += random() % 26;
    }
    string_data.push_back(str);
  }

  for (grnxx::Int64 i = 0; i < 1000; ++i) {
    grnxx::RowID row_id = table->insert_row();
    boolean_column->set(row_id, boolean_data[i]);
    integer_column->set(row_id, integer_data[i]);
    float_column->set(row_id, float_data[i]);
    string_column->set(row_id, string_data[i]);
  }

  std::vector<grnxx::RowID> all_row_ids;
  std::unique_ptr<grnxx::RowIDCursor> cursor(table->create_cursor());
  assert(cursor->get_next(INT64_MAX, &all_row_ids) == 1000);

  // 何もしない．
  {
    std::unique_ptr<grnxx::Calc> calc(table->create_calc(""));
    assert(calc);
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    grnxx::Int64 num_row_ids = calc->filter(&*row_ids.begin(), row_ids.size());
    assert(num_row_ids == 1000);
    for (grnxx::Int64 i = 0; i < 1000; ++i) {
      grnxx::RowID row_id = grnxx::MIN_ROW_ID + i;
      assert(row_ids[i] == row_id);
    }
  }

  // Boolean で絞り込む．
  {
    std::unique_ptr<grnxx::Calc> calc(table->create_calc("Boolean"));
    assert(calc);
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    grnxx::Int64 num_row_ids = calc->filter(&*row_ids.begin(), row_ids.size());
    grnxx::Int64 count = 0;
    for (grnxx::Int64 i = 0; i < 1000; ++i) {
      grnxx::RowID row_id = grnxx::MIN_ROW_ID + i;
      if (boolean_data[i]) {
        assert(row_ids[count] == row_id);
        assert(++count <= num_row_ids);
      }
    }
    assert(count == num_row_ids);
  }

  // Integer の範囲で絞り込む．
  {
    std::unique_ptr<grnxx::Calc> calc(table->create_calc("Integer < 50"));
    assert(calc);
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    grnxx::Int64 num_row_ids = calc->filter(&*row_ids.begin(), row_ids.size());
    grnxx::Int64 count = 0;
    for (grnxx::Int64 i = 0; i < 1000; ++i) {
      grnxx::RowID row_id = grnxx::MIN_ROW_ID + i;
      if (integer_data[i] < 50) {
        assert(row_ids[count] == row_id);
        assert(++count <= num_row_ids);
      }
    }
    assert(count == num_row_ids);
  }

  // Boolean と Integer と Float の範囲で絞り込む．
  {
    std::unique_ptr<grnxx::Calc> calc(
        table->create_calc("Boolean && Integer < 50 && Float < 0.5"));
    assert(calc);
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    grnxx::Int64 num_row_ids = calc->filter(&*row_ids.begin(), row_ids.size());
    grnxx::Int64 count = 0;
    for (grnxx::Int64 i = 0; i < 1000; ++i) {
      grnxx::RowID row_id = grnxx::MIN_ROW_ID + i;
      if (boolean_data[i] && (integer_data[i] < 50) && (float_data[i] < 0.5)) {
        assert(row_ids[count] == row_id);
        assert(++count <= num_row_ids);
      }
    }
    assert(count == num_row_ids);
  }

  // Boolean と Integer と String の範囲で絞り込む．
  {
    std::unique_ptr<grnxx::Calc> calc(
        table->create_calc("(Boolean && Integer >= 50) || (String <= \"A\")"));
    assert(calc);
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    grnxx::Int64 num_row_ids = calc->filter(&*row_ids.begin(), row_ids.size());
    grnxx::Int64 count = 0;
    for (grnxx::Int64 i = 0; i < 1000; ++i) {
      grnxx::RowID row_id = grnxx::MIN_ROW_ID + i;
      if ((boolean_data[i] && (integer_data[i] >= 50)) ||
          (string_data[i] <= "A")) {
        assert(row_ids[count] == row_id);
        assert(++count <= num_row_ids);
      }
    }
    assert(count == num_row_ids);
  }

  // Integer の演算結果で絞り込む．
  {
    std::unique_ptr<grnxx::Calc> calc(
        table->create_calc("(Integer * 2) > 100"));
    assert(calc);
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    grnxx::Int64 num_row_ids = calc->filter(&*row_ids.begin(), row_ids.size());
    grnxx::Int64 count = 0;
    for (grnxx::Int64 i = 0; i < 1000; ++i) {
      grnxx::RowID row_id = grnxx::MIN_ROW_ID + i;
      if ((integer_data[i] * 2) > 100) {
        assert(row_ids[count] == row_id);
        assert(++count <= num_row_ids);
      }
    }
    assert(count == num_row_ids);
  }

  // Float の演算結果で絞り込む．
  {
    std::unique_ptr<grnxx::Calc> calc(table->create_calc("(Float + 1.0) < 1.5"));
    assert(calc);
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    grnxx::Int64 num_row_ids = calc->filter(&*row_ids.begin(), row_ids.size());
    grnxx::Int64 count = 0;
    for (grnxx::Int64 i = 0; i < 1000; ++i) {
      grnxx::RowID row_id = grnxx::MIN_ROW_ID + i;
      if ((float_data[i] + 1.0) < 1.5) {
        assert(row_ids[count] == row_id);
        assert(++count <= num_row_ids);
      }
    }
    assert(count == num_row_ids);
  }

  // ゼロによる除算を起こす．
  {
    std::unique_ptr<grnxx::Calc> calc(
        table->create_calc("Integer / Integer != 0"));
    assert(calc);
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    grnxx::Int64 num_row_ids = calc->filter(&*row_ids.begin(), row_ids.size());
    assert(num_row_ids == 0);
  }

  // オーバーフローを起こす．
  {
    std::unique_ptr<grnxx::Calc> calc(
        table->create_calc("9223372036854775807 + 9223372036854775807 != 0"));
    assert(!calc);
  }

  // || の左がすべて真になる．
  {
    std::unique_ptr<grnxx::Calc> calc(
        table->create_calc("Integer <= 100 || Float < 0.5"));
    assert(calc);
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    grnxx::Int64 num_row_ids = calc->filter(&*row_ids.begin(), row_ids.size());
    assert(num_row_ids != 0);
    grnxx::Int64 count = 0;
    for (grnxx::Int64 i = 0; i < 1000; ++i) {
      grnxx::RowID row_id = grnxx::MIN_ROW_ID + i;
      if ((integer_data[i] <= 100) || (float_data[i] < 0.5)) {
        assert(row_ids[count] == row_id);
        assert(++count <= num_row_ids);
      }
    }
    assert(count == num_row_ids);
  }

  // || の左がすべて偽になる．
  {
    std::unique_ptr<grnxx::Calc> calc(
        table->create_calc("Integer < 0 || Float < 0.5"));
    assert(calc);
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    grnxx::Int64 num_row_ids = calc->filter(&*row_ids.begin(), row_ids.size());
    assert(num_row_ids != 0);
    grnxx::Int64 count = 0;
    for (grnxx::Int64 i = 0; i < 1000; ++i) {
      grnxx::RowID row_id = grnxx::MIN_ROW_ID + i;
      if ((integer_data[i] < 0) || (float_data[i] < 0.5)) {
        assert(row_ids[count] == row_id);
        assert(++count <= num_row_ids);
      }
    }
    assert(count == num_row_ids);
  }

  // || の右がすべて真になる．
  {
    std::unique_ptr<grnxx::Calc> calc(
        table->create_calc("Integer < 50 || Float >= 0.0"));
    assert(calc);
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    grnxx::Int64 num_row_ids = calc->filter(&*row_ids.begin(), row_ids.size());
    assert(num_row_ids != 0);
    grnxx::Int64 count = 0;
    for (grnxx::Int64 i = 0; i < 1000; ++i) {
      grnxx::RowID row_id = grnxx::MIN_ROW_ID + i;
      if ((integer_data[i] < 50) || (float_data[i] >= 0.0)) {
        assert(row_ids[count] == row_id);
        assert(++count <= num_row_ids);
      }
    }
    assert(count == num_row_ids);
  }

  // || の右がすべて偽になる．
  {
    std::unique_ptr<grnxx::Calc> calc(
        table->create_calc("Integer < 50 || Float < 0.0"));
    assert(calc);
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    grnxx::Int64 num_row_ids = calc->filter(&*row_ids.begin(), row_ids.size());
    assert(num_row_ids != 0);
    grnxx::Int64 count = 0;
    for (grnxx::Int64 i = 0; i < 1000; ++i) {
      grnxx::RowID row_id = grnxx::MIN_ROW_ID + i;
      if ((integer_data[i] < 50) || (float_data[i] < 0.0)) {
        assert(row_ids[count] == row_id);
        assert(++count <= num_row_ids);
      }
    }
    assert(count == num_row_ids);
  }
}

void test_sorter() {
  grnxx::Database database;

  grnxx::Table *table = database.create_table("Table");
  assert(table);

  auto boolean_column = dynamic_cast<grnxx::ColumnImpl<grnxx::Boolean> *>(
      table->create_column("Boolean", grnxx::BOOLEAN));
  assert(boolean_column);
  auto integer_column = dynamic_cast<grnxx::ColumnImpl<grnxx::Int64> *>(
      table->create_column("Integer", grnxx::INTEGER));
  assert(integer_column);
  auto float_column = dynamic_cast<grnxx::ColumnImpl<grnxx::Float> *>(
      table->create_column("Float", grnxx::FLOAT));
  assert(float_column);
  auto string_column = dynamic_cast<grnxx::ColumnImpl<grnxx::String> *>(
      table->create_column("String", grnxx::STRING));
  assert(string_column);

  std::mt19937_64 random;
  for (grnxx::Int64 i = 0; i < 1000; ++i) {
    grnxx::RowID row_id = table->insert_row();
    boolean_column->set(row_id, random() & 1);
    integer_column->set(row_id, random() % 100);
    float_column->set(row_id, 1.0 * random() / random.max());
    std::string str(1 + (random() % 10), 'A');
    for (std::size_t i = 0; i < str.size(); ++i) {
      str[i] += random() % 26;
    }
    string_column->set(row_id, str);
  }

  std::vector<grnxx::RowID> all_row_ids;
  std::unique_ptr<grnxx::RowIDCursor> cursor(table->create_cursor());
  assert(cursor->get_next(INT64_MAX, &all_row_ids) == 1000);

  // Boolean を基準に整列する．
  {
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    std::unique_ptr<grnxx::Sorter> sorter(table->create_sorter("Boolean"));
    assert(sorter);
    sorter->sort(&*row_ids.begin(), row_ids.size());
    for (grnxx::Int64 i = 1; i < 1000; ++i) {
      assert(boolean_column->get(row_ids[i - 1]) <=
             boolean_column->get(row_ids[i]));
    }
  }

  // Integer を基準に整列する．
  {
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    std::unique_ptr<grnxx::Sorter> sorter(table->create_sorter("Integer"));
    assert(sorter);
    sorter->sort(&*row_ids.begin(), row_ids.size());
    for (grnxx::Int64 i = 1; i < 1000; ++i) {
      assert(integer_column->get(row_ids[i - 1]) <=
             integer_column->get(row_ids[i]));
    }
  }

  // Float を基準に整列する．
  {
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    std::unique_ptr<grnxx::Sorter> sorter(table->create_sorter("Float"));
    assert(sorter);
    sorter->sort(&*row_ids.begin(), row_ids.size());
    for (grnxx::Int64 i = 1; i < 1000; ++i) {
      assert(float_column->get(row_ids[i - 1]) <=
             float_column->get(row_ids[i]));
    }
  }

  // String を基準に整列する．
  {
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    std::unique_ptr<grnxx::Sorter> sorter(table->create_sorter("String"));
    assert(sorter);
    sorter->sort(&*row_ids.begin(), row_ids.size());
    for (grnxx::Int64 i = 1; i < 1000; ++i) {
      assert(string_column->get(row_ids[i - 1]) <=
             string_column->get(row_ids[i]));
    }
  }

  // Boolean を基準に整列する．
  {
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    std::unique_ptr<grnxx::Sorter> sorter(table->create_sorter("-Boolean"));
    assert(sorter);
    sorter->sort(&*row_ids.begin(), row_ids.size());
    for (grnxx::Int64 i = 1; i < 1000; ++i) {
      assert(boolean_column->get(row_ids[i - 1]) >=
             boolean_column->get(row_ids[i]));
    }
  }

  // Integer を基準に整列する．
  {
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    std::unique_ptr<grnxx::Sorter> sorter(table->create_sorter("-Integer"));
    assert(sorter);
    sorter->sort(&*row_ids.begin(), row_ids.size());
    for (grnxx::Int64 i = 1; i < 1000; ++i) {
      assert(integer_column->get(row_ids[i - 1]) >=
             integer_column->get(row_ids[i]));
    }
  }

  // Float を基準に整列する．
  {
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    std::unique_ptr<grnxx::Sorter> sorter(table->create_sorter("-Float"));
    assert(sorter);
    sorter->sort(&*row_ids.begin(), row_ids.size());
    for (grnxx::Int64 i = 1; i < 1000; ++i) {
      assert(float_column->get(row_ids[i - 1]) >=
             float_column->get(row_ids[i]));
    }
  }

  // String を基準に整列する．
  {
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    std::unique_ptr<grnxx::Sorter> sorter(table->create_sorter("-String"));
    assert(sorter);
    sorter->sort(&*row_ids.begin(), row_ids.size());
    for (grnxx::Int64 i = 1; i < 1000; ++i) {
      assert(string_column->get(row_ids[i - 1]) >=
             string_column->get(row_ids[i]));
    }
  }

  // Integer を基準に整列して 100 件目まで取得する．
  {
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    std::unique_ptr<grnxx::Sorter> sorter(table->create_sorter("Integer"));
    assert(sorter);
    sorter->sort(&*row_ids.begin(), row_ids.size(), 0, 100);
    for (grnxx::Int64 i = 1; i < 100; ++i) {
      assert(integer_column->get(row_ids[i - 1]) <=
             integer_column->get(row_ids[i]));
    }
  }

  // Integer を基準に整列して，先頭の 100 件をスキップしてから 200 件目まで取得する．
  {
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    std::unique_ptr<grnxx::Sorter> sorter(table->create_sorter("Integer"));
    assert(sorter);
    sorter->sort(&*row_ids.begin(), row_ids.size(), 100, 200);
    for (grnxx::Int64 i = 101; i < 300; ++i) {
      assert(integer_column->get(row_ids[i - 1]) <=
             integer_column->get(row_ids[i]));
    }
  }

  // Boolean, Integer, Float を基準に整列する．
  {
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    std::unique_ptr<grnxx::Sorter> sorter(
        table->create_sorter("Boolean,Integer,-Float"));
    assert(sorter);
    sorter->sort(&*row_ids.begin(), row_ids.size());
    for (grnxx::Int64 i = 1; i < 1000; ++i) {
      assert(boolean_column->get(row_ids[i - 1]) <=
             boolean_column->get(row_ids[i]));
      if (boolean_column->get(row_ids[i - 1]) ==
          boolean_column->get(row_ids[i])) {
        assert(integer_column->get(row_ids[i - 1]) <=
               integer_column->get(row_ids[i]));
        if (integer_column->get(row_ids[i - 1]) ==
            integer_column->get(row_ids[i])) {
          assert(float_column->get(row_ids[i - 1]) >=
                 float_column->get(row_ids[i]));
        }
      }
    }
  }

  // Boolean, Integer, String を基準に整列して 500 件目までを取得する．
  {
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    std::unique_ptr<grnxx::Sorter> sorter(
        table->create_sorter("Boolean,Integer,-String"));
    assert(sorter);
    sorter->sort(&*row_ids.begin(), row_ids.size(), 0, 500);
    for (grnxx::Int64 i = 1; i < 500; ++i) {
      assert(boolean_column->get(row_ids[i - 1]) <=
             boolean_column->get(row_ids[i]));
      if (boolean_column->get(row_ids[i - 1]) ==
          boolean_column->get(row_ids[i])) {
        assert(integer_column->get(row_ids[i - 1]) <=
               integer_column->get(row_ids[i]));
        if (integer_column->get(row_ids[i - 1]) ==
            integer_column->get(row_ids[i])) {
          assert(string_column->get(row_ids[i - 1]) >=
                 string_column->get(row_ids[i]));
        }
      }
    }
  }

  // Boolean, Integer, _id を基準に整列する．
  {
    std::vector<grnxx::RowID> row_ids(all_row_ids);
    std::unique_ptr<grnxx::Sorter> sorter(
        table->create_sorter("Boolean,-Integer,-_id"));
    assert(sorter);
    sorter->sort(&*row_ids.begin(), row_ids.size());
    for (grnxx::Int64 i = 1; i < 1000; ++i) {
      assert(boolean_column->get(row_ids[i - 1]) <=
             boolean_column->get(row_ids[i]));
      if (boolean_column->get(row_ids[i - 1]) ==
          boolean_column->get(row_ids[i])) {
        assert(integer_column->get(row_ids[i - 1]) >=
               integer_column->get(row_ids[i]));
        if (integer_column->get(row_ids[i - 1]) ==
            integer_column->get(row_ids[i])) {
          assert(row_ids[i - 1] > row_ids[i]);
        }
      }
    }
  }
}

void test_index() {
  grnxx::Database database;

  grnxx::Table *table = database.create_table("Table");
  assert(table);

  auto boolean_column = dynamic_cast<grnxx::ColumnImpl<grnxx::Boolean> *>(
      table->create_column("Boolean", grnxx::BOOLEAN));
  assert(boolean_column);
  auto integer_column = dynamic_cast<grnxx::ColumnImpl<grnxx::Int64> *>(
      table->create_column("Integer", grnxx::INTEGER));
  assert(integer_column);
  auto float_column = dynamic_cast<grnxx::ColumnImpl<grnxx::Float> *>(
      table->create_column("Float", grnxx::FLOAT));
  assert(float_column);
  auto string_column = dynamic_cast<grnxx::ColumnImpl<grnxx::String> *>(
      table->create_column("String", grnxx::STRING));
  assert(string_column);

  auto integer_index =
      table->create_index("Integer", "Integer", grnxx::TREE_MAP);
  assert(integer_index);
  auto float_index =
      table->create_index("Float", "Float", grnxx::TREE_MAP);
  assert(float_index);
  auto string_index =
      table->create_index("String", "String", grnxx::TREE_MAP);
  assert(string_index);

  std::mt19937_64 random;
  for (grnxx::Int64 i = 0; i < 1000; ++i) {
    grnxx::RowID row_id = table->insert_row();
    boolean_column->set(row_id, random() & 1);
    integer_column->set(row_id, random() % 100);
    float_column->set(row_id, 1.0 * random() / random.max());
    std::string str(1 + (random() % 10), 'A');
    for (std::size_t i = 0; i < str.size(); ++i) {
      str[i] += random() % 26;
    }
    string_column->set(row_id, str);
  }

  // Integer 昇順に全件．
  {
    std::unique_ptr<grnxx::RowIDCursor> cursor(integer_index->find_all());
    assert(cursor);
    std::vector<grnxx::RowID> row_ids;
    assert(cursor->get_next(INT64_MAX, &row_ids) == 1000);
    auto value = integer_column->get(row_ids[0]);
    assert(value >= 0);
    assert(value < 100);
    for (std::size_t i = 1; i < row_ids.size(); ++i) {
      auto prev_value = value;
      value = integer_column->get(row_ids[i]);
      assert(value >= 0);
      assert(value < 100);
      assert(value >= prev_value);
      prev_value = value;
    }
  }

  // Integer 昇順に 30 より大きく 70 より小さい範囲．
  {
    std::unique_ptr<grnxx::RowIDCursor> cursor(
        integer_index->find_between(grnxx::Int64(30), grnxx::Int64(70),
                                    false, false));
    assert(cursor);
    std::vector<grnxx::RowID> row_ids;
    assert(cursor->get_next(INT64_MAX, &row_ids) > 100);
    auto value = integer_column->get(row_ids[0]);
    assert(value > 30);
    assert(value < 70);
    for (std::size_t i = 1; i < row_ids.size(); ++i) {
      auto prev_value = value;
      value = integer_column->get(row_ids[i]);
      assert(value > 30);
      assert(value < 70);
      assert(value >= prev_value);
      prev_value = value;
    }
  }

  // Integer 昇順に 30 以上 70 以下の範囲．
  {
    std::unique_ptr<grnxx::RowIDCursor> cursor(
        integer_index->find_between(grnxx::Int64(30), grnxx::Int64(70),
                                    true, true));
    assert(cursor);
    std::vector<grnxx::RowID> row_ids;
    assert(cursor->get_next(INT64_MAX, &row_ids) > 100);
    auto value = integer_column->get(row_ids[0]);
    assert(value >= 30);
    assert(value <= 70);
    for (std::size_t i = 1; i < row_ids.size(); ++i) {
      auto prev_value = value;
      value = integer_column->get(row_ids[i]);
      assert(value >= 30);
      assert(value <= 70);
      assert(value >= prev_value);
      prev_value = value;
    }
  }

  // Float 昇順に 0.3 より大きく 0.7 より小さい範囲．
  {
    std::unique_ptr<grnxx::RowIDCursor> cursor(
        float_index->find_between(0.3, 0.7));
    assert(cursor);
    std::vector<grnxx::RowID> row_ids;
    assert(cursor->get_next(INT64_MAX, &row_ids) > 100);
    auto value = float_column->get(row_ids[0]);
    assert(value > 0.3);
    assert(value < 0.7);
    for (std::size_t i = 1; i < row_ids.size(); ++i) {
      auto prev_value = value;
      value = float_column->get(row_ids[i]);
      assert(value > 0.3);
      assert(value < 0.7);
      assert(value >= prev_value);
      prev_value = value;
    }
  }

  // String 昇順に "G" より大きく "P" より小さい範囲．
  {
    std::unique_ptr<grnxx::RowIDCursor> cursor(
        string_index->find_between("G", "P"));
    assert(cursor);
    std::vector<grnxx::RowID> row_ids;
    assert(cursor->get_next(INT64_MAX, &row_ids) > 100);
    auto value = string_column->get(row_ids[0]);
    assert(value > "G");
    assert(value < "P");
    for (std::size_t i = 1; i < row_ids.size(); ++i) {
      auto prev_value = value;
      value = string_column->get(row_ids[i]);
      assert(value > "G");
      assert(value < "P");
      assert(value >= prev_value);
      prev_value = value;
    }
  }

  // Integer 降順に全件．
  {
    std::unique_ptr<grnxx::RowIDCursor> cursor(integer_index->find_all(true));
    assert(cursor);
    std::vector<grnxx::RowID> row_ids;
    assert(cursor->get_next(INT64_MAX, &row_ids) == 1000);
    auto value = integer_column->get(row_ids[0]);
    assert(value >= 0);
    assert(value < 100);
    for (std::size_t i = 1; i < row_ids.size(); ++i) {
      auto prev_value = value;
      value = integer_column->get(row_ids[i]);
      assert(value >= 0);
      assert(value < 100);
      assert(value <= prev_value);
      prev_value = value;
    }
  }

  // Integer 降順に 30 より大きく 70 より小さい範囲．
  {
    std::unique_ptr<grnxx::RowIDCursor> cursor(
        integer_index->find_between(grnxx::Int64(30), grnxx::Int64(70),
                                    false, false, true));
    assert(cursor);
    std::vector<grnxx::RowID> row_ids;
    assert(cursor->get_next(INT64_MAX, &row_ids) > 100);
    auto value = integer_column->get(row_ids[0]);
    assert(value > 30);
    assert(value < 70);
    for (std::size_t i = 1; i < row_ids.size(); ++i) {
      auto prev_value = value;
      value = integer_column->get(row_ids[i]);
      assert(value > 30);
      assert(value < 70);
      assert(value <= prev_value);
      prev_value = value;
    }
  }

  // Integer 降順に 30 以上 70 以下の範囲．
  {
    std::unique_ptr<grnxx::RowIDCursor> cursor(
        integer_index->find_between(grnxx::Int64(30), grnxx::Int64(70),
                                    true, true, true));
    assert(cursor);
    std::vector<grnxx::RowID> row_ids;
    assert(cursor->get_next(INT64_MAX, &row_ids) > 100);
    auto value = integer_column->get(row_ids[0]);
    assert(value >= 30);
    assert(value <= 70);
    for (std::size_t i = 1; i < row_ids.size(); ++i) {
      auto prev_value = value;
      value = integer_column->get(row_ids[i]);
      assert(value >= 30);
      assert(value <= 70);
      assert(value <= prev_value);
      prev_value = value;
    }
  }

  // Float 降順に 0.3 より大きく 0.7 より小さい範囲．
  {
    std::unique_ptr<grnxx::RowIDCursor> cursor(
        float_index->find_between(0.3, 0.7, false, false, true));
    assert(cursor);
    std::vector<grnxx::RowID> row_ids;
    assert(cursor->get_next(INT64_MAX, &row_ids) > 100);
    auto value = float_column->get(row_ids[0]);
    assert(value > 0.3);
    assert(value < 0.7);
    for (std::size_t i = 1; i < row_ids.size(); ++i) {
      auto prev_value = value;
      value = float_column->get(row_ids[i]);
      assert(value > 0.3);
      assert(value < 0.7);
      assert(value <= prev_value);
      prev_value = value;
    }
  }

  // String 降順に "G" より大きく "P" より小さい範囲．
  {
    std::unique_ptr<grnxx::RowIDCursor> cursor(
        string_index->find_between("G", "P", false, false, true));
    assert(cursor);
    std::vector<grnxx::RowID> row_ids;
    assert(cursor->get_next(INT64_MAX, &row_ids) > 100);
    auto value = string_column->get(row_ids[0]);
    assert(value > "G");
    assert(value < "P");
    for (std::size_t i = 1; i < row_ids.size(); ++i) {
      auto prev_value = value;
      value = string_column->get(row_ids[i]);
      assert(value > "G");
      assert(value < "P");
      assert(value <= prev_value);
      prev_value = value;
    }
  }

  // Integer 昇順で論理和が使えることを確認する．
  {
    std::unique_ptr<grnxx::RowIDCursor> cursor(integer_index->find_all());
    assert(cursor);
    std::vector<grnxx::RowID> ordered_row_ids;
    assert(cursor->get_next(INT64_MAX, &ordered_row_ids) == 1000);
    std::unique_ptr<grnxx::Calc> calc(
        table->create_calc("(Boolean && Integer >= 50) || (String <= \"O\")"));
    assert(calc);
    std::vector<grnxx::RowID> row_ids(ordered_row_ids);
    grnxx::Int64 num_row_ids = calc->filter(&*row_ids.begin(), row_ids.size());
    grnxx::Int64 count = 0;
    for (grnxx::Int64 i = 0; i < 1000; ++i) {
      grnxx::RowID row_id = ordered_row_ids[i];
      if ((boolean_column->get(row_id) &&
           (integer_column->get(row_id) >= 50)) ||
          (string_column->get(row_id) <= "O")) {
        assert(row_ids[count] == row_id);
        assert(++count <= num_row_ids);
      }
    }
    assert(count == num_row_ids);
  }
}

}  // namespace

int main() {
  test_database();
  test_table();
  test_column();
  test_calc();
  test_sorter();
  test_index();
  return 0;
}
