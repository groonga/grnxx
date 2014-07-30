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
#include "grnxx/datum.hpp"
#include "grnxx/db.hpp"
#include "grnxx/error.hpp"
#include "grnxx/expression.hpp"
#include "grnxx/order.hpp"
#include "grnxx/record.hpp"
#include "grnxx/sorter.hpp"
#include "grnxx/table.hpp"

namespace {

void test_db() {
  grnxx::Error error;

  // デフォルトの設定で空のデータベースを作成する．
  auto db = grnxx::open_db(&error, "");
  assert(db);
  assert(db->num_tables() == 0);

  // "Table_1" という名前のテーブルを作成する．
  auto table = db->create_table(&error, "Table_1");
  assert(table);
  assert(table->name() == "Table_1");
  assert(db->num_tables() == 1);

  assert(db->get_table(0) == table);
  assert(db->find_table(&error, "Table_1") == table);

  // 同じ名前でテーブルを作成しようとすると失敗する．
  assert(!db->create_table(&error, "Table_1"));

  // "Table_2", "Table_3" という名前のテーブルを作成する．
  assert(db->create_table(&error, "Table_2"));
  assert(db->create_table(&error, "Table_3"));
  assert(db->num_tables() == 3);

  // "Table_2" という名前のテーブルを破棄する．
  assert(db->remove_table(&error, "Table_2"));
  assert(db->num_tables() == 2);

  assert(db->get_table(0)->name() == "Table_1");
  assert(db->get_table(1)->name() == "Table_3");

  // "Table_2" という名前のテーブルを作成しなおす．
  assert(db->create_table(&error, "Table_2"));

  // "Table_3" を "Table_2" の後ろに移動する．
  assert(db->reorder_table(&error, "Table_3", "Table_2"));
  assert(db->get_table(0)->name() == "Table_1");
  assert(db->get_table(1)->name() == "Table_2");
  assert(db->get_table(2)->name() == "Table_3");

  // "Table_3" を先頭に移動する．
  assert(db->reorder_table(&error, "Table_3", ""));
  assert(db->get_table(0)->name() == "Table_3");
  assert(db->get_table(1)->name() == "Table_1");
  assert(db->get_table(2)->name() == "Table_2");

  // "Table_2" を "Table_3" の後ろに移動する．
  assert(db->reorder_table(&error, "Table_2", "Table_3"));
  assert(db->get_table(0)->name() == "Table_3");
  assert(db->get_table(1)->name() == "Table_2");
  assert(db->get_table(2)->name() == "Table_1");
}

void test_table() {
  grnxx::Error error;

  auto db = grnxx::open_db(&error, "");
  assert(db);

  auto table = db->create_table(&error, "Table");
  assert(table);
  assert(table->db() == db.get());
  assert(table->name() == "Table");
  assert(table->num_columns() == 0);
  assert(!table->key_column());
  assert(table->num_rows() == 0);
  assert(table->max_row_id() == 0);

  // Bool を格納する "Column_1" という名前のカラムを作成する．
  auto column = table->create_column(&error, "Column_1", grnxx::BOOL_DATA);
  assert(column);
  assert(column->name() == "Column_1");
  assert(table->num_columns() == 1);

  assert(table->get_column(0) == column);
  assert(table->find_column(&error, "Column_1") == column);

  // 同じ名前でカラムを作成しようとすると失敗する．
  assert(!table->create_column(&error, "Column_1", grnxx::BOOL_DATA));

  // "Column_2", "Column_3" という名前のカラムを作成する．
  assert(table->create_column(&error, "Column_2", grnxx::BOOL_DATA));
  assert(table->create_column(&error, "Column_3", grnxx::BOOL_DATA));
  assert(table->num_columns() == 3);

  // "Column_2" という名前のカラムを破棄する．
  assert(table->remove_column(&error, "Column_2"));
  assert(table->num_columns() == 2);

  assert(table->get_column(0)->name() == "Column_1");
  assert(table->get_column(1)->name() == "Column_3");

  // "Column_2" という名前のカラムを作成しなおす．
  assert(table->create_column(&error, "Column_2", grnxx::BOOL_DATA));

  // "Column_3" を "Column_2" の後ろに移動する．
  assert(table->reorder_column(&error, "Column_3", "Column_2"));
  assert(table->get_column(0)->name() == "Column_1");
  assert(table->get_column(1)->name() == "Column_2");
  assert(table->get_column(2)->name() == "Column_3");

  // "Column_3" を先頭に移動する．
  assert(table->reorder_column(&error, "Column_3", ""));
  assert(table->get_column(0)->name() == "Column_3");
  assert(table->get_column(1)->name() == "Column_1");
  assert(table->get_column(2)->name() == "Column_2");

  // "Column_2" を "Column_3" の後ろに移動する．
  assert(table->reorder_column(&error, "Column_2", "Column_3"));
  assert(table->get_column(0)->name() == "Column_3");
  assert(table->get_column(1)->name() == "Column_2");
  assert(table->get_column(2)->name() == "Column_1");

  // TODO: set_key_column(), unset_key_column().

  // 最初の行を追加する．
  grnxx::Int row_id;
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));
  assert(row_id == 1);
  assert(table->num_rows() == 1);
  assert(table->max_row_id() == 1);
  assert(!table->test_row(&error, 0));
  assert(table->test_row(&error, 1));
  assert(!table->test_row(&error, 2));

  // さらに 2 行追加する．
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

  // 2 番目の行を削除する．
  assert(table->remove_row(&error, 2));
  assert(table->num_rows() == 2);
  assert(table->max_row_id() == 3);
  assert(!table->test_row(&error, 0));
  assert(table->test_row(&error, 1));
  assert(!table->test_row(&error, 2));
  assert(table->test_row(&error, 3));
  assert(!table->test_row(&error, 4));

  // TODO: find_row().

  // デフォルト（行 ID 昇順）のカーソルを作成する．
  auto cursor = table->create_cursor(&error);
  assert(cursor);

  // カーソルからレコードを読み出す．
  grnxx::RecordSet record_set;
  assert(cursor->read(&error, 0, &record_set) == 0);

  assert(cursor->read(&error, 1, &record_set) == 1);
  assert(record_set.size() == 1);
  assert(record_set.get(0).row_id == 1);

  assert(cursor->read(&error, 2, &record_set) == 1);
  assert(record_set.size() == 2);
  assert(record_set.get(0).row_id == 1);
  assert(record_set.get(1).row_id == 3);

  record_set.clear();

  // 行 ID 降順のカーソルを作成する．
  grnxx::CursorOptions cursor_options;
  cursor_options.order_type = grnxx::REVERSE_ORDER;
  cursor = table->create_cursor(&error, cursor_options);
  assert(cursor);

  assert(cursor->read_all(&error, &record_set) == 2);
  assert(record_set.size() == 2);
  assert(record_set.get(0).row_id == 3);
  assert(record_set.get(1).row_id == 1);

  record_set.clear();

  cursor = table->create_cursor(&error, cursor_options);
  assert(cursor);

  assert(cursor->read(&error, 1, &record_set) == 1);
  assert(record_set.size() == 1);
  assert(record_set.get(0).row_id == 3);

  assert(cursor->read(&error, 2, &record_set) == 1);
  assert(record_set.size() == 2);
  assert(record_set.get(0).row_id == 3);
  assert(record_set.get(1).row_id == 1);
}

void test_bitmap() {
  constexpr int NUM_ROWS = 1 << 14;

  grnxx::Error error;

  auto db = grnxx::open_db(&error, "");
  assert(db);

  auto table = db->create_table(&error, "Table");
  assert(table);

  for (int i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    assert(row_id == (i + 1));
  }
  assert(table->num_rows() == NUM_ROWS);
  assert(table->max_row_id() == NUM_ROWS);

  for (int i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = i + 1;
    assert(table->remove_row(&error, row_id));
  }
  assert(table->num_rows() == 0);
  assert(table->max_row_id() == (grnxx::MIN_ROW_ID - 1));

  for (int i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    assert(row_id == (i + 1));
  }
  assert(table->num_rows() == NUM_ROWS);
  assert(table->max_row_id() == NUM_ROWS);

  for (int i = 0; i < NUM_ROWS; i += 2) {
    grnxx::Int row_id = i + 1;
    assert(table->remove_row(&error, row_id));
  }
  assert(table->num_rows() == (NUM_ROWS / 2));
  assert(table->max_row_id() == NUM_ROWS);

  for (int i = 0; i < NUM_ROWS; i += 2) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    assert(row_id == (i + 1));
  }
  assert(table->num_rows() == NUM_ROWS);
  assert(table->max_row_id() == NUM_ROWS);

  for (int i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = NUM_ROWS - i;
    assert(table->remove_row(&error, row_id));
    assert(table->max_row_id() == (row_id - 1));
  }
}

void test_column() {
  grnxx::Error error;

  auto db = grnxx::open_db(&error, "");
  assert(db);

  auto table = db->create_table(&error, "Table");
  assert(table);

  // 最初の行を追加する．
  grnxx::Int row_id;
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));

  // Bool を格納する "BoolColumn" という名前のカラムを作成する．
  auto bool_column = table->create_column(&error, "BoolColumn",
                                          grnxx::BOOL_DATA);
  assert(bool_column);
  assert(bool_column->table() == table);
  assert(bool_column->name() == "BoolColumn");
  assert(bool_column->data_type() == grnxx::BOOL_DATA);
  assert(!bool_column->has_key_attribute());
  assert(bool_column->num_indexes() == 0);

  // Int を格納する "IntColumn" という名前のカラムを作成する．
  auto int_column = table->create_column(&error, "IntColumn",
                                         grnxx::INT_DATA);
  assert(int_column);
  assert(int_column->table() == table);
  assert(int_column->name() == "IntColumn");
  assert(int_column->data_type() == grnxx::INT_DATA);
  assert(!int_column->has_key_attribute());
  assert(int_column->num_indexes() == 0);

  // Float を格納する "FloatColumn" という名前のカラムを作成する．
  auto float_column = table->create_column(&error, "FloatColumn",
                                           grnxx::FLOAT_DATA);
  assert(float_column);
  assert(float_column->table() == table);
  assert(float_column->name() == "FloatColumn");
  assert(float_column->data_type() == grnxx::FLOAT_DATA);
  assert(!float_column->has_key_attribute());
  assert(float_column->num_indexes() == 0);

  // Text を格納する "TextColumn" という名前のカラムを作成する．
  auto text_column = table->create_column(&error, "TextColumn",
                                           grnxx::TEXT_DATA);
  assert(text_column);
  assert(text_column->table() == table);
  assert(text_column->name() == "TextColumn");
  assert(text_column->data_type() == grnxx::TEXT_DATA);
  assert(!text_column->has_key_attribute());
  assert(text_column->num_indexes() == 0);

  grnxx::Datum datum;

  // 最初の行にデフォルト値が格納されていることを確認する．
  assert(bool_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::BOOL_DATA);
  assert(!datum.force_bool());

  assert(int_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.force_int() == 0);

  assert(float_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::FLOAT_DATA);
  assert(datum.force_float() == 0.0);

  assert(text_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::TEXT_DATA);
  assert(datum.force_text() == "");

  // 最初の行に正しく値を格納できるか確認する．
  assert(bool_column->set(&error, 1, grnxx::Bool(true)));
  assert(int_column->set(&error, 1, grnxx::Int(123)));
  assert(float_column->set(&error, 1, grnxx::Float(0.25)));
  assert(text_column->set(&error, 1, grnxx::Text("Hello, world!")));

  assert(bool_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::BOOL_DATA);
  assert(datum.force_bool());

  assert(int_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::INT_DATA);
  assert(datum.force_int() == 123);

  assert(float_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::FLOAT_DATA);
  assert(datum.force_float() == 0.25);

  assert(text_column->get(&error, 1, &datum));
  assert(datum.type() == grnxx::TEXT_DATA);
  assert(datum.force_text() == "Hello, world!");
}

void test_expression() {
  grnxx::Error error;

  auto db = grnxx::open_db(&error, "");
  assert(db);

  auto table = db->create_table(&error, "Table");
  assert(table);

  auto bool_column = table->create_column(&error, "BoolColumn",
                                          grnxx::BOOL_DATA);
  assert(bool_column);

  auto int_column = table->create_column(&error, "IntColumn",
                                         grnxx::INT_DATA);
  assert(int_column);

  auto float_column = table->create_column(&error, "FloatColumn",
                                           grnxx::FLOAT_DATA);
  assert(float_column);

  auto text_column = table->create_column(&error, "TextColumn",
                                           grnxx::TEXT_DATA);
  assert(text_column);

  // 下記のデータを格納する．
  //
  // RowID BoolColumn IntColumn FloatColumn TextColumn
  //     1      false       123       -0.25      "ABC"
  //     2       true       456        0.25      "XYZ"
  grnxx::Int row_id;
  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));
  assert(bool_column->set(&error, row_id, grnxx::Bool(false)));
  assert(int_column->set(&error, row_id, grnxx::Int(123)));
  assert(float_column->set(&error, row_id, grnxx::Float(-0.25)));
  assert(text_column->set(&error, row_id, grnxx::Text("ABC")));

  assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                           grnxx::Datum(), &row_id));
  assert(bool_column->set(&error, row_id, grnxx::Bool(true)));
  assert(int_column->set(&error, row_id, grnxx::Int(456)));
  assert(float_column->set(&error, row_id, grnxx::Float(0.25)));
  assert(text_column->set(&error, row_id, grnxx::Text("XYZ")));

  // 式構築用のオブジェクトを用意する．
  auto builder = grnxx::ExpressionBuilder::create(&error, table);
  assert(builder);

  // もっとも単純な恒真式を作成する．
  assert(builder->push_datum(&error, grnxx::Bool(true)));
  auto expression = builder->release(&error);
  assert(expression);

  // 恒真式のフィルタにかけても変化しないことを確認する．
  grnxx::RecordSet record_set;
  auto cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &record_set) == 2);

  assert(expression->filter(&error, &record_set));
  assert(record_set.size() == 2);

  // 演算子を含む恒真式を作成する．
  assert(builder->push_datum(&error, grnxx::Int(100)));
  assert(builder->push_datum(&error, grnxx::Int(100)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  assert(expression->filter(&error, &record_set));
  assert(record_set.size() == 2);

  // "BoolColumn" という名前のカラムに格納されている値を返すだけの式を作成する．
  assert(builder->push_column(&error, "BoolColumn"));
  expression = builder->release(&error);
  assert(expression);

  // フィルタとして使ったときの結果を確認する．
  assert(expression->filter(&error, &record_set));
  assert(record_set.size() == 1);
  assert(record_set.get(0).row_id == 2);

  record_set.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &record_set) == 2);

  // "IntColumn" という名前のカラムに格納されている値が 123 のときだけ
  // 真になる式を作成する．
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(123)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  // フィルタとして使ったときの結果を確認する．
  assert(expression->filter(&error, &record_set));
  assert(record_set.size() == 1);
  assert(record_set.get(0).row_id == 1);

  record_set.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &record_set) == 2);

  // "IntColumn" という名前のカラムに格納されている値が 123 でないときだけ
  // 真になる式を作成する．
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(123)));
  assert(builder->push_operator(&error, grnxx::NOT_EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  // フィルタとして使ったときの結果を確認する．
  assert(expression->filter(&error, &record_set));
  assert(record_set.size() == 1);
  assert(record_set.get(0).row_id == 2);

  record_set.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &record_set) == 2);

  // 大小関係による比較を試す．
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(300)));
  assert(builder->push_operator(&error, grnxx::LESS_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  // フィルタとして使ったときの結果を確認する．
  assert(expression->filter(&error, &record_set));
  assert(record_set.size() == 1);
  assert(record_set.get(0).row_id == 1);

  record_set.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &record_set) == 2);

  // 大小関係による比較を試す．
  assert(builder->push_column(&error, "TextColumn"));
  assert(builder->push_datum(&error, grnxx::Text("ABC")));
  assert(builder->push_operator(&error, grnxx::LESS_EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  // フィルタとして使ったときの結果を確認する．
  assert(expression->filter(&error, &record_set));
  assert(record_set.size() == 1);
  assert(record_set.get(0).row_id == 1);

  record_set.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &record_set) == 2);

  // 大小関係による比較を試す．
  assert(builder->push_column(&error, "TextColumn"));
  assert(builder->push_datum(&error, grnxx::Text("ABC")));
  assert(builder->push_operator(&error, grnxx::GREATER_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  // フィルタとして使ったときの結果を確認する．
  assert(expression->filter(&error, &record_set));
  assert(record_set.size() == 1);
  assert(record_set.get(0).row_id == 2);

  record_set.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &record_set) == 2);

  // 大小関係による比較を試す．
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(456)));
  assert(builder->push_operator(&error, grnxx::GREATER_EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  // フィルタとして使ったときの結果を確認する．
  assert(expression->filter(&error, &record_set));
  assert(record_set.size() == 1);
  assert(record_set.get(0).row_id == 2);

  record_set.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &record_set) == 2);

  // 論理演算を試す．
  assert(builder->push_column(&error, "FloatColumn"));
  assert(builder->push_datum(&error, grnxx::Float(0.0)));
  assert(builder->push_operator(&error, grnxx::GREATER_OPERATOR));
  assert(builder->push_column(&error, "BoolColumn"));
  assert(builder->push_operator(&error, grnxx::LOGICAL_AND_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  // フィルタとして使ったときの結果を確認する．
  assert(expression->filter(&error, &record_set));
  assert(record_set.size() == 1);
  assert(record_set.get(0).row_id == 2);

  record_set.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &record_set) == 2);

  // 論理演算を試す．
  assert(builder->push_datum(&error, grnxx::Bool(false)));
  assert(builder->push_column(&error, "BoolColumn"));
  assert(builder->push_operator(&error, grnxx::LOGICAL_OR_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  // フィルタとして使ったときの結果を確認する．
  assert(expression->filter(&error, &record_set));
  assert(record_set.size() == 1);
  assert(record_set.get(0).row_id == 2);

  record_set.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &record_set) == 2);

  // ビット論理積を試す．
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(255)));
  assert(builder->push_operator(&error, grnxx::BITWISE_AND_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Int(200)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  // フィルタとして使ったときの結果を確認する．
  assert(expression->filter(&error, &record_set));
  assert(record_set.size() == 1);
  assert(record_set.get(0).row_id == 2);

  record_set.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &record_set) == 2);

  // ビット論理和を試す．
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(256)));
  assert(builder->push_operator(&error, grnxx::BITWISE_OR_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Int(379)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  // フィルタとして使ったときの結果を確認する．
  assert(expression->filter(&error, &record_set));
  assert(record_set.size() == 1);
  assert(record_set.get(0).row_id == 1);

  record_set.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &record_set) == 2);

  // ビット排他的論理和を試す．
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(255)));
  assert(builder->push_operator(&error, grnxx::BITWISE_XOR_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Int(132)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  // フィルタとして使ったときの結果を確認する．
  assert(expression->filter(&error, &record_set));
  assert(record_set.size() == 1);
  assert(record_set.get(0).row_id == 1);

  record_set.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &record_set) == 2);

  // 加算を試す．
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(100)));
  assert(builder->push_operator(&error, grnxx::PLUS_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Int(223)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  // フィルタとして使ったときの結果を確認する．
  assert(expression->filter(&error, &record_set));
  assert(record_set.size() == 1);
  assert(record_set.get(0).row_id == 1);

  record_set.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &record_set) == 2);

  // 減算を試す．
  assert(builder->push_column(&error, "FloatColumn"));
  assert(builder->push_datum(&error, grnxx::Float(0.25)));
  assert(builder->push_operator(&error, grnxx::MINUS_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Float(0.0)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  // フィルタとして使ったときの結果を確認する．
  assert(expression->filter(&error, &record_set));
  assert(record_set.size() == 1);
  assert(record_set.get(0).row_id == 2);

  record_set.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &record_set) == 2);

  // 乗算を試す．
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(2)));
  assert(builder->push_operator(&error, grnxx::MULTIPLICATION_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Int(912)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  // フィルタとして使ったときの結果を確認する．
  assert(expression->filter(&error, &record_set));
  assert(record_set.size() == 1);
  assert(record_set.get(0).row_id == 2);

  record_set.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &record_set) == 2);

  // スコア計算を試す．
  assert(builder->push_column(&error, "_score"));
  assert(builder->push_datum(&error, grnxx::Float(1.0)));
  assert(builder->push_operator(&error, grnxx::PLUS_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  // スコア調整に使ったときの結果を確認する．
  assert(expression->adjust(&error, &record_set));
  assert(record_set.size() == 2);
  assert(record_set.get(0).row_id == 1);
  assert(record_set.get(0).score == 1.0);
  assert(record_set.get(1).row_id == 2);
  assert(record_set.get(1).score == 1.0);

  record_set.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &record_set) == 2);

  // 評価結果を取り出す．
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(100)));
  assert(builder->push_operator(&error, grnxx::PLUS_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  // 評価結果を確認する．
  grnxx::Array<grnxx::Int> result_set;
  assert(expression->evaluate(&error, record_set, &result_set));
  assert(result_set.size() == 2);
  assert(result_set[0] == 223);
  assert(result_set[1] == 556);
}

void test_sorter() {
  grnxx::Error error;

  auto db = grnxx::open_db(&error, "");
  assert(db);

  auto table = db->create_table(&error, "Table");
  assert(table);

  auto int_column = table->create_column(&error, "IntColumn",
                                         grnxx::INT_DATA);
  assert(int_column);

  std::vector<grnxx::Int> values(1024);
  std::mt19937_64 mersenne_twister;
  for (size_t i = 0; i < values.size(); ++i) {
    grnxx::Int row_id;
    assert(table->insert_row(&error, grnxx::NULL_ROW_ID,
                             grnxx::Datum(), &row_id));
    values[i] = mersenne_twister() % 64;
    assert(int_column->set(&error, row_id, values[i]));
  }

  grnxx::RecordSet record_set;
  auto cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &record_set) ==
         static_cast<grnxx::Int>(values.size()));
  assert(record_set.size() == static_cast<grnxx::Int>(values.size()));

  auto order_set_builder =
      grnxx::OrderSetBuilder::create(&error, table);
  assert(order_set_builder);

  auto expression_builder =
      grnxx::ExpressionBuilder::create(&error, table);
  assert(expression_builder->push_column(&error, "IntColumn"));
  auto expression = expression_builder->release(&error);
  assert(expression);
  assert(order_set_builder->append(&error, std::move(expression)));

  assert(expression_builder->push_column(&error, "_id"));
  expression = expression_builder->release(&error);
  assert(expression);
  assert(order_set_builder->append(&error, std::move(expression)));

  auto order_set = order_set_builder->release(&error);
  assert(order_set);

  auto sorter = grnxx::Sorter::create(&error, std::move(order_set));
  assert(sorter);

  assert(sorter->sort(&error, &record_set));
  assert(record_set.size() == static_cast<grnxx::Int>(values.size()));

  for (grnxx::Int i = 1; i < record_set.size(); ++i) {
    grnxx::Int lhs_id = record_set.get_row_id(i - 1) - 1;
    grnxx::Int rhs_id = record_set.get_row_id(i) - 1;
    grnxx::Int lhs_value = values[lhs_id];
    grnxx::Int rhs_value = values[rhs_id];
    assert(lhs_value <= rhs_value);
    if (lhs_value == rhs_value) {
      assert(lhs_id < rhs_id);
    }
  }
}

}  // namespace

int main() {
  test_db();
  test_table();
  test_bitmap();
  test_column();
  test_expression();
  test_sorter();

  return 0;
}
