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
#include "grnxx/datum.hpp"
#include "grnxx/db.hpp"
#include "grnxx/error.hpp"
#include "grnxx/expression.hpp"
#include "grnxx/table.hpp"

void test_expression() {
  grnxx::Error error;

  // Create a database with the default options.
  auto db = grnxx::open_db(&error, "");
  assert(db);

  // Create a table with the default options.
  auto table = db->create_table(&error, "Table");
  assert(table);

  // Create columns for Bool, Int, Float, and Text values.
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

  // Store the following data.
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

  // Create an object for building expressions.
  auto builder = grnxx::ExpressionBuilder::create(&error, table);
  assert(builder);

  // Test an expression (true).
  assert(builder->push_datum(&error, grnxx::Bool(true)));
  auto expression = builder->release(&error);
  assert(expression);

  grnxx::Array<grnxx::Record> records;
  auto cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 2);

  // Test an expression (100 == 100).
  assert(builder->push_datum(&error, grnxx::Int(100)));
  assert(builder->push_datum(&error, grnxx::Int(100)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 2);

  // Test an expression (BoolColumn).
  assert(builder->push_column(&error, "BoolColumn"));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 2);

  // Test an expression (IntColumn == 123).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(123)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 1);

  // Test an expression (IntColumn != 123).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(123)));
  assert(builder->push_operator(&error, grnxx::NOT_EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 2);

  // Test an expresison (IntColumn < 300).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(300)));
  assert(builder->push_operator(&error, grnxx::LESS_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 1);

  // Test an expresison (TextColumn <= "ABC").
  assert(builder->push_column(&error, "TextColumn"));
  assert(builder->push_datum(&error, grnxx::Text("ABC")));
  assert(builder->push_operator(&error, grnxx::LESS_EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 1);

  // Test an expresison (TextColumn > "ABC").
  assert(builder->push_column(&error, "TextColumn"));
  assert(builder->push_datum(&error, grnxx::Text("ABC")));
  assert(builder->push_operator(&error, grnxx::GREATER_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 2);

  // Test an expresison (IntColumn >= 456).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(456)));
  assert(builder->push_operator(&error, grnxx::GREATER_EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 2);

  // Test an expresison ((FloatColumn > 0.0) && BoolColumn).
  assert(builder->push_column(&error, "FloatColumn"));
  assert(builder->push_datum(&error, grnxx::Float(0.0)));
  assert(builder->push_operator(&error, grnxx::GREATER_OPERATOR));
  assert(builder->push_column(&error, "BoolColumn"));
  assert(builder->push_operator(&error, grnxx::LOGICAL_AND_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 2);

  // Test an expresison (false || BoolColumn).
  assert(builder->push_datum(&error, grnxx::Bool(false)));
  assert(builder->push_column(&error, "BoolColumn"));
  assert(builder->push_operator(&error, grnxx::LOGICAL_OR_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 2);

  // Test an expresison (+IntColumn == 123).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_operator(&error, grnxx::POSITIVE_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Int(123)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 1);

  // Test an expresison (-IntColumn == 456).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_operator(&error, grnxx::NEGATIVE_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Int(-456)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 2);

  // Test an expresison (Int(FloatColumn) == 0).
  assert(builder->push_column(&error, "FloatColumn"));
  assert(builder->push_operator(&error, grnxx::TO_INT_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Int(0)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 2);

  // Test an expresison (Float(IntColumn) < 300.0).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_operator(&error, grnxx::TO_FLOAT_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Float(300.0)));
  assert(builder->push_operator(&error, grnxx::LESS_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 1);

  // Test an expresison ((IntColumn & 255) == 200).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(255)));
  assert(builder->push_operator(&error, grnxx::BITWISE_AND_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Int(200)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 2);

  // Test an expresison ((IntColumn | 256) == 379).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(256)));
  assert(builder->push_operator(&error, grnxx::BITWISE_OR_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Int(379)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 1);

  // Test an expresison ((IntColumn ^ 255) == 132).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(255)));
  assert(builder->push_operator(&error, grnxx::BITWISE_XOR_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Int(132)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 1);

  // Test an expresison ((IntColumn + 100) == 223).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(100)));
  assert(builder->push_operator(&error, grnxx::PLUS_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Int(223)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 1);

  // Test an expresison ((FloatColumn - 0.25) == 0.0).
  assert(builder->push_column(&error, "FloatColumn"));
  assert(builder->push_datum(&error, grnxx::Float(0.25)));
  assert(builder->push_operator(&error, grnxx::MINUS_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Float(0.0)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 2);

  // Test an expresison ((IntColumn * 2) == 912).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(2)));
  assert(builder->push_operator(&error, grnxx::MULTIPLICATION_OPERATOR));
  assert(builder->push_datum(&error, grnxx::Int(912)));
  assert(builder->push_operator(&error, grnxx::EQUAL_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->filter(&error, &records));
  assert(records.size() == 1);
  assert(records.get(0).row_id == 2);

  // Test an expresison (_score + 1.0).
  assert(builder->push_column(&error, "_score"));
  assert(builder->push_datum(&error, grnxx::Float(1.0)));
  assert(builder->push_operator(&error, grnxx::PLUS_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  records.clear();
  cursor = table->create_cursor(&error);
  assert(cursor);
  assert(cursor->read_all(&error, &records) == 2);

  assert(expression->adjust(&error, &records));
  assert(records.size() == 2);
  assert(records.get(0).row_id == 1);
  assert(records.get(0).score == 1.0);
  assert(records.get(1).row_id == 2);
  assert(records.get(1).score == 1.0);

  // Test an expresison (IntColumn + 100).
  assert(builder->push_column(&error, "IntColumn"));
  assert(builder->push_datum(&error, grnxx::Int(100)));
  assert(builder->push_operator(&error, grnxx::PLUS_OPERATOR));
  expression = builder->release(&error);
  assert(expression);

  grnxx::Array<grnxx::Int> result_set;
  assert(expression->evaluate(&error, records, &result_set));
  assert(result_set.size() == 2);
  assert(result_set[0] == 223);
  assert(result_set[1] == 556);
}

int main() {
  test_expression();
  return 0;
}
