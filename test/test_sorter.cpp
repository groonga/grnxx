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
#include <algorithm>
#include <cassert>
#include <iostream>
#include <random>
#include <string>

#include "grnxx/column.hpp"
#include "grnxx/cursor.hpp"
#include "grnxx/db.hpp"
#include "grnxx/expression.hpp"
#include "grnxx/table.hpp"
#include "grnxx/sorter.hpp"

constexpr size_t NUM_ROWS = 1 << 16;

std::mt19937_64 rng;

void generate_value(grnxx::Bool *value) {
  if ((rng() % 256) == 0) {
    *value = grnxx::Bool::na();
  } else {
    *value = grnxx::Bool((rng() % 2) == 1);
  }
}

void generate_value(grnxx::Int *value) {
  if ((rng() % 256) == 0) {
    *value = grnxx::Int::na();
  } else {
    *value = grnxx::Int(static_cast<int64_t>(rng() % 256) - 128);
  }
}

void generate_value(grnxx::Float *value) {
  if ((rng() % 256) == 0) {
    *value = grnxx::Float::na();
  } else {
    *value = grnxx::Float(static_cast<int64_t>((rng() % 256) - 128) / 128.0);
  }
}

void generate_value(grnxx::Text *value) {
  static std::vector<std::string> bodies;
  if ((rng() % 256) == 0) {
    *value = grnxx::Text::na();
  } else {
    size_t size = rng() % 4;
    std::string body;
    body.resize(size);
    for (size_t i = 0; i < size; ++i) {
      body[i] = '0' + (rng() % 10);
    }
    bodies.push_back(std::move(body));
    *value = grnxx::Text(body.data(), body.size());
  }
}

grnxx::Array<grnxx::Record> create_records(grnxx::Table *table) {
  auto cursor = table->create_cursor();
  grnxx::Array<grnxx::Record> records;
  assert(cursor->read_all(&records) == NUM_ROWS);
  std::shuffle(records.buffer(), records.buffer() + records.size(),
               std::mt19937_64(rng()));
  return records;
}

grnxx::Array<grnxx::Record> create_records(
    grnxx::Table *table,
    const std::vector<grnxx::Float> &scores) {
  auto records = create_records(table);
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    records[i].score = scores[i];
  }
  return records;
}

struct RegularComparer {
  bool operator()(grnxx::Bool lhs, grnxx::Bool rhs) const {
    return lhs.is_na() ? false :
           (rhs.is_na() ? true : (lhs.raw() < rhs.raw()));
  }
  bool operator()(grnxx::Int lhs, grnxx::Int rhs) const {
    return lhs.is_na() ? false :
           (rhs.is_na() ? true : (lhs < rhs).is_true());
  }
  bool operator()(grnxx::Float lhs, grnxx::Float rhs) const {
    return lhs.is_na() ? false :
           (rhs.is_na() ? true : (lhs < rhs).is_true());
  }
  bool operator()(const grnxx::Text &lhs, const grnxx::Text &rhs) const {
    return lhs.is_na() ? false :
           (rhs.is_na() ? true : (lhs < rhs).is_true());
  }
};

struct ReverseComparer {
  bool operator()(grnxx::Bool lhs, grnxx::Bool rhs) const {
    return lhs.is_na() ? false :
           (rhs.is_na() ? true : (lhs.raw() > rhs.raw()));
  }
  bool operator()(grnxx::Int lhs, grnxx::Int rhs) const {
    return lhs.is_na() ? false :
           (rhs.is_na() ? true : (lhs > rhs).is_true());
  }
  bool operator()(grnxx::Float lhs, grnxx::Float rhs) const {
    return lhs.is_na() ? false :
           (rhs.is_na() ? true : (lhs > rhs).is_true());
  }
  bool operator()(const grnxx::Text &lhs, const grnxx::Text &rhs) const {
    return lhs.is_na() ? false :
           (rhs.is_na() ? true : (lhs > rhs).is_true());
  }
};

void test_row_id() {
  // Create a table.
  auto db = grnxx::open_db("");
  auto table = db->create_table("Table");
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    table->insert_row();
  }

  // Test a regular sorter.
  grnxx::Array<grnxx::SorterOrder> orders;
  orders.resize(1);
  auto expression_builder = grnxx::ExpressionBuilder::create(table);
  expression_builder->push_row_id();
  orders[0].expression = expression_builder->release();
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  auto sorter = grnxx::Sorter::create(std::move(orders));
  auto records = create_records(table);
  sorter->sort(&records);
  for (size_t i = 0; i < records.size(); ++i) {
    assert(records[i].row_id.raw() == static_cast<int64_t>(i));
  }

  // Test a reverse sorter.
  orders.resize(1);
  expression_builder->push_row_id();
  orders[0].expression = expression_builder->release();
  orders[0].type = grnxx::SORTER_REVERSE_ORDER;
  sorter = grnxx::Sorter::create(std::move(orders));
  records = create_records(table);
  sorter->sort(&records);
  for (size_t i = 0; i < records.size(); ++i) {
    assert(records[i].row_id.raw() == static_cast<int64_t>(NUM_ROWS - i - 1));
  }

  // Create a regular sorter with limit.
  orders.resize(1);
  expression_builder->push_row_id();
  orders[0].expression = expression_builder->release();
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  grnxx::SorterOptions options;
  options.limit = 100;
  sorter = grnxx::Sorter::create(std::move(orders), options);
  records = create_records(table);
  sorter->sort(&records);
  assert(records.size() == options.limit);
  for (size_t i = 0; i < records.size(); ++i) {
    assert(records[i].row_id.raw() == static_cast<int64_t>(i));
  }

  // Create a reverse sorter with limit.
  orders.resize(1);
  expression_builder->push_row_id();
  orders[0].expression = expression_builder->release();
  orders[0].type = grnxx::SORTER_REVERSE_ORDER;
  options.limit = 100;
  sorter = grnxx::Sorter::create(std::move(orders), options);
  records = create_records(table);
  sorter->sort(&records);
  assert(records.size() == options.limit);
  for (size_t i = 0; i < records.size(); ++i) {
    assert(records[i].row_id.raw() == static_cast<int64_t>(NUM_ROWS - i - 1));
  }

  // Create a regular sorter with offset and limit.
  orders.resize(1);
  expression_builder->push_row_id();
  orders[0].expression = expression_builder->release();
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  options.offset = 100;
  options.limit = 100;
  sorter = grnxx::Sorter::create(std::move(orders), options);
  records = create_records(table);
  sorter->sort(&records);
  assert(records.size() == options.limit);
  for (size_t i = 0; i < records.size(); ++i) {
    assert(records[i].row_id.raw() == static_cast<int64_t>(100 + i));
  }
}

void test_score() {
  // Create a table.
  auto db = grnxx::open_db("");
  auto table = db->create_table("Table");
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    table->insert_row();
  }

  // Generate scores.
  std::vector<grnxx::Float> scores(NUM_ROWS);
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    generate_value(&scores[i]);
  }
  std::vector<grnxx::Float> regular_scores(scores);
  std::sort(regular_scores.begin(), regular_scores.end(), RegularComparer());
  std::vector<grnxx::Float> reverse_scores(scores);
  std::sort(reverse_scores.begin(), reverse_scores.end(), ReverseComparer());

  // Test a regular sorter.
  grnxx::Array<grnxx::SorterOrder> orders;
  orders.resize(1);
  auto expression_builder = grnxx::ExpressionBuilder::create(table);
  expression_builder->push_score();
  orders[0].expression = expression_builder->release();
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  auto sorter = grnxx::Sorter::create(std::move(orders));
  auto records = create_records(table, scores);
  sorter->sort(&records);
  for (size_t i = 0; i < records.size(); ++i) {
    assert(records[i].score.match(regular_scores[i]));
  }

  // Test a reverse sorter.
  orders.resize(1);
  expression_builder->push_score();
  orders[0].expression = expression_builder->release();
  orders[0].type = grnxx::SORTER_REVERSE_ORDER;
  sorter = grnxx::Sorter::create(std::move(orders));
  records = create_records(table, scores);
  sorter->sort(&records);
  for (size_t i = 0; i < records.size(); ++i) {
    assert(records[i].score.match(reverse_scores[i]));
  }

  // Test a regular sorter with limit.
  orders.resize(1);
  expression_builder->push_score();
  orders[0].expression = expression_builder->release();
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  grnxx::SorterOptions options;
  options.limit = 100;
  sorter = grnxx::Sorter::create(std::move(orders), options);
  records = create_records(table, scores);
  sorter->sort(&records);
  assert(records.size() == options.limit);
  for (size_t i = 0; i < records.size(); ++i) {
    assert(records[i].score.match(regular_scores[i]));
  }

  // Test a reverse sorter with limit.
  orders.resize(1);
  expression_builder->push_score();
  orders[0].expression = expression_builder->release();
  orders[0].type = grnxx::SORTER_REVERSE_ORDER;
  options.limit = 100;
  sorter = grnxx::Sorter::create(std::move(orders), options);
  records = create_records(table, scores);
  sorter->sort(&records);
  assert(records.size() == options.limit);
  for (size_t i = 0; i < records.size(); ++i) {
    assert(records[i].score.match(reverse_scores[i]));
  }

  // Test a regular sorter with offset and limit.
  orders.resize(1);
  expression_builder->push_score();
  orders[0].expression = expression_builder->release();
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  options.offset = 100;
  options.limit = 100;
  sorter = grnxx::Sorter::create(std::move(orders), options);
  records = create_records(table, scores);
  sorter->sort(&records);
  assert(records.size() == options.limit);
  for (size_t i = 0; i < records.size(); ++i) {
    assert(records[i].score.match(regular_scores[100 + i]));
  }
}

template <typename T>
void test_value() {
  // Create a table.
  auto db = grnxx::open_db("");
  auto table = db->create_table("Table");
  auto column = table->create_column("Column", T::type());
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    table->insert_row();
  }

  // Generate values.
  std::vector<T> values(NUM_ROWS);
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    generate_value(&values[i]);
    column->set(grnxx::Int(i), values[i]);
  }
  std::vector<T> regular_values(values);
  std::sort(regular_values.begin(), regular_values.end(), RegularComparer());
  std::vector<T> reverse_values(values);
  std::sort(reverse_values.begin(), reverse_values.end(), ReverseComparer());

  // Test a regular sorter.
  grnxx::Array<grnxx::SorterOrder> orders;
  orders.resize(1);
  auto expression_builder = grnxx::ExpressionBuilder::create(table);
  expression_builder->push_column("Column");
  orders[0].expression = expression_builder->release();
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  auto sorter = grnxx::Sorter::create(std::move(orders));
  auto records = create_records(table);
  sorter->sort(&records);
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(values[row_id].match(regular_values[i]));
  }

  // Test a reverse sorter.
  orders.resize(1);
  expression_builder->push_column("Column");
  orders[0].expression = expression_builder->release();
  orders[0].type = grnxx::SORTER_REVERSE_ORDER;
  sorter = grnxx::Sorter::create(std::move(orders));
  records = create_records(table);
  sorter->sort(&records);
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(values[row_id].match(reverse_values[i]));
  }

  // Test a regular sorter with limit.
  orders.resize(1);
  expression_builder->push_column("Column");
  orders[0].expression = expression_builder->release();
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  grnxx::SorterOptions options;
  options.limit = 100;
  sorter = grnxx::Sorter::create(std::move(orders), options);
  records = create_records(table);
  sorter->sort(&records);
  assert(records.size() == options.limit);
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(values[row_id].match(regular_values[i]));
  }

  // Test a reverse sorter with limit.
  orders.resize(1);
  expression_builder->push_column("Column");
  orders[0].expression = expression_builder->release();
  orders[0].type = grnxx::SORTER_REVERSE_ORDER;
  options.limit = 100;
  sorter = grnxx::Sorter::create(std::move(orders), options);
  records = create_records(table);
  sorter->sort(&records);
  assert(records.size() == options.limit);
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(values[row_id].match(reverse_values[i]));
  }

  // Test a regular sorter with offset and limit.
  orders.resize(1);
  expression_builder->push_column("Column");
  orders[0].expression = expression_builder->release();
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  options.offset = 100;
  options.limit = 100;
  sorter = grnxx::Sorter::create(std::move(orders), options);
  records = create_records(table);
  sorter->sort(&records);
  assert(records.size() == options.limit);
  for (size_t i = 0; i < records.size(); ++i) {
    size_t row_id = records[i].row_id.raw();
    assert(values[row_id].match(regular_values[100 + i]));
  }
}

void test_composite() {
  // Create a table.
  auto db = grnxx::open_db("");
  auto table = db->create_table("Table");
  auto bool_column = table->create_column("Bool", grnxx::BOOL_DATA);
  auto int_column = table->create_column("Int", grnxx::INT_DATA);
  auto float_column = table->create_column("Float", grnxx::FLOAT_DATA);
  auto text_column = table->create_column("Text", grnxx::TEXT_DATA);
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    table->insert_row();
  }

  // Generate values.
  std::vector<grnxx::Bool> bool_values(NUM_ROWS);
  std::vector<grnxx::Float> float_values(NUM_ROWS);
  std::vector<grnxx::Int> int_values(NUM_ROWS);
  std::vector<grnxx::Text> text_values(NUM_ROWS);
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    generate_value(&bool_values[i]);
    generate_value(&int_values[i]);
    generate_value(&float_values[i]);
    generate_value(&text_values[i]);
    bool_column->set(grnxx::Int(i), bool_values[i]);
    int_column->set(grnxx::Int(i), int_values[i]);
    float_column->set(grnxx::Int(i), float_values[i]);
    text_column->set(grnxx::Int(i), text_values[i]);
  }

  // Test a regular sorter (Bool, Int, Float).
  grnxx::Array<grnxx::SorterOrder> orders;
  orders.resize(3);
  auto expression_builder = grnxx::ExpressionBuilder::create(table);
  expression_builder->push_column("Bool");
  orders[0].expression = expression_builder->release();
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  expression_builder->push_column("Int");
  orders[1].expression = expression_builder->release();
  orders[1].type = grnxx::SORTER_REGULAR_ORDER;
  expression_builder->push_column("Float");
  orders[2].expression = expression_builder->release();
  orders[2].type = grnxx::SORTER_REGULAR_ORDER;
  auto sorter = grnxx::Sorter::create(std::move(orders));
  auto records = create_records(table);
  sorter->sort(&records);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    auto lhs_value = bool_values[lhs_row_id];
    auto rhs_value = bool_values[rhs_row_id];
    if (lhs_value.unmatch(rhs_value)) {
      assert(RegularComparer()(lhs_value, rhs_value));
    } else {
      auto lhs_value = int_values[lhs_row_id];
      auto rhs_value = int_values[rhs_row_id];
      if (lhs_value.unmatch(rhs_value)) {
        assert(RegularComparer()(lhs_value, rhs_value));
      } else {
        auto lhs_value = float_values[lhs_row_id];
        auto rhs_value = float_values[rhs_row_id];
        if (lhs_value.unmatch(rhs_value)) {
          assert(RegularComparer()(lhs_value, rhs_value));
        }
      }
    }
  }

  // Test a reverse sorter (Int, Float, Bool).
  orders.resize(3);
  expression_builder->push_column("Int");
  orders[0].expression = expression_builder->release();
  orders[0].type = grnxx::SORTER_REVERSE_ORDER;
  expression_builder->push_column("Float");
  orders[1].expression = expression_builder->release();
  orders[1].type = grnxx::SORTER_REVERSE_ORDER;
  expression_builder->push_column("Bool");
  orders[2].expression = expression_builder->release();
  orders[2].type = grnxx::SORTER_REVERSE_ORDER;
  sorter = grnxx::Sorter::create(std::move(orders));
  records = create_records(table);
  sorter->sort(&records);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    auto lhs_value = int_values[lhs_row_id];
    auto rhs_value = int_values[rhs_row_id];
    if (lhs_value.unmatch(rhs_value)) {
      assert(ReverseComparer()(lhs_value, rhs_value));
    } else {
      auto lhs_value = float_values[lhs_row_id];
      auto rhs_value = float_values[rhs_row_id];
      if (lhs_value.unmatch(rhs_value)) {
        assert(ReverseComparer()(lhs_value, rhs_value));
      } else {
        auto lhs_value = bool_values[lhs_row_id];
        auto rhs_value = bool_values[rhs_row_id];
        if (lhs_value.unmatch(rhs_value)) {
          assert(ReverseComparer()(lhs_value, rhs_value));
        }
      }
    }
  }

  // Test a regular sorter (Text, Bool, Int) with limit.
  orders.resize(3);
  expression_builder->push_column("Text");
  orders[0].expression = expression_builder->release();
  orders[0].type = grnxx::SORTER_REGULAR_ORDER;
  expression_builder->push_column("Bool");
  orders[1].expression = expression_builder->release();
  orders[1].type = grnxx::SORTER_REGULAR_ORDER;
  expression_builder->push_column("Int");
  orders[2].expression = expression_builder->release();
  orders[2].type = grnxx::SORTER_REGULAR_ORDER;
  grnxx::SorterOptions options;
  options.limit = 100;
  sorter = grnxx::Sorter::create(std::move(orders), options);
  records = create_records(table);
  sorter->sort(&records);
  assert(records.size() == options.limit);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    auto lhs_value = text_values[lhs_row_id];
    auto rhs_value = text_values[rhs_row_id];
    if (lhs_value.unmatch(rhs_value)) {
      assert(RegularComparer()(lhs_value, rhs_value));
    } else {
      auto lhs_value = bool_values[lhs_row_id];
      auto rhs_value = bool_values[rhs_row_id];
      if (lhs_value.unmatch(rhs_value)) {
        assert(RegularComparer()(lhs_value, rhs_value));
      } else {
        auto lhs_value = int_values[lhs_row_id];
        auto rhs_value = int_values[rhs_row_id];
        if (lhs_value.unmatch(rhs_value)) {
          assert(RegularComparer()(lhs_value, rhs_value));
        }
      }
    }
  }

  // Test a reverse sorter (Text, Bool, Int) with limit.
  orders.resize(3);
  expression_builder->push_column("Bool");
  orders[0].expression = expression_builder->release();
  orders[0].type = grnxx::SORTER_REVERSE_ORDER;
  expression_builder->push_column("Text");
  orders[1].expression = expression_builder->release();
  orders[1].type = grnxx::SORTER_REVERSE_ORDER;
  expression_builder->push_column("Float");
  orders[2].expression = expression_builder->release();
  orders[2].type = grnxx::SORTER_REVERSE_ORDER;
  options.limit = 100;
  sorter = grnxx::Sorter::create(std::move(orders), options);
  records = create_records(table);
  sorter->sort(&records);
  assert(records.size() == options.limit);
  for (size_t i = 1; i < records.size(); ++i) {
    size_t lhs_row_id = records[i - 1].row_id.raw();
    size_t rhs_row_id = records[i].row_id.raw();
    auto lhs_value = bool_values[lhs_row_id];
    auto rhs_value = bool_values[rhs_row_id];
    if (lhs_value.unmatch(rhs_value)) {
      assert(ReverseComparer()(lhs_value, rhs_value));
    } else {
      auto lhs_value = text_values[lhs_row_id];
      auto rhs_value = text_values[rhs_row_id];
      if (lhs_value.unmatch(rhs_value)) {
        assert(ReverseComparer()(lhs_value, rhs_value));
      } else {
        auto lhs_value = float_values[lhs_row_id];
        auto rhs_value = float_values[rhs_row_id];
        if (lhs_value.unmatch(rhs_value)) {
          assert(ReverseComparer()(lhs_value, rhs_value));
        }
      }
    }
  }
}

int main() {
  test_row_id();
  test_score();
  test_value<grnxx::Bool>();
  test_value<grnxx::Int>();
  test_value<grnxx::Float>();
  test_value<grnxx::Text>();
  test_composite();
  return 0;
}
