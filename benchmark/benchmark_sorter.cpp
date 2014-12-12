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
#include <cstdio>
#include <ctime>
#include <iostream>
#include <random>

#include "grnxx/db.hpp"
#include "grnxx/sorter.hpp"

namespace {

constexpr size_t SIZE = 2000000;
constexpr size_t LOOP = 5;

class Timer {
 public:
  Timer() : base_(now()) {}

  double elapsed() const {
    return now() - base_;
  }

  static double now() {
    struct timespec ts;
    ::clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + (ts.tv_nsec / 1000000000.0);
  }

 private:
  double base_;
};

void create_bool_columns(grnxx::Table *table) {
  grnxx::Column *columns[3];
  columns[0] = table->create_column("Bool1", grnxx::BOOL_DATA);
  columns[1] = table->create_column("Bool2", grnxx::BOOL_DATA);
  columns[2] = table->create_column("Bool3", grnxx::BOOL_DATA);

  std::mt19937_64 rng;
  for (size_t i = 0; i < SIZE; ++i) {
    grnxx::Int row_id(i);
    columns[0]->set(row_id, grnxx::Bool((rng() % 4) != 0));
    columns[1]->set(row_id, grnxx::Bool((rng() % 2) != 0));
    if ((rng() % 4) != 0) {
      columns[2]->set(row_id, grnxx::Bool((rng() % 2) != 0));
    }
  }
}

void create_int_columns(grnxx::Table *table) {
  grnxx::Column *columns[3];
  columns[0] = table->create_column("Int1", grnxx::INT_DATA);
  columns[1] = table->create_column("Int2", grnxx::INT_DATA);
  columns[2] = table->create_column("Int3", grnxx::INT_DATA);

  std::mt19937_64 rng;
  for (size_t i = 0; i < SIZE; ++i) {
    grnxx::Int row_id(i);
    if ((rng() % 256) != 0) {
      columns[0]->set(row_id, grnxx::Int(rng() % 256));
    }
    if ((rng() % 65536) != 0) {
      columns[1]->set(row_id, grnxx::Int(rng() % 65536));
    }
    columns[2]->set(row_id, grnxx::Int(rng()));
  }
}

void create_float_columns(grnxx::Table *table) {
  grnxx::Column *columns[3];
  columns[0] = table->create_column("Float1", grnxx::FLOAT_DATA);
  columns[1] = table->create_column("Float2", grnxx::FLOAT_DATA);
  columns[2] = table->create_column("Float3", grnxx::FLOAT_DATA);

  std::mt19937_64 rng;
  for (size_t i = 0; i < SIZE; ++i) {
    grnxx::Int row_id(i);
    if ((rng() % 256) != 0) {
      columns[0]->set(row_id, grnxx::Float((rng() % 256) / 256.0));
    }
    if ((rng() % 65536) != 0) {
      columns[1]->set(row_id, grnxx::Float((rng() % 65536) / 65536.0));
    }
    columns[2]->set(row_id, grnxx::Float(1.0 * rng() / rng.max()));
  }
}

void create_text_columns(grnxx::Table *table) {
  grnxx::Column *columns[3];
  columns[0] = table->create_column("Text1", grnxx::TEXT_DATA);
  columns[1] = table->create_column("Text2", grnxx::TEXT_DATA);
  columns[2] = table->create_column("Text3", grnxx::TEXT_DATA);

  std::mt19937_64 rng;
  char buf[16];
  for (size_t i = 0; i < SIZE; ++i) {
    grnxx::Int row_id(i);
    std::sprintf(buf, "%02d", static_cast<int>(rng() % 100));
    columns[0]->set(row_id, grnxx::Text(buf));
    std::sprintf(buf, "%04d", static_cast<int>(rng() % 10000));
    columns[1]->set(row_id, grnxx::Text(buf));
    std::sprintf(buf, "%06d", static_cast<int>(rng() % 1000000));
    columns[2]->set(row_id, grnxx::Text(buf));
  }
}

grnxx::Table *create_table(grnxx::DB *db) {
  auto table = db->create_table("Table");
  for (size_t i = 0; i < SIZE; ++i) {
    table->insert_row();
  }
  create_bool_columns(table);
  create_int_columns(table);
  create_float_columns(table);
  create_text_columns(table);
  return table;
}

grnxx::Array<grnxx::Record> create_records(grnxx::Table *table) {
  grnxx::Array<grnxx::Record> records;
  auto cursor = table->create_cursor();
  assert(cursor->read_all(&records) == SIZE);
  return records;
}

void benchmark_row_id(grnxx::Table *table, size_t limit) {
  if (limit != std::numeric_limits<size_t>::max()) {
    std::cout << "limit = " << limit << ", ";
  } else {
    std::cout << "limit = N/A, ";
  }

  double min_elapsed = std::numeric_limits<double>::max();
  std::mt19937_64 rng;
  for (size_t i = 0; i < LOOP; ++i) {
    std::vector<grnxx::Int> row_ids(SIZE);
    for (size_t i = 0; i < SIZE; ++i) {
      row_ids[i] = grnxx::Int(i);
    }
    std::shuffle(row_ids.begin(), row_ids.end(), rng);

    grnxx::Array<grnxx::Record> records;
    records.resize(SIZE);
    for (size_t j = 0; j < SIZE; ++j) {
      records[j].row_id = row_ids[j];
      records[j].score = grnxx::Float(0.0);
    }

    Timer timer;
    auto expression_builder = grnxx::ExpressionBuilder::create(table);
    grnxx::Array<grnxx::SorterOrder> orders;
    orders.resize(1);
    expression_builder->push_row_id();
    orders[0].expression = std::move(expression_builder->release());
    orders[0].type = grnxx::SORTER_REGULAR_ORDER;
    grnxx::SorterOptions options;
    options.limit = limit;
    auto sorter = grnxx::Sorter::create(std::move(orders), options);
    sorter->sort(&records);
    double elapsed = timer.elapsed();
    if (elapsed < min_elapsed) {
      min_elapsed = elapsed;
    }
  }
  std::cout << "min. elapsed [s] = " << min_elapsed << std::endl;
}

void benchmark_row_id(grnxx::Table *table) {
  std::cout << __PRETTY_FUNCTION__ << std::endl;

  benchmark_row_id(table, 10);
  benchmark_row_id(table, 100);
  benchmark_row_id(table, 1000);
  benchmark_row_id(table, 10000);
  benchmark_row_id(table, 100000);
  benchmark_row_id(table, std::numeric_limits<size_t>::max());
}

void benchmark_score(grnxx::Table *table, const char *column_name) {
  double min_elapsed = std::numeric_limits<double>::max();
  for (size_t i = 0; i < LOOP; ++i) {
    auto records = create_records(table);
    auto expression_builder = grnxx::ExpressionBuilder::create(table);
    expression_builder->push_column(column_name);
    expression_builder->release()->adjust(&records);

    Timer timer;
    grnxx::Array<grnxx::SorterOrder> orders;
    orders.resize(1);
    expression_builder->push_score();
    orders[0].expression = std::move(expression_builder->release());
    orders[0].type = grnxx::SORTER_REGULAR_ORDER;
    auto sorter = grnxx::Sorter::create(std::move(orders));
    sorter->sort(&records);
    double elapsed = timer.elapsed();
    if (elapsed < min_elapsed) {
      min_elapsed = elapsed;
    }
  }
  std::cout << column_name << ": "
            << "RowID: min. elapsed [s] = " << min_elapsed << std::endl;
}

void benchmark_score(grnxx::Table *table) {
  std::cout << __PRETTY_FUNCTION__ << std::endl;

  benchmark_score(table, "Float1");
  benchmark_score(table, "Float2");
  benchmark_score(table, "Float3");
}

void benchmark_columns(grnxx::Table *table, const char *column_names) {
  // Parse "column_names" as comma-separated column names.
  grnxx::Array<grnxx::String> column_name_array;
  grnxx::String string(column_names);
  while (!string.is_empty()) {
    size_t delim_pos = 0;
    while (delim_pos < string.size()) {
      if (string[delim_pos] == ',') {
        break;
      }
      ++delim_pos;
    }
    column_name_array.push_back(string.substring(0, delim_pos));
    if (delim_pos == string.size()) {
      break;
    }
    string = string.substring(delim_pos + 1);
  }

  double min_elapsed = std::numeric_limits<double>::max();
  for (size_t i = 0; i < LOOP; ++i) {
    grnxx::Array<grnxx::Record> records = create_records(table);

    Timer timer;
    auto expression_builder = grnxx::ExpressionBuilder::create(table);
    grnxx::Array<grnxx::SorterOrder> orders;
    orders.resize(column_name_array.size());
    for (size_t j = 0; j < orders.size(); ++j) {
      expression_builder->push_column(column_name_array[j]);
      orders[j].expression = std::move(expression_builder->release());
      orders[j].type = grnxx::SORTER_REGULAR_ORDER;
    }
    auto sorter = grnxx::Sorter::create(std::move(orders));
    sorter->sort(&records);
    double elapsed = timer.elapsed();
    if (elapsed < min_elapsed) {
      min_elapsed = elapsed;
    }
  }
  std::cout << column_names << ": "
            << "min. elapsed [s] = " << min_elapsed << std::endl;
}

void benchmark_bool(grnxx::Table *table) {
  std::cout << __PRETTY_FUNCTION__ << std::endl;

  benchmark_columns(table, "Bool1");
  benchmark_columns(table, "Bool2");
  benchmark_columns(table, "Bool3");
  benchmark_columns(table, "Bool1,Bool2");
  benchmark_columns(table, "Bool1,Bool3");
  benchmark_columns(table, "Bool2,Bool3");
  benchmark_columns(table, "Bool1,Bool2,Bool3");
}

void benchmark_int(grnxx::Table *table) {
  std::cout << __PRETTY_FUNCTION__ << std::endl;

  benchmark_columns(table, "Int1");
  benchmark_columns(table, "Int2");
  benchmark_columns(table, "Int3");
  benchmark_columns(table, "Int1,Int2");
  benchmark_columns(table, "Int1,Int3");
  benchmark_columns(table, "Int2,Int3");
  benchmark_columns(table, "Int1,Int2,Int3");
}

void benchmark_float(grnxx::Table *table) {
  std::cout << __PRETTY_FUNCTION__ << std::endl;

  benchmark_columns(table, "Float1");
  benchmark_columns(table, "Float2");
  benchmark_columns(table, "Float3");
  benchmark_columns(table, "Float1,Float2");
  benchmark_columns(table, "Float1,Float3");
  benchmark_columns(table, "Float2,Float3");
  benchmark_columns(table, "Float1,Float2,Float3");
}

void benchmark_text(grnxx::Table *table) {
  std::cout << __PRETTY_FUNCTION__ << std::endl;

  benchmark_columns(table, "Text1");
  benchmark_columns(table, "Text2");
  benchmark_columns(table, "Text3");
  benchmark_columns(table, "Text1,Text2");
  benchmark_columns(table, "Text1,Text3");
  benchmark_columns(table, "Text2,Text3");
  benchmark_columns(table, "Text1,Text2,Text3");
}

}  // namespace

int main() {
  auto db = grnxx::open_db("");
  auto table = create_table(db.get());

  benchmark_row_id(table);
  benchmark_score(table);
  benchmark_bool(table);
  benchmark_int(table);
  benchmark_float(table);
  benchmark_text(table);
  return 0;
}
