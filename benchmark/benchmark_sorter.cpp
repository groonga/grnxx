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
#include <time.h>

#include <cassert>
#include <iostream>
#include <random>

#include "grnxx/column.hpp"
#include "grnxx/cursor.hpp"
#include "grnxx/db.hpp"
#include "grnxx/expression.hpp"
#include "grnxx/table.hpp"
#include "grnxx/sorter.hpp"

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

grnxx::Array<grnxx::Record> create_records(grnxx::Table *table) {
  auto cursor = table->create_cursor();
  grnxx::Array<grnxx::Record> records;
  size_t count = cursor->read_all(&records);
  assert(count == table->num_rows());
  return records;
}

void benchmark_int() {
  constexpr size_t NUM_ROWS = 1 << 21;
  auto db = grnxx::open_db("");
  auto table = db->create_table("Table");
  auto float_1 = table->create_column("Float_1", grnxx::FLOAT_DATA);
  auto float_2 = table->create_column("Float_2", grnxx::FLOAT_DATA);
  auto float_3 = table->create_column("Float_3", grnxx::FLOAT_DATA);
  std::mt19937_64 rng;
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = table->insert_row();
    if ((rng() % 4) != 0) {
      float_1->set(row_id, grnxx::Float(1.0 * (rng() % 256) / 255));
    }
    if ((rng() % 4) != 0) {
      float_2->set(row_id, grnxx::Float(1.0 * (rng() % 65536) / 65535));
    }
    if ((rng() % 4) != 0) {
      float_3->set(row_id, grnxx::Float(1.0 * rng() / rng.max()));
    }
  }

  {
    double best_elapsed = std::numeric_limits<double>::max();
    for (int i = 0; i < 5; ++i) {
      grnxx::Array<grnxx::Record> records = create_records(table);
      Timer timer;
      auto expression_builder = grnxx::ExpressionBuilder::create(table);
      grnxx::Array<grnxx::SorterOrder> orders;
      orders.resize(1);
      expression_builder->push_column("Float_1");
      orders[0].expression = std::move(expression_builder->release());
      orders[0].type = grnxx::SORTER_REGULAR_ORDER;
      auto sorter = grnxx::Sorter::create(std::move(orders));
      sorter->sort(&records);
      double elapsed = timer.elapsed();
      if (elapsed < best_elapsed) {
        best_elapsed = elapsed;
      }
    }
    std::cout << "Float_1" << std::endl;
    std::cout << "best elapsed [s] = " << best_elapsed << std::endl;
  }

  {
    double best_elapsed = std::numeric_limits<double>::max();
    for (int i = 0; i < 5; ++i) {
      grnxx::Array<grnxx::Record> records = create_records(table);
      Timer timer;
      auto expression_builder = grnxx::ExpressionBuilder::create(table);
      grnxx::Array<grnxx::SorterOrder> orders;
      orders.resize(1);
      expression_builder->push_column("Float_2");
      orders[0].expression = std::move(expression_builder->release());
      orders[0].type = grnxx::SORTER_REGULAR_ORDER;
      auto sorter = grnxx::Sorter::create(std::move(orders));
      sorter->sort(&records);
      double elapsed = timer.elapsed();
      if (elapsed < best_elapsed) {
        best_elapsed = elapsed;
      }
    }
    std::cout << "Float_2" << std::endl;
    std::cout << "best elapsed [s] = " << best_elapsed << std::endl;
  }

  {
    double best_elapsed = std::numeric_limits<double>::max();
    for (int i = 0; i < 5; ++i) {
      grnxx::Array<grnxx::Record> records = create_records(table);
      Timer timer;
      auto expression_builder = grnxx::ExpressionBuilder::create(table);
      grnxx::Array<grnxx::SorterOrder> orders;
      orders.resize(1);
      expression_builder->push_column("Float_3");
      orders[0].expression = std::move(expression_builder->release());
      orders[0].type = grnxx::SORTER_REGULAR_ORDER;
      auto sorter = grnxx::Sorter::create(std::move(orders));
      sorter->sort(&records);
      double elapsed = timer.elapsed();
      if (elapsed < best_elapsed) {
        best_elapsed = elapsed;
      }
    }
    std::cout << "Float_3" << std::endl;
    std::cout << "best elapsed [s] = " << best_elapsed << std::endl;
  }

  {
    double best_elapsed = std::numeric_limits<double>::max();
    for (int i = 0; i < 5; ++i) {
      grnxx::Array<grnxx::Record> records = create_records(table);
      Timer timer;
      auto expression_builder = grnxx::ExpressionBuilder::create(table);
      grnxx::Array<grnxx::SorterOrder> orders;
      orders.resize(2);
      expression_builder->push_column("Float_1");
      orders[0].expression = std::move(expression_builder->release());
      orders[0].type = grnxx::SORTER_REGULAR_ORDER;
      expression_builder->push_column("Float_2");
      orders[1].expression = std::move(expression_builder->release());
      orders[1].type = grnxx::SORTER_REGULAR_ORDER;
      auto sorter = grnxx::Sorter::create(std::move(orders));
      sorter->sort(&records);
      double elapsed = timer.elapsed();
      if (elapsed < best_elapsed) {
        best_elapsed = elapsed;
      }
    }
    std::cout << "Float_1, Float_2" << std::endl;
    std::cout << "best elapsed [s] = " << best_elapsed << std::endl;
  }

  {
    double best_elapsed = std::numeric_limits<double>::max();
    for (int i = 0; i < 5; ++i) {
      grnxx::Array<grnxx::Record> records = create_records(table);
      Timer timer;
      auto expression_builder = grnxx::ExpressionBuilder::create(table);
      grnxx::Array<grnxx::SorterOrder> orders;
      orders.resize(2);
      expression_builder->push_column("Float_1");
      orders[0].expression = std::move(expression_builder->release());
      orders[0].type = grnxx::SORTER_REGULAR_ORDER;
      expression_builder->push_column("Float_3");
      orders[1].expression = std::move(expression_builder->release());
      orders[1].type = grnxx::SORTER_REGULAR_ORDER;
      auto sorter = grnxx::Sorter::create(std::move(orders));
      sorter->sort(&records);
      double elapsed = timer.elapsed();
      if (elapsed < best_elapsed) {
        best_elapsed = elapsed;
      }
    }
    std::cout << "Float_1, Float_3" << std::endl;
    std::cout << "best elapsed [s] = " << best_elapsed << std::endl;
  }

  {
    double best_elapsed = std::numeric_limits<double>::max();
    for (int i = 0; i < 5; ++i) {
      grnxx::Array<grnxx::Record> records = create_records(table);
      Timer timer;
      auto expression_builder = grnxx::ExpressionBuilder::create(table);
      grnxx::Array<grnxx::SorterOrder> orders;
      orders.resize(2);
      expression_builder->push_column("Float_2");
      orders[0].expression = std::move(expression_builder->release());
      orders[0].type = grnxx::SORTER_REGULAR_ORDER;
      expression_builder->push_column("Float_3");
      orders[1].expression = std::move(expression_builder->release());
      orders[1].type = grnxx::SORTER_REGULAR_ORDER;
      auto sorter = grnxx::Sorter::create(std::move(orders));
      sorter->sort(&records);
      double elapsed = timer.elapsed();
      if (elapsed < best_elapsed) {
        best_elapsed = elapsed;
      }
    }
    std::cout << "Float_2, Float_3" << std::endl;
    std::cout << "best elapsed [s] = " << best_elapsed << std::endl;
  }

  {
    double best_elapsed = std::numeric_limits<double>::max();
    for (int i = 0; i < 5; ++i) {
      grnxx::Array<grnxx::Record> records = create_records(table);
      Timer timer;
      auto expression_builder = grnxx::ExpressionBuilder::create(table);
      grnxx::Array<grnxx::SorterOrder> orders;
      orders.resize(3);
      expression_builder->push_column("Float_1");
      orders[0].expression = std::move(expression_builder->release());
      orders[0].type = grnxx::SORTER_REGULAR_ORDER;
      expression_builder->push_column("Float_2");
      orders[1].expression = std::move(expression_builder->release());
      orders[1].type = grnxx::SORTER_REGULAR_ORDER;
      expression_builder->push_column("Float_3");
      orders[2].expression = std::move(expression_builder->release());
      orders[2].type = grnxx::SORTER_REGULAR_ORDER;
      auto sorter = grnxx::Sorter::create(std::move(orders));
      sorter->sort(&records);
      double elapsed = timer.elapsed();
      if (elapsed < best_elapsed) {
        best_elapsed = elapsed;
      }
    }
    std::cout << "Float_1, Float_2, Float_3" << std::endl;
    std::cout << "best elapsed [s] = " << best_elapsed << std::endl;
  }
}

void benchmark_float() {
  constexpr size_t NUM_ROWS = 1 << 21;
  auto db = grnxx::open_db("");
  auto table = db->create_table("Table");
  auto int_1 = table->create_column("Int_1", grnxx::INT_DATA);
  auto int_2 = table->create_column("Int_2", grnxx::INT_DATA);
  auto int_3 = table->create_column("Int_3", grnxx::INT_DATA);
  std::mt19937_64 rng;
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    grnxx::Int row_id = table->insert_row();
    if ((rng() % 4) != 0) {
      int_1->set(row_id, grnxx::Int(rng() % 256));
    }
    if ((rng() % 4) != 0) {
      int_2->set(row_id, grnxx::Int(rng() % 65536));
    }
    if ((rng() % 4) != 0) {
      int_3->set(row_id, grnxx::Int(rng()));
    }
  }

  {
    double best_elapsed = std::numeric_limits<double>::max();
    for (int i = 0; i < 5; ++i) {
      grnxx::Array<grnxx::Record> records = create_records(table);
      Timer timer;
      auto expression_builder = grnxx::ExpressionBuilder::create(table);
      grnxx::Array<grnxx::SorterOrder> orders;
      orders.resize(1);
      expression_builder->push_column("Int_1");
      orders[0].expression = std::move(expression_builder->release());
      orders[0].type = grnxx::SORTER_REGULAR_ORDER;
      auto sorter = grnxx::Sorter::create(std::move(orders));
      sorter->sort(&records);
      double elapsed = timer.elapsed();
      if (elapsed < best_elapsed) {
        best_elapsed = elapsed;
      }
    }
    std::cout << "Int_1" << std::endl;
    std::cout << "best elapsed [s] = " << best_elapsed << std::endl;
  }

  {
    double best_elapsed = std::numeric_limits<double>::max();
    for (int i = 0; i < 5; ++i) {
      grnxx::Array<grnxx::Record> records = create_records(table);
      Timer timer;
      auto expression_builder = grnxx::ExpressionBuilder::create(table);
      grnxx::Array<grnxx::SorterOrder> orders;
      orders.resize(1);
      expression_builder->push_column("Int_2");
      orders[0].expression = std::move(expression_builder->release());
      orders[0].type = grnxx::SORTER_REGULAR_ORDER;
      auto sorter = grnxx::Sorter::create(std::move(orders));
      sorter->sort(&records);
      double elapsed = timer.elapsed();
      if (elapsed < best_elapsed) {
        best_elapsed = elapsed;
      }
    }
    std::cout << "Int_2" << std::endl;
    std::cout << "best elapsed [s] = " << best_elapsed << std::endl;
  }

  {
    double best_elapsed = std::numeric_limits<double>::max();
    for (int i = 0; i < 5; ++i) {
      grnxx::Array<grnxx::Record> records = create_records(table);
      Timer timer;
      auto expression_builder = grnxx::ExpressionBuilder::create(table);
      grnxx::Array<grnxx::SorterOrder> orders;
      orders.resize(1);
      expression_builder->push_column("Int_3");
      orders[0].expression = std::move(expression_builder->release());
      orders[0].type = grnxx::SORTER_REGULAR_ORDER;
      auto sorter = grnxx::Sorter::create(std::move(orders));
      sorter->sort(&records);
      double elapsed = timer.elapsed();
      if (elapsed < best_elapsed) {
        best_elapsed = elapsed;
      }
    }
    std::cout << "Int_3" << std::endl;
    std::cout << "best elapsed [s] = " << best_elapsed << std::endl;
  }

  {
    double best_elapsed = std::numeric_limits<double>::max();
    for (int i = 0; i < 5; ++i) {
      grnxx::Array<grnxx::Record> records = create_records(table);
      Timer timer;
      auto expression_builder = grnxx::ExpressionBuilder::create(table);
      grnxx::Array<grnxx::SorterOrder> orders;
      orders.resize(2);
      expression_builder->push_column("Int_1");
      orders[0].expression = std::move(expression_builder->release());
      orders[0].type = grnxx::SORTER_REGULAR_ORDER;
      expression_builder->push_column("Int_2");
      orders[1].expression = std::move(expression_builder->release());
      orders[1].type = grnxx::SORTER_REGULAR_ORDER;
      auto sorter = grnxx::Sorter::create(std::move(orders));
      sorter->sort(&records);
      double elapsed = timer.elapsed();
      if (elapsed < best_elapsed) {
        best_elapsed = elapsed;
      }
    }
    std::cout << "Int_1, Int_2" << std::endl;
    std::cout << "best elapsed [s] = " << best_elapsed << std::endl;
  }

  {
    double best_elapsed = std::numeric_limits<double>::max();
    for (int i = 0; i < 5; ++i) {
      grnxx::Array<grnxx::Record> records = create_records(table);
      Timer timer;
      auto expression_builder = grnxx::ExpressionBuilder::create(table);
      grnxx::Array<grnxx::SorterOrder> orders;
      orders.resize(2);
      expression_builder->push_column("Int_1");
      orders[0].expression = std::move(expression_builder->release());
      orders[0].type = grnxx::SORTER_REGULAR_ORDER;
      expression_builder->push_column("Int_3");
      orders[1].expression = std::move(expression_builder->release());
      orders[1].type = grnxx::SORTER_REGULAR_ORDER;
      auto sorter = grnxx::Sorter::create(std::move(orders));
      sorter->sort(&records);
      double elapsed = timer.elapsed();
      if (elapsed < best_elapsed) {
        best_elapsed = elapsed;
      }
    }
    std::cout << "Int_1, Int_3" << std::endl;
    std::cout << "best elapsed [s] = " << best_elapsed << std::endl;
  }

  {
    double best_elapsed = std::numeric_limits<double>::max();
    for (int i = 0; i < 5; ++i) {
      grnxx::Array<grnxx::Record> records = create_records(table);
      Timer timer;
      auto expression_builder = grnxx::ExpressionBuilder::create(table);
      grnxx::Array<grnxx::SorterOrder> orders;
      orders.resize(2);
      expression_builder->push_column("Int_2");
      orders[0].expression = std::move(expression_builder->release());
      orders[0].type = grnxx::SORTER_REGULAR_ORDER;
      expression_builder->push_column("Int_3");
      orders[1].expression = std::move(expression_builder->release());
      orders[1].type = grnxx::SORTER_REGULAR_ORDER;
      auto sorter = grnxx::Sorter::create(std::move(orders));
      sorter->sort(&records);
      double elapsed = timer.elapsed();
      if (elapsed < best_elapsed) {
        best_elapsed = elapsed;
      }
    }
    std::cout << "Int_2, Int_3" << std::endl;
    std::cout << "best elapsed [s] = " << best_elapsed << std::endl;
  }

  {
    double best_elapsed = std::numeric_limits<double>::max();
    for (int i = 0; i < 5; ++i) {
      grnxx::Array<grnxx::Record> records = create_records(table);
      Timer timer;
      auto expression_builder = grnxx::ExpressionBuilder::create(table);
      grnxx::Array<grnxx::SorterOrder> orders;
      orders.resize(3);
      expression_builder->push_column("Int_1");
      orders[0].expression = std::move(expression_builder->release());
      orders[0].type = grnxx::SORTER_REGULAR_ORDER;
      expression_builder->push_column("Int_2");
      orders[1].expression = std::move(expression_builder->release());
      orders[1].type = grnxx::SORTER_REGULAR_ORDER;
      expression_builder->push_column("Int_3");
      orders[2].expression = std::move(expression_builder->release());
      orders[2].type = grnxx::SORTER_REGULAR_ORDER;
      auto sorter = grnxx::Sorter::create(std::move(orders));
      sorter->sort(&records);
      double elapsed = timer.elapsed();
      if (elapsed < best_elapsed) {
        best_elapsed = elapsed;
      }
    }
    std::cout << "Int_1, Int_2, Int_3" << std::endl;
    std::cout << "best elapsed [s] = " << best_elapsed << std::endl;
  }
}

int main() {
  benchmark_int();
  benchmark_float();
  return 0;
}
