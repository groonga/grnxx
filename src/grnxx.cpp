#include <getopt.h>
#include <unistd.h>

#include <cctype>
#include <iostream>
#include <sstream>

#include "grnxx/calc.hpp"
#include "grnxx/column.hpp"
#include "grnxx/database.hpp"
#include "grnxx/index.hpp"
#include "grnxx/library.hpp"
#include "grnxx/sorter.hpp"
#include "grnxx/table.hpp"
#include "grnxx/timer.hpp"

namespace {

bool verbose_mode = false;

// 入力された行をコマンドと引数に切り分ける．
bool extract_command(const std::string &line,
                     grnxx::String *command, grnxx::String *params) {
  // 先頭の空白をスキップする．
  std::size_t begin = line.find_first_not_of(" \t\n");
  if ((begin == line.npos) || (line[begin] == '#')) {
    // 空行および'#'で始まる行（コメント）は無視する．
    return false;
  }
  // 次の空白までをコマンドとして抜き出す．
  std::size_t end = line.find_first_of(" \t\n", begin);
  if (end == line.npos) {
    end = line.size();
  }
  *command = grnxx::String(line.data() + begin, end - begin);
  // コマンドの後ろにある空白をスキップする．
  begin = line.find_first_not_of(" \t\n", end);
  if (begin == line.npos) {
    begin = line.size();
  }
  end = line.find_last_not_of(" \t\n");
  if (end == line.npos) {
    end = line.size();
  }
  *params = grnxx::String(line.data() + begin, end - begin + 1);
  return true;
}

// table_create コマンド．
bool run_table_create(grnxx::Database *database,
                      const grnxx::String &params) {
  auto delim_pos = params.find_first_of(" \t\n");
  if (delim_pos == params.npos) {
    delim_pos = params.size();
  }
  grnxx::String table_name = params.prefix(delim_pos);
  if (!database->create_table(table_name)) {
    std::cerr << "Error: grnxx::Database::create_table() failed: "
              << "table_name = " << table_name << std::endl;
    return false;
  }
  std::cout << "OK\n";
  return true;
}

// table_remove コマンド．
bool run_table_remove(grnxx::Database *database,
                      const grnxx::String &params) {
  auto delim_pos = params.find_first_of(" \t\n");
  if (delim_pos == params.npos) {
    delim_pos = params.size();
  }
  grnxx::String table_name = params.prefix(delim_pos);
  if (!database->drop_table(table_name)) {
    std::cerr << "Error: grnxx::Database::drop_table() failed: "
              << "table_name = " << table_name << std::endl;
    return false;
  }
  std::cout << "OK\n";
  return true;
}

// table_list コマンド．
bool run_table_list(grnxx::Database *database,
                    const grnxx::String &params) {
  std::vector<grnxx::Table *> tables;
  database->get_tables(&tables);
  for (auto table : tables) {
    std::cout << "id = " << table->id()
              << ", name = " << table->name() << '\n';
  }
  std::cout << "OK\n";
  return true;
}

// column_create コマンド．
bool run_column_create(grnxx::Database *database,
                       const grnxx::String &params) {
  auto end = params.find_first_of(" \t\n");
  if (end == params.npos) {
    std::cerr << "Error: too few arguments" << std::endl;
    return false;
  }
  grnxx::String table_name = params.prefix(end);
  auto table = database->get_table_by_name(table_name);
  if (!table) {
    std::cerr << "Error: table not found: "
              << "table_name = " << table_name << std::endl;
    return false;
  }
  auto begin = params.find_first_not_of(" \t\n", end);
  end = params.find_first_of(" \t\n", begin);
  if (end == params.npos) {
    std::cerr << "Error: too few arguments" << std::endl;
    return false;
  }
  grnxx::String column_name = params.extract(begin, end - begin);
  begin = params.find_first_not_of(" \t\n", end);
  end = params.find_first_of(" \t\n", begin);
  if (end == params.npos) {
    end = params.size();
  }
  grnxx::String data_type_name = params.extract(begin, end - begin);
  grnxx::DataType data_type;
  if (data_type_name == "BOOLEAN") {
    data_type = grnxx::BOOLEAN;
  } else if ((data_type_name == "INT8") ||
             (data_type_name == "INT16") ||
             (data_type_name == "INT32") ||
             (data_type_name == "INT64")) {
    data_type = grnxx::INTEGER;
  } else if (data_type_name == "FLOAT") {
    data_type = grnxx::FLOAT;
  } else if (data_type_name == "STRING") {
    data_type = grnxx::STRING;
  } else {
    std::cerr << "Error: Unknown data type: "
              << "data_type = " << data_type_name << std::endl;
    return false;
  }
  if (!table->create_column(column_name, data_type)) {
    std::cerr << "Error: grnxx::Table::create_column() failed: "
              << "table_name = \"" << table_name << '"'
              << ", column_name = \"" << column_name << '"'
              << ", data_type = " << data_type << std::endl;
    return false;
  }
  std::cout << "OK\n";
  return true;
}

// column_remove コマンド．
bool run_column_remove(grnxx::Database *database,
                       const grnxx::String &params) {
  auto end = params.find_first_of(" \t\n");
  if (end == params.npos) {
    std::cerr << "Error: too few arguments" << std::endl;
    return false;
  }
  grnxx::String table_name = params.prefix(end);
  auto table = database->get_table_by_name(table_name);
  if (!table) {
    std::cerr << "Error: table not found: "
              << "table_name = " << table_name << std::endl;
    return false;
  }
  auto begin = params.find_first_not_of(" \t\n", end);
  end = params.find_first_of(" \t\n", begin);
  if (end == params.npos) {
    end = params.size();
  }
  grnxx::String column_name = params.extract(begin, end - begin);
  if (!table->drop_column(column_name)) {
    std::cerr << "Error: grnxx::Table::drop_column() failed: "
              << "table_name = " << table_name
              << ", column_name = " << column_name << std::endl;
    return false;
  }
  std::cout << "OK\n";
  return true;
}

// column_list コマンド．
bool run_column_list(grnxx::Database *database,
                     const grnxx::String &params) {
  auto delim_pos = params.find_first_of(" \t\n");
  if (delim_pos == params.npos) {
    delim_pos = params.size();
  }
  grnxx::String table_name = params.prefix(delim_pos);
  auto table = database->get_table_by_name(table_name);
  if (!table) {
    std::cerr << "Error: table not found: table_name = "
              << table_name << std::endl;
    return false;
  }
  std::vector<grnxx::Column *> columns;
  table->get_columns(&columns);
  for (auto column : columns) {
    std::cout << "id = " << column->id()
              << ", name = \"" << column->name() << '"'
              << ", type = " << column->data_type() << '\n';
  }
  std::cout << "OK\n";
  return true;
}

// index_create コマンド．
bool run_index_create(grnxx::Database *database,
                       const grnxx::String &params) {
  auto end = params.find_first_of(" \t\n");
  if (end == params.npos) {
    std::cerr << "Error: too few arguments" << std::endl;
    return false;
  }
  grnxx::String table_name = params.prefix(end);
  auto table = database->get_table_by_name(table_name);
  if (!table) {
    std::cerr << "Error: table not found: "
              << "table_name = " << table_name << std::endl;
    return false;
  }
  auto begin = params.find_first_not_of(" \t\n", end);
  end = params.find_first_of(" \t\n", begin);
  if (end == params.npos) {
    std::cerr << "Error: too few arguments" << std::endl;
    return false;
  }
  grnxx::String index_name = params.extract(begin, end - begin);
  begin = params.find_first_not_of(" \t\n", end);
  end = params.find_first_of(" \t\n", begin);
  if (end == params.npos) {
    end = params.size();
  }
  grnxx::String column_name = params.extract(begin, end - begin);
  if (end == params.size()) {
    begin = end;
  } else {
    begin = params.find_first_not_of(" \t\n", end);
    end = params.find_first_of(" \t\n", begin);
    if (end == params.npos) {
      end = params.size();
    }
  }
  grnxx::String index_type_name = params.extract(begin, end - begin);
  grnxx::IndexType index_type = grnxx::TREE_MAP;
  if (index_type_name.empty()) {
    index_type = grnxx::TREE_MAP;
  } else if (index_type_name == "TREE_MAP") {
    index_type = grnxx::TREE_MAP;
  } else {
    std::cerr << "Error: Unknown index type: "
              << "index_type = " << index_type_name << std::endl;
    return false;
  }
  if (!table->create_index(index_name, column_name, index_type)) {
    std::cerr << "Error: grnxx::Table::create_index() failed: "
              << "table_name = " << table_name
              << ", index_name = " << index_name
              << ", column_name = " << column_name
              << ", index_type = " << index_type << std::endl;
    return false;
  }
  std::cout << "OK\n";
  return true;
}

// index_remove コマンド．
bool run_index_remove(grnxx::Database *database,
                      const grnxx::String &params) {
  auto end = params.find_first_of(" \t\n");
  if (end == params.npos) {
    std::cerr << "Error: too few arguments" << std::endl;
    return false;
  }
  grnxx::String table_name = params.prefix(end);
  auto table = database->get_table_by_name(table_name);
  if (!table) {
    std::cerr << "Error: table not found: "
              << "table_name = " << table_name << std::endl;
    return false;
  }
  auto begin = params.find_first_not_of(" \t\n", end);
  end = params.find_first_of(" \t\n", begin);
  if (end == params.npos) {
    end = params.size();
  }
  grnxx::String index_name = params.extract(begin, end - begin);
  if (!table->drop_index(index_name)) {
    std::cerr << "Error: grnxx::Table::drop_index() failed: "
              << "table_name = " << table_name
              << ", index_name = " << index_name << std::endl;
    return false;
  }
  std::cout << "OK\n";
  return true;
}

// index_list コマンド．
bool run_index_list(grnxx::Database *database,
                    const grnxx::String &params) {
  auto delim_pos = params.find_first_of(" \t\n");
  if (delim_pos == params.npos) {
    delim_pos = params.size();
  }
  grnxx::String table_name = params.prefix(delim_pos);
  auto table = database->get_table_by_name(table_name);
  if (!table) {
    std::cerr << "Error: table not found: table_name = "
              << table_name << std::endl;
    return false;
  }
  std::vector<grnxx::Index *> indexes;
  table->get_indexes(&indexes);
  for (auto index : indexes) {
    std::cout << "id = " << index->id()
              << ", name = \"" << index->name() << '"'
              << ", column = \"" << index->column()->name() << '"'
              << ", type = " << index->type() << '\n';
  }
  std::cout << "OK\n";
  return true;
}

// load コマンド．
bool run_load(grnxx::Database *database,
              const grnxx::String &params) {
  auto end = params.find_first_of(" \t\n");
  if (end == params.npos) {
    end = params.size();
  }
  grnxx::String table_name = params.prefix(end);
  auto table = database->get_table_by_name(table_name);
  if (!table) {
    std::cerr << "Error: table not found: table_name = "
              << table_name << std::endl;
    return false;
  }
  auto begin = params.find_first_not_of(" \t\n", end);
  if (begin == params.npos) {
    // 入力がひとつもない．
    std::cout << "OK: 0 rows\n";
    return true;
  }
  grnxx::Int64 count = 0;
  grnxx::Datum datum;
  do {
    ++count;
    grnxx::RowID row_id = table->insert_row();
    if (!row_id) {
      std::cerr << "Error: Table::add_row() failed" << std::endl;
      return false;
    }
    for (grnxx::ColumnID column_id = table->min_column_id();
         column_id <= table->max_column_id(); ++column_id) {
      auto column = table->get_column_by_id(column_id);
      if (!column) {
        continue;
      }
      if (params[begin] == '"') {
        // 文字列．
        end = params.find_first_of('"', ++begin);
        if (end == params.npos) {
          end = params.size();
        }
        datum = params.extract(begin, end - begin);
        if (end != params.size()) {
          ++end;
        }
      } else {
        // そのほか．
        end = params.find_first_of(" \t\n", begin);
        if (end == params.npos) {
          end = params.size();
        }
        datum = params.extract(begin, end - begin);
      }
      column->generic_set(table->max_row_id(), datum);
      begin = params.find_first_not_of(" \t\n", end);
      if (begin == params.npos) {
        break;
      }
    }
  } while (begin != params.npos);
  std::cout << "OK: " << count << " rows\n";
  return true;
}

struct SelectQuery {
  grnxx::String table_name;
  grnxx::String output_column_names;
  grnxx::String index_query;
  grnxx::String calc_query;
  grnxx::Int64 offset;
  grnxx::Int64 limit;
  grnxx::String column_names_for_sort_by;
  grnxx::String column_names_for_group_by;
};

bool parse_select_params(grnxx::String params, SelectQuery *query) {
  // テーブルの名前を切り出す．
  auto boundary = params.find_first_of(" \t\n");
  if (boundary == params.npos) {
    boundary = params.size();
  }
  query->table_name = params.prefix(boundary);
  params = params.except_prefix(boundary);
  boundary = params.find_first_not_of(" \t\n");
  if (boundary == params.npos) {
    boundary = params.size();
  }
  params = params.except_prefix(boundary);

  // 出力カラム名を切り出す．
  boundary = params.find_first_of(" \t\n");
  if (boundary == params.npos) {
    boundary = params.size();
  }
  query->output_column_names = params.prefix(boundary);
  params = params.except_prefix(boundary);
  boundary = params.find_first_not_of(" \t\n");
  if (boundary == params.npos) {
    boundary = params.size();
  }
  params = params.except_prefix(boundary);

  // 索引を使うクエリを切り出す．
  query->index_query = "";
  if (!params.empty() && (params[0] == '`')) {
    boundary = params.find_first_of('`', 1);
    if (boundary == params.npos) {
      std::cerr << "Error: closing back quote does not exist" << std::endl;
      return false;
    }
    query->index_query = params.extract(1, boundary - 1);
    params = params.except_prefix(boundary + 1);
  }
  boundary = params.find_first_not_of(" \t\n");
  if (boundary == params.npos) {
    boundary = params.size();
  }
  params = params.except_prefix(boundary);

  // フィルタのクエリを切り出す．
  query->calc_query = "";
  if (!params.empty() && (params[0] == '\'')) {
    boundary = params.find_first_of('\'', 1);
    if (boundary == params.npos) {
      std::cerr << "Error: closing single quote does not exist" << std::endl;
      return false;
    }
    query->calc_query = params.extract(1, boundary - 1);
    params = params.except_prefix(boundary + 1);
  }
  boundary = params.find_first_not_of(" \t\n");
  if (boundary == params.npos) {
    boundary = params.size();
  }
  params = params.except_prefix(boundary);

  // オフセットを切り出す．
  query->offset = 0;
  if (!params.empty() && std::isdigit(params[0])) {
    boundary = params.find_first_of(" \t\n");
    if (boundary == params.npos) {
      boundary = params.size();
    }
    query->offset = std::strtol(reinterpret_cast<const char *>(params.data()),
                                nullptr, 10);
    params = params.except_prefix(boundary);
  }
  boundary = params.find_first_not_of(" \t\n");
  if (boundary == params.npos) {
    boundary = params.size();
  }
  params = params.except_prefix(boundary);

  // 上限数を切り出す．
  query->limit = INT64_MAX;
  if (!params.empty() && std::isdigit(params[0])) {
    boundary = params.find_first_of(" \t\n");
    if (boundary == params.npos) {
      boundary = params.size();
    }
    query->limit = std::strtol(reinterpret_cast<const char *>(params.data()),
                               nullptr, 10);
    params = params.except_prefix(boundary);
  }
  boundary = params.find_first_not_of(" \t\n");
  if (boundary == params.npos) {
    boundary = params.size();
  }
  params = params.except_prefix(boundary);

  // 整列条件を切り出す．
  boundary = params.find_first_of(" \t\n");
  if (boundary == params.npos) {
    boundary = params.size();
  }
  query->column_names_for_sort_by = params.prefix(boundary);
  params = params.except_prefix(boundary);
  boundary = params.find_first_not_of(" \t\n");
  if (boundary == params.npos) {
    boundary = params.size();
  }
  params = params.except_prefix(boundary);

  // グループ化の条件を切り出す．
  boundary = params.find_first_of(" \t\n");
  if (boundary == params.npos) {
    boundary = params.size();
  }
  query->column_names_for_group_by = params.prefix(boundary);
  params = params.except_prefix(boundary);
  boundary = params.find_first_not_of(" \t\n");
  if (boundary == params.npos) {
    boundary = params.size();
  }
  params = params.except_prefix(boundary);

  if (verbose_mode) {
    std::cout << "table_name = " << query->table_name << '\n'
              << "output = " << query->output_column_names << '\n'
              << "index_query = " << query->index_query << '\n'
              << "calc_query = " << query->calc_query << '\n'
              << "offset = " << query->offset << '\n'
              << "limit = " << query->limit << '\n'
              << "sort_by = " << query->column_names_for_sort_by << '\n'
              << "group_by = " << query->column_names_for_group_by << '\n';
  }

  return true;
}

grnxx::RowIDCursor *create_index_cursor(grnxx::Table *table,
                                        grnxx::String query) {
  // 索引の名前を切り出す．
  auto boundary = query.find_first_of(" \t\n");
  if (boundary == query.npos) {
    boundary = query.size();
  }
  grnxx::String index_name = query.prefix(boundary);
  bool reverse_order = false;
  if (index_name.starts_with("-")) {
    index_name = index_name.except_prefix(1);
    reverse_order = true;
  }
  grnxx::Index *index = table->get_index_by_name(index_name);
  if (!index) {
    std::cerr << "Error: index not found: index_name = "
              << index_name << std::endl;
    return nullptr;
  }
  query = query.except_prefix(boundary);
  boundary = query.find_first_not_of(" \t\n");
  if (boundary == query.npos) {
    boundary = query.size();
  }
  query = query.except_prefix(boundary);

  // 特に指示がなければすべての行を取得できるカーソルを作成する．
  if (query.empty()) {
    return index->find_all(reverse_order);
  }

  // 指定された範囲に対応するカーソルを作成する．
  if (!query.empty() && ((query[0] == '[') || (query[0] == '('))) {
    // 角括弧なら「以上」と「以下」で，丸括弧なら「より大きい」と「より小さい」になる．
    grnxx::UInt8 last_byte = query[query.size() - 1];
    if ((last_byte != ']') && (last_byte != ')')) {
      std::cerr << "Error: closing bracket not found" << std::endl;
      return nullptr;
    }
    bool greater_equal = (query[0] == '[');
    bool less_equal = (last_byte == ']');
    query = query.trim(1, 1);

    // 括弧内両端の空白を取り除く．
    boundary = query.find_first_not_of(" \t\n");
    if (boundary == query.npos) {
      std::cerr << "Error: empty brackets" << std::endl;
      return nullptr;
    }
    query = query.except_prefix(boundary);
    query = query.prefix(query.find_last_not_of(" \t\n") + 1);

    // 一つ目の値を切り出す．
    bool has_begin = false;
    grnxx::Datum begin;
    if (query[0] == '"') {
      boundary = query.find_first_of('"', 1);
      if (boundary == query.npos) {
        std::cerr << "Error: closing double quote not found" << std::endl;
        return nullptr;
      }
      has_begin = true;
      begin = query.extract(1, boundary - 1);
      query = query.except_prefix(boundary + 1);
    } else {
      boundary = query.find_first_of(" \t\n,");
      if (boundary == query.npos) {
        std::cerr << "Error: delimiter not found" << std::endl;
        return nullptr;
      } else if (boundary != 0) {
        has_begin = true;
        begin = query.prefix(boundary);
        query = query.except_prefix(boundary);
      }
    }

    // 空白，区切り文字，空白の順でスキップする．
    boundary = query.find_first_not_of(" \t\n");
    if (boundary == query.npos) {
      std::cerr << "Error: delimiter not found" << std::endl;
      return nullptr;
    }
    query = query.except_prefix(boundary);
    if (query[0] != ',') {
      std::cerr << "Error: delimiter not found" << std::endl;
      return nullptr;
    }
    query = query.except_prefix(1);
    boundary = query.find_first_not_of(" \t\n");
    if (boundary == query.npos) {
      boundary = query.size();
    }
    query = query.except_prefix(boundary);

    // 二つ目の値を切り出す．
    bool has_end = false;
    grnxx::Datum end;
    if (query[0] == '"') {
      boundary = query.find_first_of('"', 1);
      if (boundary == query.npos) {
        std::cerr << "Error: closing double quote not found" << std::endl;
        return nullptr;
      }
      has_end = true;
      end = query.extract(1, boundary - 1);
      query = query.except_prefix(boundary + 1);
    } else {
      boundary = query.find_first_of(" \t\n");
      if (boundary == query.npos) {
        boundary = query.size();
      }
      if (boundary != 0) {
        has_end = true;
        end = query.prefix(boundary);
        query = query.except_prefix(boundary);
      }
    }

    if (!query.empty()) {
      std::cerr << "Error: invalid format" << std::endl;
      return nullptr;
    }

    if (verbose_mode) {
      std::cout << "index_name = " << index_name << '\n'
                << "begin = " << (has_begin ? begin : "N/A") << '\n'
                << "end = " << (has_end ? end : "N/A") << '\n'
                << "greater_equal = " << greater_equal << '\n'
                << "less_equal = " << less_equal << '\n';
    }

    if (has_begin) {
      if (has_end) {
        return index->find_between(begin, end, greater_equal, less_equal,
                                   reverse_order);
      } else {
        return index->find_greater(begin, greater_equal, reverse_order);
      }
    } else if (has_end) {
        return index->find_less(end, less_equal, reverse_order);
    } else {
      return index->find_all(reverse_order);
    }
    return nullptr;
  }

  // 指定された値に対応するカーソルを作成する．
  grnxx::Datum datum;
  if (query[0] == '"') {
    boundary = query.find_first_of('"', 1);
    if (boundary == query.npos) {
      std::cerr << "Error: closing double quote not found" << std::endl;
      return nullptr;
    }
    datum = query.extract(1, boundary - 1);
    query = query.except_prefix(boundary + 1);
  } else {
    datum = query;
  }

  if (verbose_mode) {
    std::cout << "index_name = " << index_name << '\n'
              << "datum = " << datum << '\n';
  }

  return index->find_equal(datum);
}

// select コマンド．
bool run_select(grnxx::Database *database, const grnxx::String &params,
                std::ostream &stream) {
  // 引数を解釈する．
  SelectQuery query;
  if (!parse_select_params(params, &query)) {
    return false;
  }

  // 指定されたテーブルを探す．
  grnxx::Table *table = database->get_table_by_name(query.table_name);
  if (!table) {
    std::cerr << "Error: table not found: table_name = "
              << query.table_name << std::endl;
    return false;
  }

  // 行 ID の一覧を取得するためのカーソルを用意する．
  std::unique_ptr<grnxx::RowIDCursor> cursor;
  if (query.index_query.empty()) {
    // テーブル由来のカーソルを使う．
    cursor.reset(table->create_cursor());
    if (!cursor) {
      std::cerr << "Error: Table::create_cursor failed" << std::endl;
      return false;
    }
  } else {
    // 索引由来のカーソルを使う．
    cursor.reset(create_index_cursor(table, query.index_query));
    if (!cursor) {
      return false;
    }
  }

  // 絞り込みに用いるフィルタを用意する．
  std::unique_ptr<grnxx::Calc> calc(table->create_calc(query.calc_query));
  if (!calc) {
    std::cerr << "Error: Table::create_calc() failed: query = "
              << query.calc_query << std::endl;
    return false;
  }

  // 検索をおこなう．
  constexpr grnxx::Int64 BLOCK_SIZE = 1024;
  grnxx::Int64 num_filtered_rows = 0;
  std::vector<grnxx::RowID> row_ids;
  if (query.column_names_for_sort_by.empty()) {
    // 整列条件がなければ offset, limit を考慮する．
    if (calc->empty()) {
      // 検索条件がなければ，先頭の offset 件をスキップした後，
      // 最大 limit 件を取り出して終了する．
      cursor->get_next(nullptr, query.offset);
      grnxx::Int64 num_rows = 0;
      while (num_rows < query.limit) {
        grnxx::Int64 limit_left = query.limit - num_rows;
        grnxx::Int64 block_size =
            (limit_left < BLOCK_SIZE) ? limit_left : BLOCK_SIZE;
        row_ids.resize(num_rows + block_size);
        grnxx::Int64 num_new_rows =
            cursor->get_next(&row_ids[num_rows], block_size);
        num_rows += num_new_rows;
        if (num_new_rows < block_size) {
          break;
        }
      }
      row_ids.resize(num_rows);
    } else {
      // 検索条件があればブロック単位でフィルタにかける．
      grnxx::Int64 num_rows = 0;
      grnxx::Int64 offset_left = query.offset;
      for ( ; ; ) {
        row_ids.resize(num_rows + BLOCK_SIZE);
        grnxx::Int64 num_new_rows =
            cursor->get_next(&row_ids[num_rows], BLOCK_SIZE);
        if (num_new_rows == 0) {
          break;
        }
        num_filtered_rows += num_new_rows;
        num_new_rows = calc->filter(&row_ids[num_rows], num_new_rows);
        if (offset_left != 0) {
          if (num_new_rows > offset_left) {
            for (grnxx::Int64 i = 0; i < (num_new_rows - offset_left); ++i) {
              row_ids[i] = row_ids[offset_left + i];
            }
            num_new_rows -= offset_left;
            offset_left = 0;
          } else {
            offset_left -= num_new_rows;
            num_new_rows = 0;
          }
        }
        num_rows += num_new_rows;
        if (num_rows >= query.limit) {
          num_rows = query.limit;
          break;
        }
      }
      row_ids.resize(num_rows);
    }
  } else {
    // 整列条件が指定されているときは，後で offset, limit を適用する．
    grnxx::Int64 num_rows = 0;
    for ( ; ; ) {
      row_ids.resize(num_rows + BLOCK_SIZE);
      grnxx::Int64 num_new_rows =
          cursor->get_next(&row_ids[num_rows], BLOCK_SIZE);
      num_rows += num_new_rows;
      if (num_new_rows < BLOCK_SIZE) {
        break;
      }
    }
    row_ids.resize(num_rows);
  }

  if (verbose_mode) {
    std::cout << "num_filtered_rows = " << num_filtered_rows << '\n';
  }

  // 整列をおこなう．
  if (!query.column_names_for_sort_by.empty()) {
    std::unique_ptr<grnxx::Sorter> sorter(
        table->create_sorter(query.column_names_for_sort_by));
    if (!sorter) {
      std::cerr << "Error: Table::create_sorter() failed: query = "
                << query.column_names_for_sort_by << std::endl;
      return false;
    }
    sorter->sort(&*row_ids.begin(), row_ids.size(), query.offset, query.limit);
    if (query.offset >= row_ids.size()) {
      // 先頭の offset 個を取り除くと空になる．
      row_ids.clear();
    } else if (query.offset > 0) {
      // 先頭の offset 個を取り除く．
      grnxx::Int64 num_rows = row_ids.size() - query.offset;
      if (num_rows > query.limit) {
        num_rows = query.limit;
      }
      for (grnxx::Int64 i = 0; i < num_rows; ++i) {
        row_ids[i] = row_ids[query.offset + i];
      }
      row_ids.resize(num_rows);
    } else if (query.limit < row_ids.size()) {
      // 先頭の limit 個までを残す．
      row_ids.resize(query.limit);
    }
  }

  // グループ化をおこなう．
  std::vector<grnxx::Int64> boundaries;
  if (!query.column_names_for_group_by.empty()) {
    if (!table->group_by(&*row_ids.begin(), row_ids.size(),
                         query.column_names_for_group_by, &boundaries)) {
      std::cerr << "Error: Table::group_by() failed: column_names = "
                << query.column_names_for_group_by << std::endl;
      return false;
    }
  }

  // 結果を出力する．
  table->write_to(stream << "result = ", &*row_ids.begin(), row_ids.size(),
                  query.output_column_names) << '\n';
  if (!boundaries.empty()) {
    table->write_to(stream, &*row_ids.begin(), row_ids.size(),
                    boundaries, query.output_column_names) << '\n';
  }
  std::cout << "OK: " << row_ids.size() << " rows\n";

  return false;
}

// 対話モード．
void run_terminal() {
  grnxx::Database database;
  std::string line;
  grnxx::String command;
  grnxx::String params;
  while (std::getline(std::cin, line)) {
    if (extract_command(line, &command, &params)) {
      if (verbose_mode) {
        std::cout << "command = " << command
                  << ", params = " << params << '\n';
      }
      if (command == "table_create") {
        run_table_create(&database, params);
      } else if (command == "table_remove") {
        run_table_remove(&database, params);
      } else if (command == "table_list") {
        run_table_list(&database, params);
      } else if (command == "column_create") {
        run_column_create(&database, params);
      } else if (command == "column_remove") {
        run_column_remove(&database, params);
      } else if (command == "column_list") {
        run_column_list(&database, params);
      } else if (command == "index_create") {
        run_index_create(&database, params);
      } else if (command == "index_remove") {
        run_index_remove(&database, params);
      } else if (command == "index_list") {
        run_index_list(&database, params);
      } else if (command == "load") {
        run_load(&database, params);
      } else if (command == "select") {
        grnxx::Timer timer;
        run_select(&database, params, std::cout);
        std::cerr << "select: " << timer.elapsed()
                  << " [s] elapsed" << std::endl;
      } else if (command == "count") {
        grnxx::Timer timer;
        std::stringstream stream;
        run_select(&database, params, stream);
        std::cerr << "count: " << timer.elapsed()
                  << " [s] elapsed" << std::endl;
      } else if (command == "quit") {
        break;
      } else {
        std::cerr << "Error: unknown command: "
                  << "command= " << command << std::endl;
      }
    }
  }
}

void print_version() {
  std::cout << grnxx::Library::name() << ' '
            << grnxx::Library::version() << "\n\n";

  std::cout << "options:";
  if (grnxx::Library::enable_varint()) {
    std::cout << " varint";
  }
  std::cout << std::endl;
}

void print_usage(int argc, char *argv[]) {
  std::cout << "Usage: " << argv[0] << " [OPTION]...\n\n"
               "Options:\n"
               "  -v, --verbose:  enable verbose mode\n"
               "  -h, --help:     print this help\n"
               "  -V, --version:  print grnxx version\n"
            << std::flush;
}

}  // namespace

int main(int argc, char *argv[]) {
  const struct option long_options[] = {
    { "verbose", 0, nullptr, 'v' },
    { "help", 0, nullptr, 'h' },
    { "version", 0, nullptr, 'V' },
    { nullptr, 0, nullptr, 0 }
  };
  int value;
  while ((value = ::getopt_long(argc, argv, "vhV",
                                long_options, nullptr)) != -1) {
    switch (value) {
      case 'v': {
        verbose_mode = true;
        break;
      }
      case 'h': {
        print_usage(argc, argv);
        return 0;
      }
      case 'V': {
        print_version();
        return 0;
      }
      default: {
        print_usage(argc, argv);
        return 1;
      }
    }
  }
  run_terminal();
  return 0;
}
