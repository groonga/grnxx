#include "grnxx.h"

#include <grnxx/db.hpp>
#include <grnxx/expression.hpp>
#include <grnxx/library.hpp>
#include <grnxx/pipeline.hpp>

extern "C" {

// -- Library --

const char *grnxx_package() {
  return grnxx::Library::package();
}

const char *grnxx_version() {
  return grnxx::Library::version();
}

// -- Component --

struct grnxx_db {
  grnxx::DB *db() {
    return reinterpret_cast<grnxx::DB *>(this);
  }
};

struct grnxx_table {
  grnxx::Table *table() {
    return reinterpret_cast<grnxx::Table *>(this);
  }
};

struct grnxx_column {
  grnxx::Column *column() {
    return reinterpret_cast<grnxx::Column *>(this);
  }
};

struct grnxx_index {
  grnxx::Index *index() {
    return reinterpret_cast<grnxx::Index *>(this);
  }
};

struct grnxx_cursor {
  grnxx::Cursor *cursor() {
    return reinterpret_cast<grnxx::Cursor *>(this);
  }
};

struct grnxx_expression {
  grnxx::Expression *expression() {
    return reinterpret_cast<grnxx::Expression *>(this);
  }
};

struct grnxx_expression_builder {
  grnxx::ExpressionBuilder *builder() {
    return reinterpret_cast<grnxx::ExpressionBuilder *>(this);
  }
};

struct grnxx_sorter {
  grnxx::Sorter *sorter() {
    return reinterpret_cast<grnxx::Sorter *>(this);
  }
};

struct grnxx_merger {
  grnxx::Merger *merger() {
    return reinterpret_cast<grnxx::Merger *>(this);
  }
};

struct grnxx_pipeline {
  grnxx::Pipeline *pipeline() {
    return reinterpret_cast<grnxx::Pipeline *>(this);
  }
};

struct grnxx_pipeline_builder {
  grnxx::PipelineBuilder *builder() {
    return reinterpret_cast<grnxx::PipelineBuilder *>(this);
  }
};

// -- Utility --

static grnxx::Datum grnxx_value_to_datum(grnxx::DataType data_type,
                                         const void *value) {
  if (!value) {
    return grnxx::Datum(grnxx::NA());
  }
  switch (data_type) {
    case GRNXX_BOOL: {
      const grnxx_bool *x = static_cast<const grnxx_bool *>(value);
      return grnxx::Datum((*x == GRNXX_BOOL_NA) ? grnxx::Bool::na() :
                          grnxx::Bool(*x != GRNXX_BOOL_FALSE));
    }
    case GRNXX_INT: {
      return grnxx::Datum(grnxx::Int(*static_cast<const int64_t *>(value)));
    }
    case GRNXX_FLOAT: {
      return grnxx::Datum(grnxx::Float(*static_cast<const double *>(value)));
    }
    case GRNXX_GEO_POINT: {
      const grnxx_geo_point *geo_point =
          static_cast<const grnxx_geo_point *>(value);
      return grnxx::GeoPoint(grnxx::Int(geo_point->latitude),
                             grnxx::Int(geo_point->longitude));
    }
    case GRNXX_TEXT: {
      const grnxx_text *x = static_cast<const grnxx_text *>(value);
      return grnxx::Text(x->data, x->size);
    }
  }
  return grnxx::Datum(grnxx::NA());
}

// -- DB --

grnxx_db *grnxx_db_create() try {
  auto db = grnxx::open_db("");
  return reinterpret_cast<grnxx_db *>(db.release());
} catch (...) {
  return nullptr;
}

void grnxx_db_close(grnxx_db *db) {
  delete db->db();
}

size_t grnxx_db_num_tables(grnxx_db *db) {
  return db->db()->num_tables();
}

grnxx_table *grnxx_db_create_table(grnxx_db *db, const char *name) try {
  return reinterpret_cast<grnxx_table *>(db->db()->create_table(name));
} catch (...) {
  return nullptr;
}

bool grnxx_db_remove_table(grnxx_db *db, const char *name) try {
  db->db()->remove_table(name);
  return true;
} catch (...) {
  return false;
}

bool grnxx_db_rename_table(grnxx_db *db,
                           const char *name,
                           const char *new_name) try {
  db->db()->rename_table(name, new_name);
  return true;
} catch (...) {
  return false;
}

bool grnxx_db_reorder_table(grnxx_db *db,
                            const char *name,
                            const char *prev_name) try {
  db->db()->reorder_table(name, prev_name);
  return true;
} catch (...) {
  return false;
}

grnxx_table *grnxx_db_get_table(grnxx_db *db, size_t table_id) {
  return reinterpret_cast<grnxx_table *>(db->db()->get_table(table_id));
}

grnxx_table *grnxx_db_find_table(grnxx_db *db, const char *name) {
  return reinterpret_cast<grnxx_table *>(db->db()->find_table(name));
}

// -- Table --

grnxx_db *grnxx_table_db(grnxx_table *table) {
  return reinterpret_cast<grnxx_db *>(table->table()->db());
}

const char *grnxx_table_name(grnxx_table *table, size_t *size) {
  *size = table->table()->name().size();
  return table->table()->name().data();
}

size_t grnxx_table_num_columns(grnxx_table *table) {
  return table->table()->num_columns();
}

grnxx_column *grnxx_table_key_column(grnxx_table *table) {
  return reinterpret_cast<grnxx_column *>(table->table()->key_column());
}

size_t grnxx_table_num_rows(grnxx_table *table) {
  return table->table()->num_rows();
}

int64_t grnxx_table_max_row_id(grnxx_table *table) {
  return table->table()->max_row_id().raw();
}

bool grnxx_table_is_empty(grnxx_table *table) {
  return table->table()->is_empty();
}

bool grnxx_table_is_full(grnxx_table *table) {
  return table->table()->is_full();
}

grnxx_column *grnxx_table_create_column(
    grnxx_table *table,
    const char *name,
    grnxx_data_type data_type,
    const grnxx_column_options *options) try {
  grnxx::ColumnOptions internal_options;
  if (options) {
    internal_options.reference_table_name = options->reference_table_name;
  }
  return reinterpret_cast<grnxx_column *>(
      table->table()->create_column(name, data_type, internal_options));
} catch (...) {
  return nullptr;
}

bool grnxx_table_remove_column(grnxx_table *table, const char *name) try {
  table->table()->remove_column(name);
  return true;
} catch (...) {
  return false;
}

bool grnxx_table_rename_column(grnxx_table *table,
                               const char *name,
                               const char *new_name) try {
  table->table()->rename_column(name, new_name);
  return true;
} catch (...) {
  return false;
}

bool grnxx_table_reorder_column(grnxx_table *table,
                                const char *name,
                                const char *prev_name) try {
  table->table()->reorder_column(name, prev_name);
  return true;
} catch (...) {
  return false;
}

grnxx_column *grnxx_table_get_column(grnxx_table *table, size_t column_id) {
  return reinterpret_cast<grnxx_column *>(
      table->table()->get_column(column_id));
}

grnxx_column *grnxx_table_find_column(grnxx_table *table, const char *name) {
  return reinterpret_cast<grnxx_column *>(table->table()->find_column(name));
}

bool grnxx_table_set_key_column(grnxx_table *table, const char *name) try {
  table->table()->set_key_column(name);
  return true;
} catch (...) {
  return false;
}

bool grnxx_table_unset_key_column(grnxx_table *table) try {
  table->table()->unset_key_column();
  return true;
} catch (...) {
  return false;
}

int64_t grnxx_table_insert_row(grnxx_table *table, const void *key) try {
  auto key_column = table->table()->key_column();
  if (key_column) {
    table->table()->insert_row(
        grnxx_value_to_datum(key_column->data_type(), key));
  } else {
    return table->table()->insert_row().raw();
  }
} catch (...) {
  return grnxx::Int::raw_na();
}

size_t grnxx_table_insert_rows(grnxx_table *table,
                               size_t num_keys,
                               const void *keys,
                               int64_t *row_ids) {
  size_t count = 0;
  auto key_column = table->table()->key_column();
  if (key_column) {
    grnxx::Datum key;
    while (count < num_keys) try {
      switch (key_column->data_type()) {
        case GRNXX_BOOL: {
          auto value = static_cast<const grnxx_bool *>(keys)[count];
          key = (value == GRNXX_BOOL_NA) ? grnxx::Bool(grnxx::NA()) :
                grnxx::Bool(value == GRNXX_BOOL_TRUE);
          break;
        }
        case GRNXX_INT: {
          auto value = static_cast<const int64_t *>(keys)[count];
          key = grnxx::Int(value);
          break;
        }
        case GRNXX_FLOAT: {
          auto value = static_cast<const double *>(keys)[count];
          key = grnxx::Float(value);
          break;
        }
        case GRNXX_GEO_POINT: {
          auto value = static_cast<const grnxx_geo_point *>(keys)[count];
          key = grnxx::GeoPoint(grnxx::Int(value.latitude),
                                grnxx::Int(value.longitude));
          break;
        }
        case GRNXX_TEXT: {
          auto value = static_cast<const grnxx_text *>(keys)[count];
          key = grnxx::Text(value.data, value.size);
          break;
        }
      }
      row_ids[count] = table->table()->insert_row(key).raw();
      ++count;
    } catch (...) {
      return count;
    }
  } else {
    while (count < num_keys) try {
      row_ids[count] = table->table()->insert_row().raw();
      ++count;
    } catch (...) {
      return count;
    }
  }
  return count;
}

bool grnxx_table_insert_row_at(grnxx_table *table,
                               int64_t row_id,
                               const void *key) try {
  auto key_column = table->table()->key_column();
  if (key_column) {
    table->table()->insert_row_at(
        grnxx::Int(row_id),
        grnxx_value_to_datum(key_column->data_type(), key));
  } else {
    table->table()->insert_row_at(grnxx::Int(row_id));
  }
  return true;
} catch (...) {
  return false;
}

int64_t grnxx_table_find_or_insert_row(grnxx_table *table,
                                       const void *key,
                                       bool *inserted) try {
  auto key_column = table->table()->key_column();
  return table->table()->find_or_insert_row(
      grnxx_value_to_datum(key_column->data_type(), key), inserted).raw();
} catch (...) {
  return grnxx::Int::raw_na();
}

bool grnxx_table_remove_row(grnxx_table *table, int64_t row_id) try {
  table->table()->remove_row(grnxx::Int(row_id));
  return true;
} catch (...) {
  return false;
}

bool grnxx_table_test_row(grnxx_table *table, int64_t row_id) try {
  return table->table()->test_row(grnxx::Int(row_id));
} catch (...) {
  return false;
}

int64_t grnxx_table_find_row(grnxx_table *table, const void *key) try {
  auto key_column = table->table()->key_column();
  if (!key_column) {
    return grnxx::Int::raw_na();
  }
  return table->table()->find_row(
      grnxx_value_to_datum(key_column->data_type(), key)).raw();
} catch (...) {
  return false;
}

grnxx_cursor *grnxx_table_create_cursor(grnxx_table *table,
                                        const grnxx_cursor_options *options) try {
  auto cursor = table->table()->create_cursor(
      *reinterpret_cast<const grnxx::CursorOptions *>(options));
  return reinterpret_cast<grnxx_cursor *>(cursor.release());
} catch (...) {
  return nullptr;
}

// -- Column --

grnxx_table *grnxx_column_table(grnxx_column *column) {
  return reinterpret_cast<grnxx_table *>(column->column()->table());
}

const char *grnxx_column_name(grnxx_column *column, size_t *size) {
  *size = column->column()->name().size();
  return column->column()->name().data();
}

grnxx_data_type grnxx_column_data_type(grnxx_column *column) {
  return column->column()->data_type();
}

grnxx_table *grnxx_column_reference_table(grnxx_column *column) {
  return reinterpret_cast<grnxx_table *>(column->column()->reference_table());
}

bool grnxx_column_is_key(grnxx_column *column) {
  return column->column()->is_key();
}

size_t grnxx_column_num_indexes(grnxx_column *column) {
  return column->column()->num_indexes();
}

grnxx_index *grnxx_column_create_index(grnxx_column *column,
                                       const char *name,
                                       grnxx_index_type index_type) try {
  switch (index_type) {
    case GRNXX_TREE_INDEX: {
      return reinterpret_cast<grnxx_index *>(
        column->column()->create_index(name, GRNXX_TREE_INDEX));
    }
    case GRNXX_HASH_INDEX: {
      return reinterpret_cast<grnxx_index *>(
        column->column()->create_index(name, GRNXX_HASH_INDEX));
    }
    default: {
      return nullptr;
    }
  }
} catch (...) {
  return nullptr;
}

bool grnxx_column_remove_index(grnxx_column *column, const char *name) try {
  column->column()->remove_index(name);
  return true;
} catch (...) {
  return false;
}

bool grnxx_column_rename_index(grnxx_column *column,
                               const char *name,
                               const char *new_name) try {
  column->column()->rename_index(name, new_name);
  return true;
} catch (...) {
  return false;
}

bool grnxx_column_reorder_index(grnxx_column *column,
                                const char *name,
                                const char *prev_name) try {
  column->column()->reorder_index(name, prev_name);
  return true;
} catch (...) {
  return false;
}

grnxx_index *grnxx_column_get_index(grnxx_column *column, size_t index_id) {
  return reinterpret_cast<grnxx_index *>(
      column->column()->get_index(index_id));
}

grnxx_index *grnxx_column_find_index(grnxx_column *column, const char *name) {
  return reinterpret_cast<grnxx_index *>(column->column()->find_index(name));
}

bool grnxx_column_set(grnxx_column *column,
                      int64_t row_id,
                      const void *value) try {
  if (!value) {
    column->column()->set(grnxx::Int(row_id), grnxx::NA());
    return true;
  }
  column->column()->set(
      grnxx::Int(row_id),
      grnxx_value_to_datum(column->column()->data_type(), value));
  return true;
} catch (...) {
  return false;
}

bool grnxx_column_get(grnxx_column *column, int64_t row_id, void *value) try {
  grnxx::Datum datum;
  column->column()->get(grnxx::Int(row_id), &datum);
  switch (column->column()->data_type()) {
    case GRNXX_BOOL: {
      *static_cast<grnxx_bool *>(value) = datum.force_bool().raw();
      break;
    }
    case GRNXX_INT: {
      *static_cast<int64_t *>(value) = datum.force_int().raw();
      break;
    }
    case GRNXX_FLOAT: {
      *static_cast<double *>(value) = datum.force_float().raw();
      break;
    }
    case GRNXX_GEO_POINT: {
      grnxx_geo_point *geo_point = static_cast<grnxx_geo_point *>(value);
      geo_point->latitude = datum.force_geo_point().raw_latitude();
      geo_point->longitude = datum.force_geo_point().raw_longitude();
      break;
    }
    case GRNXX_TEXT: {
      grnxx_text *text = static_cast<grnxx_text *>(value);
      const grnxx::Text &stored_text = datum.force_text();
      if (stored_text.is_na()) {
        text->data = nullptr;
        text->size = grnxx::Text::raw_na_size();
      } else {
        text->data = datum.force_text().raw_data();
        text->size = datum.force_text().raw_size();
      }
      break;
    }
    default: {
      return false;
    }
  }
  return true;
} catch (...) {
  return false;
}

bool grnxx_column_contains(grnxx_column *column, const void *value) try {
  if (!value) {
    return column->column()->contains(grnxx::NA());
  }
  return column->column()->contains(
      grnxx_value_to_datum(column->column()->data_type(), value));
} catch (...) {
  return false;
}

int64_t grnxx_column_find_one(grnxx_column *column, const void *value) try {
  if (!value) {
    return column->column()->find_one(grnxx::NA()).raw();
  }
  return column->column()->find_one(
      grnxx_value_to_datum(column->column()->data_type(), value)).raw();
} catch (...) {
  return grnxx::Int::raw_na();
}

// -- Index --

grnxx_column *grnxx_index_column(grnxx_index *index) {
  return reinterpret_cast<grnxx_column *>(index->index()->column());
}

const char *grnxx_index_name(grnxx_index *index, size_t *size) {
  *size = index->index()->name().size();
  return index->index()->name().data();
}

grnxx_index_type grnxx_index_index_type(grnxx_index *index) {
  switch (index->index()->type()) {
    case GRNXX_TREE_INDEX: {
      return GRNXX_TREE_INDEX;
    }
    case GRNXX_HASH_INDEX: {
      return GRNXX_HASH_INDEX;
    }
  }
}

size_t grnxx_index_num_entries(grnxx_index *index) {
  return index->index()->num_entries();
}

bool grnxx_index_test_uniqueness(grnxx_index *index) try {
  return index->index()->test_uniqueness();
} catch (...) {
  return false;
}

bool grnxx_index_contains(grnxx_index *index, const void *value) try {
  auto column = index->index()->column();
  if (!value) {
    return column->contains(grnxx::NA());
  }
  return index->index()->contains(
      grnxx_value_to_datum(column->data_type(), value));
} catch (...) {
  return false;
}

int64_t grnxx_index_find_one(grnxx_index *index, const void *value) try {
  auto column = index->index()->column();
  if (!value) {
    return column->find_one(grnxx::NA()).raw();
  }
  return index->index()->find_one(
      grnxx_value_to_datum(column->data_type(), value)).raw();
} catch (...) {
  return grnxx::Int::raw_na();
}

grnxx_cursor *grnxx_index_find(grnxx_index *index,
                               const void *value,
                               const grnxx_cursor_options *options) try {
  const grnxx::CursorOptions *cursor_options =
      reinterpret_cast<const grnxx::CursorOptions *>(options);
  return reinterpret_cast<grnxx_cursor *>(index->index()->find(
      grnxx_value_to_datum(index->index()->column()->data_type(), value),
      *cursor_options).release());
} catch (...) {
  return nullptr;
}

grnxx_cursor *grnxx_index_find_in_range(grnxx_index *index,
                                        const void *lower_bound_value,
                                        bool lower_bound_is_inclusive,
                                        const void *upper_bound_value,
                                        bool upper_bound_is_inclusive,
                                        const grnxx_cursor_options *options) try {
  const grnxx::CursorOptions *cursor_options =
      reinterpret_cast<const grnxx::CursorOptions *>(options);
  grnxx::DataType data_type = index->index()->column()->data_type();
  grnxx::IndexRange range;
  range.set_lower_bound(
      grnxx_value_to_datum(data_type, lower_bound_value),
      lower_bound_is_inclusive ? grnxx::INCLUSIVE_END_POINT :
                                 grnxx::EXCLUSIVE_END_POINT);
  range.set_upper_bound(
      grnxx_value_to_datum(data_type, upper_bound_value),
      upper_bound_is_inclusive ? grnxx::INCLUSIVE_END_POINT :
                                 grnxx::EXCLUSIVE_END_POINT);
  return reinterpret_cast<grnxx_cursor *>(
      index->index()->find_in_range(range, *cursor_options).release());
} catch (...) {
  return nullptr;
}

grnxx_cursor *grnxx_index_find_starts_with(grnxx_index *index,
                                           const void *prefix,
                                           bool prefix_is_inclusive,
                                           const grnxx_cursor_options *options) try {
  const grnxx::CursorOptions *cursor_options =
      reinterpret_cast<const grnxx::CursorOptions *>(options);
  grnxx::EndPoint end_point;
  if (prefix_is_inclusive) {
    end_point.type = grnxx::INCLUSIVE_END_POINT;
  } else {
    end_point.type = grnxx::EXCLUSIVE_END_POINT;
  }
  end_point.value =
      grnxx_value_to_datum(index->index()->column()->data_type(), prefix);
  return reinterpret_cast<grnxx_cursor *>(
      index->index()->find_starts_with(end_point, *cursor_options).release());
} catch (...) {
  return nullptr;
}

grnxx_cursor *grnxx_index_find_prefixes(grnxx_index *index,
                                        const void *value,
                                        const grnxx_cursor_options *options) try {
  const grnxx::CursorOptions *cursor_options =
      reinterpret_cast<const grnxx::CursorOptions *>(options);
  return reinterpret_cast<grnxx_cursor *>(index->index()->find_prefixes(
      grnxx_value_to_datum(index->index()->column()->data_type(), value),
      *cursor_options).release());
} catch (...) {
  return nullptr;
}

// -- Cursor --

void grnxx_cursor_close(grnxx_cursor *cursor) {
  delete cursor->cursor();
}

size_t grnxx_cursor_read(grnxx_cursor *cursor,
                         grnxx_record *records,
                         size_t size) try {
  return cursor->cursor()->read(grnxx::ArrayRef<grnxx::Record>(
      reinterpret_cast<grnxx::Record *>(records), size));
} catch (...) {
  return 0;
}

// -- Expression --

grnxx_expression *grnxx_expression_parse(grnxx_table *table,
                                         const char *query) try {
  return reinterpret_cast<grnxx_expression *>(
      grnxx::Expression::parse(table->table(), query).release());
} catch (...) {
  return nullptr;
}

void grnxx_expression_close(grnxx_expression *expression) {
  delete expression->expression();
}

grnxx_table *grnxx_expression_table(grnxx_expression *expression) {
  return reinterpret_cast<grnxx_table *>(
      const_cast<grnxx::Table *>(expression->expression()->table()));
}

grnxx_data_type grnxx_expression_data_type(grnxx_expression *expression) {
  return expression->expression()->data_type();
}

bool grnxx_expression_is_row_id(grnxx_expression *expression) {
  return expression->expression()->is_row_id();
}

bool grnxx_expression_is_score(grnxx_expression *expression) {
  return expression->expression()->is_score();
}

size_t grnxx_expression_block_size(grnxx_expression *expression) {
  return expression->expression()->block_size();
}

bool grnxx_expression_filter(grnxx_expression *expression,
                             grnxx_record *records,
                             size_t *size,
                             size_t offset,
                             size_t limit) try {
  grnxx::ArrayRef<grnxx::Record> array(
      reinterpret_cast<grnxx::Record *>(records), *size);
  expression->expression()->filter(array, &array, offset, limit);
  *size = array.size();
  return true;
} catch (...) {
  return false;
}

bool grnxx_expression_adjust(grnxx_expression *expression,
                             grnxx_record *records,
                             size_t size) try {
  grnxx::ArrayRef<grnxx::Record> array(
      reinterpret_cast<grnxx::Record *>(records), size);
  expression->expression()->adjust(array);
  return true;
} catch (...) {
  return false;
}

bool grnxx_expression_evaluate(grnxx_expression *expression,
                               const grnxx_record *records,
                               size_t size,
                               void *values) try {
  grnxx::ArrayCRef<grnxx::Record> internal_records(
      reinterpret_cast<const grnxx::Record *>(records), size);
  switch (expression->expression()->data_type()) {
    case GRNXX_BOOL: {
      grnxx::ArrayRef<grnxx::Bool> internal_values(
          reinterpret_cast<grnxx::Bool *>(values), size);
      expression->expression()->evaluate(internal_records, internal_values);
      break;
    }
    case GRNXX_INT: {
      grnxx::ArrayRef<grnxx::Int> internal_values(
          reinterpret_cast<grnxx::Int *>(values), size);
      expression->expression()->evaluate(internal_records, internal_values);
      break;
    }
    case GRNXX_FLOAT: {
      grnxx::ArrayRef<grnxx::Float> internal_values(
          reinterpret_cast<grnxx::Float *>(values), size);
      expression->expression()->evaluate(internal_records, internal_values);
      break;
    }
    case GRNXX_GEO_POINT: {
      grnxx::ArrayRef<grnxx::GeoPoint> internal_values(
          reinterpret_cast<grnxx::GeoPoint *>(values), size);
      expression->expression()->evaluate(internal_records, internal_values);
      break;
    }
    case GRNXX_TEXT: {
      grnxx::ArrayRef<grnxx::Text> internal_values(
          reinterpret_cast<grnxx::Text *>(values), size);
      expression->expression()->evaluate(internal_records, internal_values);
      break;
    }
    default: {
      return false;
    }
  }
  return true;
} catch (...) {
  return false;
}

// -- ExpressionBuilder --

grnxx_expression_builder *grnxx_expression_builder_create(grnxx_table *table) try {
  return reinterpret_cast<grnxx_expression_builder *>(
      grnxx::ExpressionBuilder::create(table->table()).release());
} catch (...) {
  return nullptr;
}

void grnxx_expression_builder_close(grnxx_expression_builder *builder) {
  delete builder->builder();
}

grnxx_table *grnxx_expression_builder_table(grnxx_expression_builder *builder) {
  return reinterpret_cast<grnxx_table *>(
      const_cast<grnxx::Table *>(builder->builder()->table()));
}

bool grnxx_expression_builder_push_constant(grnxx_expression_builder *builder,
                                            grnxx_data_type data_type,
                                            const void *value) try {
  builder->builder()->push_constant(grnxx_value_to_datum(data_type, value));
  return true;
} catch (...) {
  return false;
}

bool grnxx_expression_builder_push_row_id(grnxx_expression_builder *builder) try {
  builder->builder()->push_row_id();
  return true;
} catch (...) {
  return false;
}

bool grnxx_expression_builder_push_score(grnxx_expression_builder *builder) try {
  builder->builder()->push_score();
  return true;
} catch (...) {
  return false;
}

bool grnxx_expression_builder_push_column(grnxx_expression_builder *builder,
                                          const char *column_name) try {
  builder->builder()->push_column(column_name);
  return true;
} catch (...) {
  return false;
}

bool grnxx_expression_builder_push_operator(
    grnxx_expression_builder *builder,
    grnxx_operator_type operator_type) try {
  builder->builder()->push_operator(operator_type);
  return true;
} catch (...) {
  return false;
}

bool grnxx_expression_builder_begin_subexpression(
    grnxx_expression_builder *builder) try {
  builder->builder()->begin_subexpression();
  return true;
} catch (...) {
  return false;
}

bool grnxx_expression_builder_end_subexpression(
    grnxx_expression_builder *builder) try {
  builder->builder()->end_subexpression();
  return true;
} catch (...) {
  return false;
}

void grnxx_expression_builder_clear(grnxx_expression_builder *builder) {
  builder->builder()->clear();
}

grnxx_expression *grnxx_expression_builder_release(
    grnxx_expression_builder *builder) try {
  return reinterpret_cast<grnxx_expression *>(
      builder->builder()->release().release());
} catch (...) {
  return nullptr;
}

// -- Sorter --

grnxx_sorter *grnxx_sorter_create(grnxx_sorter_order *orders,
                                  size_t num_orders,
                                  const grnxx_sorter_options *options) try {
  grnxx::Array<grnxx::SorterOrder> internal_orders;
  internal_orders.resize(num_orders);
  for (size_t i = 0; i < num_orders; ++i) {
    internal_orders[i].expression.reset(orders[i].expression->expression());
    internal_orders[i].type = orders[i].order_type;
  }
  grnxx::SorterOptions internal_options;
  if (options) {
    internal_options.offset = options->offset;
    internal_options.limit = options->limit;
  }
  auto sorter = grnxx::Sorter::create(
      std::move(internal_orders), internal_options);
  return reinterpret_cast<grnxx_sorter *>(sorter.release());
} catch (...) {
  return nullptr;
}

void grnxx_sorter_close(grnxx_sorter *sorter) {
  delete sorter->sorter();
}

// -- Merger --

static grnxx::MergerOptions grnxx_merger_convert_options(
    const grnxx_merger_options *options) {
  grnxx::MergerOptions internal_options;
  if (options) {
    internal_options.logical_operator_type = options->logical_operator_type;
    internal_options.score_operator_type = options->score_operator_type;
    internal_options.missing_score = grnxx::Float(options->missing_score);
    internal_options.offset = options->offset;
    internal_options.limit = options->limit;
  }
  return internal_options;
}

grnxx_merger *grnxx_merger_create(const grnxx_merger_options *options) try {
  auto internal_options = grnxx_merger_convert_options(options);
  auto merger = grnxx::Merger::create(std::move(internal_options));
  return reinterpret_cast<grnxx_merger *>(merger.release());
} catch (...) {
  return nullptr;
}

void grnxx_merger_close(grnxx_merger *merger) {
  delete merger->merger();
}

// -- Pipeline --

void grnxx_pipeline_close(grnxx_pipeline *pipeline) {
  delete pipeline->pipeline();
}

grnxx_table *grnxx_pipeline_table(grnxx_pipeline *pipeline) {
  return reinterpret_cast<grnxx_table *>(
      const_cast<grnxx::Table *>(pipeline->pipeline()->table()));
}

bool grnxx_pipeline_flush(grnxx_pipeline *pipeline,
                          grnxx_record **records,
                          size_t *size) try {
  grnxx::Array<grnxx::Record> internal_records;
  pipeline->pipeline()->flush(&internal_records);
  // TODO: Deep copy should be skipped.
  *records = static_cast<grnxx_record *>(
    std::malloc(sizeof(grnxx_record) * internal_records.size()));
  if (!*records) {
    return false;
  }
  for (size_t i = 0; i < internal_records.size(); ++i) {
    (*records)[i].row_id = internal_records[i].row_id.raw();
    (*records)[i].score = internal_records[i].score.raw();
  }
  *size = internal_records.size();
  return true;
} catch (...) {
  return false;
}

// -- PipelineBuilder --

grnxx_pipeline_builder *grnxx_pipeline_builder_create(grnxx_table *table) try {
  auto builder = grnxx::PipelineBuilder::create(table->table());
  return reinterpret_cast<grnxx_pipeline_builder *>(builder.release());
} catch (...) {
  return nullptr;
}

void grnxx_pipeline_builder_close(grnxx_pipeline_builder *builder) {
  delete builder->builder();
}

grnxx_table *grnxx_pipeline_builder_table(grnxx_pipeline_builder *builder) {
  return reinterpret_cast<grnxx_table *>(
      const_cast<grnxx::Table *>(builder->builder()->table()));
}

bool grnxx_pipeline_builder_push_cursor(grnxx_pipeline_builder *builder,
                                        grnxx_cursor *cursor) try {
  builder->builder()->push_cursor(
      std::unique_ptr<grnxx::Cursor>(cursor->cursor()));
  return true;
} catch (...) {
  return false;
}

bool grnxx_pipeline_builder_push_filter(grnxx_pipeline_builder *builder,
                                        grnxx_expression *expression,
                                        size_t offset,
                                        size_t limit) try {
  builder->builder()->push_filter(
      std::unique_ptr<grnxx::Expression>(expression->expression()),
      offset, limit);
  return true;
} catch (...) {
  return false;
}

bool grnxx_pipeline_builder_push_adjuster(grnxx_pipeline_builder *builder,
                                          grnxx_expression *expression) try {
  builder->builder()->push_adjuster(
      std::unique_ptr<grnxx::Expression>(expression->expression()));
  return true;
} catch (...) {
  return false;
}

bool grnxx_pipeline_builder_push_sorter(grnxx_pipeline_builder *builder,
                                        grnxx_sorter *sorter) try {
  builder->builder()->push_sorter(
      std::unique_ptr<grnxx::Sorter>(sorter->sorter()));
  return true;
} catch (...) {
  return false;
}

bool grnxx_pipeline_builder_push_merger(grnxx_pipeline_builder *builder,
                                        const grnxx_merger_options *options) try {
  auto internal_options = grnxx_merger_convert_options(options);
  builder->builder()->push_merger(internal_options);
  return true;
} catch (...) {
  return false;
}


void grnxx_pipeline_builder_clear(grnxx_pipeline_builder *builder) {
  builder->builder()->clear();
}

grnxx_pipeline *grnxx_pipeline_builder_release(
    grnxx_pipeline_builder *builder,
    const grnxx_pipeline_options *options) try {
  grnxx::PipelineOptions internal_options;
  if (options) {
    // Nothing to do.
  }
  auto pipeline = builder->builder()->release(internal_options);
  return reinterpret_cast<grnxx_pipeline *>(pipeline.release());
} catch (...) {
  return nullptr;
}

}  // extern "C"
