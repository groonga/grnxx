#ifndef GRNXX_H
#define GRNXX_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <grnxx/constants.h>

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

const char *grnxx_package(void);
const char *grnxx_version(void);

typedef struct grnxx_db grnxx_db;
typedef struct grnxx_table grnxx_table;
typedef struct grnxx_column grnxx_column;
typedef struct grnxx_index grnxx_index;
typedef struct grnxx_cursor grnxx_cursor;
typedef struct grnxx_expression grnxx_expression;
typedef struct grnxx_expression_builder grnxx_expression_builder;
typedef struct grnxx_sorter grnxx_sorter;
typedef struct grnxx_merger grnxx_merger;
typedef struct grnxx_pipeline grnxx_pipeline;
typedef struct grnxx_pipeline_builder grnxx_pipeline_builder;

typedef uint8_t grnxx_bool;

enum {
  GRNXX_BOOL_NA    = 1,
  GRNXX_BOOL_TRUE  = 3,
  GRNXX_BOOL_FALSE = 0
};

typedef struct {
  int32_t latitude;
  int32_t longitude;
} grnxx_geo_point;

typedef struct {
  const char *data;
  int64_t size;
} grnxx_text;

typedef struct {
  int64_t row_id;
  double score;
} grnxx_record;

typedef struct {
  const char *reference_table_name;
} grnxx_column_options;

// TODO: Settings should be done by using functions?
typedef struct {
  size_t offset;
  size_t limit;
  grnxx_order_type order_type;
} grnxx_cursor_options;

typedef struct {
  grnxx_expression *expression;
  grnxx_order_type order_type;
} grnxx_sorter_order;

// TODO: Settings should be done by using functions?
typedef struct {
  size_t offset;
  size_t limit;
} grnxx_sorter_options;

// TODO: Settings should be done by using functions?
typedef struct {
  grnxx_merger_operator_type logical_operator_type;
  grnxx_merger_operator_type score_operator_type;
  double missing_score;
  size_t offset;
  size_t limit;
} grnxx_merger_options;

typedef struct {
} grnxx_pipeline_options;

// -- DB --

grnxx_db *grnxx_db_create(void);
void grnxx_db_close(grnxx_db *db);

size_t grnxx_db_num_tables(grnxx_db *db);

grnxx_table *grnxx_db_create_table(grnxx_db *db, const char *name);
bool grnxx_db_remove_table(grnxx_db *db, const char *name);
bool grnxx_db_rename_table(grnxx_db *db,
                           const char *name,
                           const char *new_name);
bool grnxx_db_reorder_table(grnxx_db *db,
                            const char *name,
                            const char *prev_name);
grnxx_table *grnxx_db_get_table(grnxx_db *db, size_t table_id);
grnxx_table *grnxx_db_find_table(grnxx_db *db, const char *name);

// -- Table --

grnxx_db *grnxx_table_db(grnxx_table *table);
const char *grnxx_table_name(grnxx_table *table, size_t *size);
size_t grnxx_table_num_columns(grnxx_table *table);
grnxx_column *grnxx_table_key_column(grnxx_table *table);
size_t grnxx_table_num_rows(grnxx_table *table);
int64_t grnxx_table_max_row_id(grnxx_table *table);
bool grnxx_table_is_empty(grnxx_table *table);
bool grnxx_table_is_full(grnxx_table *table);

// TODO: Support references.
grnxx_column *grnxx_table_create_column(grnxx_table *table,
                                        const char *name,
                                        grnxx_data_type data_type,
                                        const grnxx_column_options *options);
bool grnxx_table_remove_column(grnxx_table *table, const char *name);
bool grnxx_table_rename_column(grnxx_table *table,
                               const char *name,
                               const char *new_name);
bool grnxx_table_reorder_column(grnxx_table *table,
                                const char *name,
                                const char *prev_name);
grnxx_column *grnxx_table_get_column(grnxx_table *table, size_t column_id);
grnxx_column *grnxx_table_find_column(grnxx_table *table, const char *name);

bool grnxx_table_set_key_column(grnxx_table *table, const char *name);
bool grnxx_table_unset_key_column(grnxx_table *table);

int64_t grnxx_table_insert_row(grnxx_table *table, const void *key);
size_t grnxx_table_insert_rows(grnxx_table *table,
                               size_t num_keys,
                               const void *keys,
                               int64_t *row_ids);
bool grnxx_table_insert_row_at(grnxx_table *table,
                               int64_t row_id,
                               const void *key);
int64_t grnxx_table_find_or_insert_row(grnxx_table *table,
                                       const void *key,
                                       bool *inserted);
bool grnxx_table_remove_row(grnxx_table *table, int64_t row_id);
bool grnxx_table_test_row(grnxx_table *table, int64_t row_id);
int64_t grnxx_table_find_row(grnxx_table *table, const void *key);

grnxx_cursor *grnxx_table_create_cursor(grnxx_table *table,
                                        const grnxx_cursor_options *options);

// -- Column --

grnxx_table *grnxx_column_table(grnxx_column *column);
const char *grnxx_column_name(grnxx_column *column, size_t *size);
grnxx_data_type grnxx_column_data_type(grnxx_column *column);
grnxx_table *grnxx_column_reference_table(grnxx_column *column);
bool grnxx_column_is_key(grnxx_column *column);
size_t grnxx_column_num_indexes(grnxx_column *column);

grnxx_index *grnxx_column_create_index(grnxx_column *column,
                                       const char *name,
                                       grnxx_index_type index_type);
bool grnxx_column_remove_index(grnxx_column *column, const char *name);
bool grnxx_column_rename_index(grnxx_column *column,
                               const char *name,
                               const char *new_name);
bool grnxx_column_reorder_index(grnxx_column *column,
                                const char *name,
                                const char *prev_name);
grnxx_index *grnxx_column_get_index(grnxx_column *column, size_t index_id);
grnxx_index *grnxx_column_find_index(grnxx_column *column, const char *name);

bool grnxx_column_set(grnxx_column *column, int64_t row_id, const void *value);
bool grnxx_column_get(grnxx_column *column, int64_t row_id, void *value);

bool grnxx_column_contains(grnxx_column *column, const void *value);
int64_t grnxx_column_find_one(grnxx_column *column, const void *value);

// -- Index --

grnxx_column *grnxx_index_column(grnxx_index *index);
const char *grnxx_index_name(grnxx_index *index, size_t *size);
grnxx_index_type grnxx_index_index_type(grnxx_index *index);
size_t grnxx_index_num_entries(grnxx_index *index);

bool grnxx_index_test_uniqueness(grnxx_index *index);

bool grnxx_index_contains(grnxx_index *index, const void *value);
int64_t grnxx_index_find_one(grnxx_index *index, const void *value);

grnxx_cursor *grnxx_index_find(grnxx_index *index,
                               const void *value,
                               const grnxx_cursor_options *options);
grnxx_cursor *grnxx_index_find_in_range(grnxx_index *index,
                                        const void *lower_bound_value,
                                        bool lower_bound_is_inclusive,
                                        const void *upper_bound_value,
                                        bool upper_bound_is_inclusive,
                                        const grnxx_cursor_options *options);
grnxx_cursor *grnxx_index_find_starts_with(grnxx_index *index,
                                           const void *prefix,
                                           bool prefix_is_inclusive,
                                           const grnxx_cursor_options *options);
grnxx_cursor *grnxx_index_find_prefixes(grnxx_index *index,
                                        const void *value,
                                        const grnxx_cursor_options *options);

// -- Cursor --

void grnxx_cursor_close(grnxx_cursor *cursor);

size_t grnxx_cursor_read(grnxx_cursor *cursor,
                         grnxx_record *records,
                         size_t size);

// -- Expression --

grnxx_expression *grnxx_expression_parse(grnxx_table *table,
                                         const char *query);
void grnxx_expression_close(grnxx_expression *expression);

grnxx_table *grnxx_expression_table(grnxx_expression *expression);
grnxx_data_type grnxx_expression_data_type(grnxx_expression *expression);
bool grnxx_expression_is_row_id(grnxx_expression *expression);
bool grnxx_expression_is_score(grnxx_expression *expression);
size_t grnxx_expression_block_size(grnxx_expression *expression);

bool grnxx_expression_filter(grnxx_expression *expression,
                             grnxx_record *records,
                             size_t *size,
                             size_t offset,
                             size_t limit);
bool grnxx_expression_adjust(grnxx_expression *expression,
                             grnxx_record *records,
                             size_t size);
bool grnxx_expression_evaluate(grnxx_expression *expression,
                               const grnxx_record *records,
                               size_t size,
                               void *values);

// -- ExpressionBuilder --

grnxx_expression_builder *grnxx_expression_builder_create(grnxx_table *table);
void grnxx_expression_builder_close(grnxx_expression_builder *builder);

grnxx_table *grnxx_expression_builder_table(grnxx_expression_builder *builder);

bool grnxx_expression_builder_push_constant(grnxx_expression_builder *builder,
                                            grnxx_data_type data_type,
                                            const void *value);
bool grnxx_expression_builder_push_row_id(grnxx_expression_builder *builder);
bool grnxx_expression_builder_push_score(grnxx_expression_builder *builder);
bool grnxx_expression_builder_push_column(grnxx_expression_builder *builder,
                                          const char *column_name);
bool grnxx_expression_builder_push_operator(grnxx_expression_builder *builder,
                                            grnxx_operator_type operator_type);

bool grnxx_expression_builder_begin_subexpression(
    grnxx_expression_builder *builder);
bool grnxx_expression_builder_end_subexpression(
    grnxx_expression_builder *builder);

void grnxx_expression_builder_clear(grnxx_expression_builder *builder);

grnxx_expression *grnxx_expression_builder_release(
    grnxx_expression_builder *builder);

// -- Sorter --

// On success, the ownership of given expressions in "order" is moved to the
// created sorter object.
// On failure, given expressions are closed.
grnxx_sorter *grnxx_sorter_create(grnxx_sorter_order *orders,
                                  size_t num_orders,
                                  const grnxx_sorter_options *options);
void grnxx_sorter_close(grnxx_sorter *sorter);

// -- Merger --

grnxx_merger *grnxx_merger_create(const grnxx_merger_options *options);
void grnxx_merger_close(grnxx_merger *merger);

// -- Pipeline --

void grnxx_pipeline_close(grnxx_pipeline *pipeline);

grnxx_table *grnxx_pipeline_table(grnxx_pipeline *pipeline);

// On success, the starting address of records is stored into "*records".
// The caller must free the "*records".
bool grnxx_pipeline_flush(grnxx_pipeline *pipeline,
                          grnxx_record **records,
                          size_t *size);

// -- PipelineBuilder --

grnxx_pipeline_builder *grnxx_pipeline_builder_create(grnxx_table *table);
void grnxx_pipeline_builder_close(grnxx_pipeline_builder *builder);

grnxx_table *grnxx_pipeline_builder_table(grnxx_pipeline_builder *builder);

// On success, the ownership of given objects is moved to the pipeline.
// On failure, given objects are closed.
bool grnxx_pipeline_builder_push_cursor(grnxx_pipeline_builder *builder,
                                        grnxx_cursor *cursor);
bool grnxx_pipeline_builder_push_filter(grnxx_pipeline_builder *builder,
                                        grnxx_expression *expression,
                                        size_t offset,
                                        size_t limit);
bool grnxx_pipeline_builder_push_adjuster(grnxx_pipeline_builder *builder,
                                          grnxx_expression *expression);
bool grnxx_pipeline_builder_push_sorter(grnxx_pipeline_builder *builder,
                                        grnxx_sorter *sorter);
bool grnxx_pipeline_builder_push_merger(grnxx_pipeline_builder *builder,
                                        const grnxx_merger_options *options);

void grnxx_pipeline_builder_clear(grnxx_pipeline_builder *builder);

grnxx_pipeline *grnxx_pipeline_builder_release(
    grnxx_pipeline_builder *builder,
    const grnxx_pipeline_options *options);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  // GRNXX_H
