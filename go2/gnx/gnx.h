#ifndef GNX_H
#define GNX_H

#include <groonga.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

typedef enum gnx_data_type {
  GNX_NA,
  GNX_BOOL,
  GNX_INT,
  GNX_FLOAT,
  GNX_GEO_POINT,
  GNX_TEXT
} gnx_data_type;

typedef uint8_t gnx_bool;
typedef int64_t gnx_int;
typedef double gnx_float;
//typedef grn_geo_point gnx_geo_point;
typedef struct {
  int32_t latitude;
  int32_t longitude;
} gnx_geo_point;
typedef struct {
  const char *data;
  gnx_int size;
} gnx_text;

#define GNX_TRUE ((gnx_bool)3)
#define GNX_FALSE ((gnx_bool)0)

#define GNX_NA_BOOL ((gnx_bool)1)
#define GNX_NA_INT (((gnx_int)1) << 63)

gnx_bool gnx_insert_row(grn_ctx *ctx, const char *table_name,
                        gnx_data_type key_type, const void *key,
                        gnx_int *row_id);
gnx_bool gnx_insert_row2(grn_ctx *ctx, grn_obj *table,
                         gnx_data_type key_type, const void *key,
                         gnx_int *row_id);

gnx_bool gnx_set_value(grn_ctx *ctx, const char *table_name,
                       const char *column_name, gnx_int row_id,
                       gnx_data_type value_type, const void *value);
gnx_bool gnx_set_value2(grn_ctx *ctx,grn_obj *column, gnx_int row_id,
                        gnx_data_type value_type, const void *value);

gnx_int gnx_insert_rows(grn_ctx *ctx, const char *table_name,
                        gnx_int num_keys, gnx_data_type key_type,
                        const void *keys, gnx_int *row_ids,
                        gnx_bool *inserted);
gnx_int gnx_insert_rows2(grn_ctx *ctx, grn_obj *table,
                        gnx_int num_keys, gnx_data_type key_type,
                        const void *keys, gnx_int *row_ids,
                        gnx_bool *inserted);

gnx_int gnx_set_values(grn_ctx *ctx, const char *table_name,
                       const char *column_name, gnx_int num_values,
                       const gnx_int *row_ids, gnx_data_type value_type,
                       const void *values, gnx_bool *updated);
gnx_int gnx_set_values2(grn_ctx *ctx, grn_obj *table, grn_obj *column,
                        gnx_int num_values, const gnx_int *row_ids,
                        gnx_data_type value_type, const void *values,
                        gnx_bool *updated);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  // GNX_H
