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
//typedef gnx_geo_point struct { int32_t latitude; int32_t longitude; };
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

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  // GNX_H
