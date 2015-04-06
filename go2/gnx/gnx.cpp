#include "gnx.h"

#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

gnx_bool gnx_insert_row(grn_ctx *ctx, const char *table_name,
                        gnx_data_type key_type, const void *key,
                        gnx_int *row_id) {
  grn_obj *table = grn_ctx_get(ctx, table_name, strlen(table_name));
  if (!table) {
    *row_id = GNX_NA_INT;
    return GNX_NA_BOOL;
  }
  // TODO: type check.
  unsigned int key_size = 0;
  switch (key_type) {
    case GNX_NA: {
      key = nullptr;
      break;
    }
    case GNX_INT: {
      key_size = sizeof(gnx_int);
      break;
    }
    case GNX_FLOAT: {
      key_size = sizeof(gnx_float);
      break;
    }
//    case GNX_GEO_POINT: {
//      key_size = sizeof(gnx_geo_point);
//      break;
//    }
    case GNX_TEXT: {
      gnx_text text = *static_cast<const gnx_text *>(key);
      key = text.data;
      key_size = text.size;
      break;
    }
    default: {
      *row_id = GNX_NA_INT;
      return GNX_NA_BOOL;
    }
  }
  int added;
  grn_id id = grn_table_add(ctx, table, key, key_size, &added);
  if (id == GRN_ID_NIL) {
    *row_id = GNX_NA_INT;
    return GNX_NA_BOOL;
  }
  *row_id = id;
  return added ? GNX_TRUE : GNX_FALSE;
}

gnx_bool gnx_set_value(grn_ctx *ctx, const char *table_name,
                       const char *column_name, gnx_int row_id,
                       gnx_data_type value_type, const void *value) {
  grn_obj *table = grn_ctx_get(ctx, table_name, strlen(table_name));
  if (!table) {
    return GNX_NA_BOOL;
  }
  grn_obj *column = grn_obj_column(
    ctx, table, column_name, strlen(column_name));
  if (!column) {
    return GNX_NA_BOOL;
  }
  grn_obj obj;
  switch (value_type) {
//    case GNX_NA: {
//      break;
//    }
    case GNX_BOOL: {
      GRN_BOOL_INIT(&obj, 0);
      GRN_BOOL_SET(ctx, &obj,
        *static_cast<const gnx_bool *>(value) == GNX_TRUE);
      break;
    }
    case GNX_INT: {
      GRN_INT64_INIT(&obj, 0);
      GRN_INT64_SET(ctx, &obj, *static_cast<const gnx_int *>(value));
      break;
    }
    case GNX_FLOAT: {
      GRN_FLOAT_INIT(&obj, 0);
      GRN_FLOAT_SET(ctx, &obj, *static_cast<const gnx_float *>(value));
      break;
    }
//    case GNX_GEO_POINT: {
//      break;
//    }
    case GNX_TEXT: {
      gnx_text text = *static_cast<const gnx_text *>(value);
      GRN_TEXT_INIT(&obj, 0);
      GRN_TEXT_SET(ctx, &obj, text.data, text.size);
      break;
    }
    default: {
      return GNX_NA_BOOL;
    }
  }
  grn_rc rc = grn_obj_set_value(ctx, column, row_id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  if (rc != GRN_SUCCESS) {
    return GNX_NA_BOOL;
  }
  return GNX_TRUE;
}

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus
