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

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus
