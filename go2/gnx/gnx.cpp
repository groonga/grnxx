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
  return gnx_insert_row2(ctx, table, key_type, key, row_id);
}

gnx_bool gnx_insert_row2(grn_ctx *ctx, grn_obj *table,
                         gnx_data_type key_type, const void *key,
                         gnx_int *row_id) {
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
    case GNX_GEO_POINT: {
      key_size = sizeof(gnx_geo_point);
      break;
    }
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
  return gnx_set_value2(ctx, column, row_id, value_type, value);
}

gnx_bool gnx_set_value2(grn_ctx *ctx, grn_obj *column, gnx_int row_id,
                        gnx_data_type value_type, const void *value) {
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
    case GNX_GEO_POINT: {
      gnx_geo_point geo_point = *static_cast<const gnx_geo_point *>(value);
      GRN_WGS84_GEO_POINT_INIT(&obj, 0);
      GRN_GEO_POINT_SET(ctx, &obj, geo_point.latitude, geo_point.longitude);
      break;
    }
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


gnx_int gnx_insert_rows(grn_ctx *ctx, const char *table_name,
                        gnx_int num_keys, gnx_data_type key_type,
                        const void *keys, gnx_int *row_ids,
                        gnx_bool *inserted) {
  grn_obj *table = grn_ctx_get(ctx, table_name, strlen(table_name));
  if (!table) {
    return GNX_NA_BOOL;
  }
  return gnx_insert_rows2(ctx, table, num_keys, key_type, keys, row_ids,
                          inserted);
}

gnx_int gnx_insert_rows2(grn_ctx *ctx, grn_obj *table,
                        gnx_int num_keys, gnx_data_type key_type,
                        const void *keys, gnx_int *row_ids,
                        gnx_bool *inserted) {
  // TODO: type check.
  gnx_int count = 0;
  switch (key_type) {
    case GNX_NA: {
      for (gnx_int i = 0; i < num_keys; ++i) {
        int added;
        grn_id id = grn_table_add(ctx, table, nullptr, 0, &added);
        if (id == GRN_ID_NIL) {
          row_ids[i] = GNX_NA_INT;
        } else {
          row_ids[i] = id;
          ++count;
        }
        inserted[i] = added ? GNX_TRUE : GNX_FALSE;
      }
      break;
    }
    case GNX_INT: {
      const gnx_int *int_keys = static_cast<const gnx_int *>(keys);
      for (gnx_int i = 0; i < num_keys; ++i) {
        int added;
        grn_id id = grn_table_add(ctx, table, &int_keys[i],
                                  sizeof(gnx_int), &added);
        if (id == GRN_ID_NIL) {
          row_ids[i] = GNX_NA_INT;
        } else {
          row_ids[i] = id;
          ++count;
        }
        inserted[i] = added ? GNX_TRUE : GNX_FALSE;
      }
      break;
    }
    case GNX_FLOAT: {
      const gnx_float *float_keys = static_cast<const gnx_float *>(keys);
      for (gnx_int i = 0; i < num_keys; ++i) {
        int added;
        grn_id id = grn_table_add(ctx, table, &float_keys[i],
                                  sizeof(gnx_float), &added);
        if (id == GRN_ID_NIL) {
          row_ids[i] = GNX_NA_INT;
        } else {
          row_ids[i] = id;
          ++count;
        }
        inserted[i] = added ? GNX_TRUE : GNX_FALSE;
      }
      break;
    }
    case GNX_GEO_POINT: {
      const gnx_geo_point *geo_point_keys =
        static_cast<const gnx_geo_point *>(keys);
      for (gnx_int i = 0; i < num_keys; ++i) {
        int added;
        grn_id id = grn_table_add(ctx, table, &geo_point_keys[i],
                                  sizeof(gnx_geo_point), &added);
        if (id == GRN_ID_NIL) {
          row_ids[i] = GNX_NA_INT;
        } else {
          row_ids[i] = id;
          ++count;
        }
        inserted[i] = added ? GNX_TRUE : GNX_FALSE;
      }
      break;
    }
    case GNX_TEXT: {
      const gnx_text *text_keys = static_cast<const gnx_text *>(keys);
      for (gnx_int i = 0; i < num_keys; ++i) {
        const void *key = text_keys[i].data;
        unsigned int key_size = text_keys[i].size;
        int added;
        grn_id id = grn_table_add(ctx, table, key, key_size, &added);
        if (id == GRN_ID_NIL) {
          row_ids[i] = GNX_NA_INT;
        } else {
          row_ids[i] = id;
          ++count;
        }
        inserted[i] = added ? GNX_TRUE : GNX_FALSE;
      }
      break;
    }
    default: {
      return GNX_NA_INT;
    }
  }
  return count;
}

gnx_int gnx_set_values(grn_ctx *ctx, const char *table_name,
                       const char *column_name, gnx_int num_values,
                       const gnx_int *row_ids, gnx_data_type value_type,
                       const void *values, gnx_bool *updated) {
  grn_obj *table = grn_ctx_get(ctx, table_name, strlen(table_name));
  if (!table) {
    return GNX_NA_BOOL;
  }
  grn_obj *column = grn_obj_column(
    ctx, table, column_name, strlen(column_name));
  if (!column) {
    return GNX_NA_BOOL;
  }
  return gnx_set_values2(ctx, table, column, num_values, row_ids,
                         value_type, values, updated);
}

gnx_int gnx_set_values2(grn_ctx *ctx, grn_obj *table, grn_obj *column,
                        gnx_int num_values, const gnx_int *row_ids,
                        gnx_data_type value_type, const void *values,
                        gnx_bool *updated) {
  gnx_int count = 0;
  switch (value_type) {
//    case GNX_NA: {
//      break;
//    }
    case GNX_BOOL: {
      grn_obj obj;
      GRN_BOOL_INIT(&obj, 0);
      for (gnx_int i = 0; i < num_values; ++i) {
        GRN_BOOL_SET(ctx, &obj,
          static_cast<const gnx_bool *>(values)[i] == GNX_TRUE);
        grn_rc rc = grn_obj_set_value(ctx, column, row_ids[i], &obj,
                                      GRN_OBJ_SET);
        if (rc == GRN_SUCCESS) {
          updated[i] = GNX_TRUE;
          ++count;
        } else {
          updated[i] = GNX_FALSE;
        }
      }
      GRN_OBJ_FIN(ctx, &obj);
      break;
    }
    case GNX_INT: {
      grn_obj obj;
      GRN_INT64_INIT(&obj, 0);
      for (gnx_int i = 0; i < num_values; ++i) {
        GRN_INT64_SET(ctx, &obj, static_cast<const gnx_int *>(values)[i]);
        grn_rc rc = grn_obj_set_value(ctx, column, row_ids[i], &obj,
                                      GRN_OBJ_SET);
        if (rc == GRN_SUCCESS) {
          updated[i] = GNX_TRUE;
          ++count;
        } else {
          updated[i] = GNX_FALSE;
        }
      }
      GRN_OBJ_FIN(ctx, &obj);
      break;
    }
    case GNX_FLOAT: {
      grn_obj obj;
      GRN_FLOAT_INIT(&obj, 0);
      for (gnx_int i = 0; i < num_values; ++i) {
        GRN_FLOAT_SET(ctx, &obj, static_cast<const gnx_float *>(values)[i]);
        grn_rc rc = grn_obj_set_value(ctx, column, row_ids[i], &obj,
                                      GRN_OBJ_SET);
        if (rc == GRN_SUCCESS) {
          updated[i] = GNX_TRUE;
          ++count;
        } else {
          updated[i] = GNX_FALSE;
        }
      }
      GRN_OBJ_FIN(ctx, &obj);
      break;
    }
    case GNX_GEO_POINT: {
      grn_obj obj;
      GRN_WGS84_GEO_POINT_INIT(&obj, 0);
      for (gnx_int i = 0; i < num_values; ++i) {
        gnx_geo_point value = static_cast<const gnx_geo_point *>(values)[i];
        GRN_GEO_POINT_SET(ctx, &obj, value.latitude, value.longitude);
        grn_rc rc = grn_obj_set_value(ctx, column, row_ids[i], &obj,
                                      GRN_OBJ_SET);
        if (rc == GRN_SUCCESS) {
          updated[i] = GNX_TRUE;
          ++count;
        } else {
          updated[i] = GNX_FALSE;
        }
      }
      GRN_OBJ_FIN(ctx, &obj);
      break;
    }
    case GNX_TEXT: {
      grn_obj obj;
      GRN_TEXT_INIT(&obj, 0);
      for (gnx_int i = 0; i < num_values; ++i) {
        gnx_text text = static_cast<const gnx_text *>(values)[i];
        GRN_TEXT_SET(ctx, &obj, text.data, text.size);
        grn_rc rc = grn_obj_set_value(ctx, column, row_ids[i], &obj,
                                      GRN_OBJ_SET);
        if (rc == GRN_SUCCESS) {
          updated[i] = GNX_TRUE;
          ++count;
        } else {
          updated[i] = GNX_FALSE;
        }
      }
      GRN_OBJ_FIN(ctx, &obj);
      break;
    }
    default: {
      return GNX_NA_INT;
    }
  }
  return count;
}

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus
