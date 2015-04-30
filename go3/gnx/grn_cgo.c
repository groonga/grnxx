#include "grn_cgo.h"

#include <string.h>

#define GRN_CGO_MAX_DATA_TYPE_ID GRN_DB_WGS84_GEO_POINT

grn_obj *grn_cgo_find_table(grn_ctx *ctx, const char *name, int name_len) {
  grn_obj *obj = grn_ctx_get(ctx, name, name_len);
  if (!obj) {
    return NULL;
  }
  switch (obj->header.type) {
    case GRN_TABLE_HASH_KEY:
    case GRN_TABLE_PAT_KEY:
    case GRN_TABLE_DAT_KEY:
    case GRN_TABLE_NO_KEY: {
      return obj;
    }
    default: {
      // The object is not a table.
      return NULL;
    }
  }
}

// grn_cgo_init_type_info() initializes the members of type_info.
// The initialized type info specifies a valid Void type.
static void grn_cgo_init_type_info(grn_cgo_type_info *type_info) {
  type_info->data_type = GRN_DB_VOID;
  type_info->dimension = 0;
  type_info->ref_table = NULL;
}

grn_bool grn_cgo_table_get_key_info(grn_ctx *ctx, grn_obj *table,
                                    grn_cgo_type_info *key_info) {
  grn_cgo_init_type_info(key_info);
  while (table) {
    switch (table->header.type) {
      case GRN_TABLE_HASH_KEY:
      case GRN_TABLE_PAT_KEY:
      case GRN_TABLE_DAT_KEY: {
        if (table->header.domain <= GRN_CGO_MAX_DATA_TYPE_ID) {
          key_info->data_type = table->header.domain;
          return GRN_TRUE;
        }
        table = grn_ctx_at(ctx, table->header.domain);
        if (!table) {
          return GRN_FALSE;
        }
        if (!key_info->ref_table) {
          key_info->ref_table = table;
        }
        break;
      }
      case GRN_TABLE_NO_KEY: {
        return GRN_TRUE;
      }
      default: {
        return GRN_FALSE;
      }
    }
  }
  return GRN_FALSE;
}

grn_bool grn_cgo_table_get_value_info(grn_ctx *ctx, grn_obj *table,
                                      grn_cgo_type_info *value_info) {
  grn_cgo_init_type_info(value_info);
  if (!table) {
    return GRN_FALSE;
  }
  switch (table->header.type) {
    case GRN_TABLE_HASH_KEY:
    case GRN_TABLE_PAT_KEY:
    case GRN_TABLE_DAT_KEY:
    case GRN_TABLE_NO_KEY: {
      grn_id range = grn_obj_get_range(ctx, table);
      if (range <= GRN_CGO_MAX_DATA_TYPE_ID) {
        value_info->data_type = range;
        return GRN_TRUE;
      }
      value_info->ref_table = grn_ctx_at(ctx, range);
      grn_cgo_type_info key_info;
      if (!grn_cgo_table_get_key_info(ctx, value_info->ref_table, &key_info)) {
        return GRN_FALSE;
      }
      value_info->data_type = key_info.data_type;
      return GRN_TRUE;
    }
    default: {
      return GRN_FALSE;
    }
  }
}

grn_bool grn_cgo_column_get_value_info(grn_ctx *ctx, grn_obj *column,
                                       grn_cgo_type_info *value_info) {
  grn_cgo_init_type_info(value_info);
  if (!column) {
    return GRN_FALSE;
  }
  switch (column->header.type) {
    case GRN_COLUMN_FIX_SIZE: {
      break;
    }
    case GRN_COLUMN_VAR_SIZE: {
      grn_obj_flags type = column->header.flags & GRN_OBJ_COLUMN_TYPE_MASK;
      if (type == GRN_OBJ_COLUMN_VECTOR) {
        ++value_info->dimension;
      }
      break;
    }
    default: {
      return GRN_FALSE;
    }
  }
  grn_id range = grn_obj_get_range(ctx, column);
  if (range <= GRN_CGO_MAX_DATA_TYPE_ID) {
    value_info->data_type = range;
    return GRN_TRUE;
  }
  value_info->ref_table = grn_ctx_at(ctx, range);
  grn_cgo_type_info key_info;
  if (!grn_cgo_table_get_key_info(ctx, value_info->ref_table, &key_info)) {
    return GRN_FALSE;
  }
  value_info->data_type = key_info.data_type;
  return GRN_TRUE;
}

char *grn_cgo_table_get_name(grn_ctx *ctx, grn_obj *table) {
  if (!table) {
    return NULL;
  }
  switch (table->header.type) {
    case GRN_TABLE_HASH_KEY:
    case GRN_TABLE_PAT_KEY:
    case GRN_TABLE_DAT_KEY:
    case GRN_TABLE_NO_KEY: {
      break;
    }
    default: {
      return NULL;
    }
  }
  char buf[GRN_TABLE_MAX_KEY_SIZE];
  int len = grn_obj_name(ctx, table, buf, GRN_TABLE_MAX_KEY_SIZE);
  if (len <= 0) {
    return NULL;
  }
  char *table_name = (char *)malloc(len + 1);
  if (!table_name) {
    return NULL;
  }
  memcpy(table_name, buf, len);
  table_name[len] = '\0';
  return table_name;
}

// grn_cgo_table_insert_row() calls grn_table_add() and converts the result.
static grn_cgo_row_info grn_cgo_table_insert_row(
    grn_ctx *ctx, grn_obj *table, const void *key_ptr, size_t key_size) {
  grn_cgo_row_info row_info;
  int inserted;
  row_info.id = grn_table_add(ctx, table, key_ptr, key_size, &inserted);
  row_info.inserted = inserted ? GRN_TRUE : GRN_FALSE;
  return row_info;
}

grn_cgo_row_info grn_cgo_table_insert_void(grn_ctx *ctx, grn_obj *table) {
  return grn_cgo_table_insert_row(ctx, table, NULL, 0);
}

grn_cgo_row_info grn_cgo_table_insert_bool(grn_ctx *ctx, grn_obj *table,
                                           grn_bool key) {
  return grn_cgo_table_insert_row(ctx, table, &key, sizeof(key));
}

grn_cgo_row_info grn_cgo_table_insert_int(grn_ctx *ctx, grn_obj *table,
                                          int64_t key) {
  return grn_cgo_table_insert_row(ctx, table, &key, sizeof(key));
}

grn_cgo_row_info grn_cgo_table_insert_float(grn_ctx *ctx, grn_obj *table,
                                            double key) {
  return grn_cgo_table_insert_row(ctx, table, &key, sizeof(key));
}

grn_cgo_row_info grn_cgo_table_insert_geo_point(grn_ctx *ctx, grn_obj *table,
                                                grn_geo_point key) {
  return grn_cgo_table_insert_row(ctx, table, &key, sizeof(key));
}

grn_cgo_row_info grn_cgo_table_insert_text(grn_ctx *ctx, grn_obj *table,
                                           const grn_cgo_text *key) {
  return grn_cgo_table_insert_row(ctx, table, key->ptr, key->size);
}
