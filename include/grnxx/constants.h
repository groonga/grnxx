#ifndef GRNXX_CONSTANTS_H
#define GRNXX_CONSTANTS_H

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

typedef enum {
  // N/A.
  GRNXX_NA,
  // True or false.
  GRNXX_BOOL,
  // 64-bit signed integer.
  GRNXX_INT,
  // Double precision (64-bit) floating point number.
  GRNXX_FLOAT,
  // Latitude-longitude in milliseconds.
  GRNXX_GEO_POINT,
  // Byte string.
  GRNXX_TEXT,
  // Vector of Bool.
  GRNXX_BOOL_VECTOR,
  // Vector of Int.
  GRNXX_INT_VECTOR,
  // Vector of Float.
  GRNXX_FLOAT_VECTOR,
  // Vector of GeoPoint.
  GRNXX_GEO_POINT_VECTOR,
  // Vector of Text.
  GRNXX_TEXT_VECTOR
} grnxx_data_type;

typedef enum {
  // The natural order (the ascending order in most cases).
  GRNXX_REGULAR_ORDER,
  // The reverse order (the descending order in most cases).
  GRNXX_REVERSE_ORDER
} grnxx_order_type;

typedef enum {
  // Tree indexes support range search.
  GRNXX_TREE_INDEX,
  // Hash indexes support exact match search.
  GRNXX_HASH_INDEX
} grnxx_index_type;

typedef enum {
  GRNXX_MERGER_AND,             // For Logical.
  GRNXX_MERGER_OR,              // For Logical.
  GRNXX_MERGER_XOR,             // For Logical.
  GRNXX_MERGER_PLUS,            // For Score.
  GRNXX_MERGER_MINUS,           // For Logical, Score.
  GRNXX_MERGER_MULTIPLICATION,  // For Score.
  GRNXX_MERGER_LEFT,            // For Logical, Score.
  GRNXX_MERGER_RIGHT,           // For Logical, Score.
  GRNXX_MERGER_ZERO             // For Score.
} grnxx_merger_operator_type;

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  // GRNXX_GRNXX_H
