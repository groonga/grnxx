#ifndef GRNXX_TYPES_CONSTANTS_HPP
#define GRNXX_TYPES_CONSTANTS_HPP

#include "grnxx/types/base_types.hpp"

namespace grnxx {

// Zero is reserved for representing a null reference.
constexpr Int NULL_ROW_ID = 0;
constexpr Int MIN_ROW_ID  = 1;
constexpr Int MAX_ROW_ID  = (Int(1) << 40) - 1;

// Database data types.
enum DataType {
  // Content: True or false.
  // Default: false.
  BOOL_DATA,
  // Content: 64-bit signed integer.
  // Default: 0.
  INT_DATA,
  // Content: Double precision (64-bit) floating point number.
  // Default: 0.0.
  FLOAT_DATA,
  // Content: Latitude-longitude in milliseconds.
  // Default: (0, 0).
  GEO_POINT_DATA,
  // Content: Byte string.
  // Default: "".
  TEXT_DATA,
  // Content: Vector of Bool.
  // Default: {}.
  BOOL_VECTOR_DATA,
  // Content: Vector of Int.
  // Default: {}.
  INT_VECTOR_DATA,
  // Content: Vector of Float.
  // Default: {}.
  FLOAT_VECTOR_DATA,
  // Content: Vector of GeoPoint.
  // Default: {}.
  GEO_POINT_VECTOR_DATA,
  // Content: Vector of Text.
  // Default: {}.
  TEXT_VECTOR_DATA
};

enum IndexType {
  // Tree index supports range search.
  TREE_INDEX,
  // Hash index supports exact match search.
  HASH_INDEX
};

enum OrderType {
  // The natural order (the ascending order).
  REGULAR_ORDER,
  // The reverse order of REGULAR_ORDER (the descending order).
  REVERSE_ORDER
};

}  // namespace grnxx

#endif  // GRNXX_TYPES_CONSTANTS_HPP
