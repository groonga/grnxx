#ifndef GRNXX_DATA_TYPES_DATA_TYPE_HPP
#define GRNXX_DATA_TYPES_DATA_TYPE_HPP

namespace grnxx {

enum DataType {
  // N/A.
  NA_DATA,
  // True or false.
  BOOL_DATA,
  // 64-bit signed integer.
  INT_DATA,
  // Double precision (64-bit) floating point number.
  FLOAT_DATA,
  // Latitude-longitude in milliseconds.
  GEO_POINT_DATA,
  // Byte string.
  TEXT_DATA,
  // Vector of Bool.
  BOOL_VECTOR_DATA,
  // Vector of Int.
  INT_VECTOR_DATA,
  // Vector of Float.
  FLOAT_VECTOR_DATA,
  // Vector of GeoPoint.
  GEO_POINT_VECTOR_DATA,
  // Vector of Text.
  TEXT_VECTOR_DATA
};

}  // namespace grnxx

#endif  // GRNXX_DATA_TYPES_DATA_TYPE_HPP
