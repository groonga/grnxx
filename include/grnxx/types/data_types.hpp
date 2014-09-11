#ifndef GRNXX_TYPES_DATA_TYPES_HPP
#define GRNXX_TYPES_DATA_TYPES_HPP

#include "grnxx/types/base_types.hpp"
#include "grnxx/types/geo_point.hpp"
#include "grnxx/types/string.hpp"
#include "grnxx/types/vector.hpp"

namespace grnxx {

// Built-in data types (Bool, Int, and Float) are provided in
// grnxx/types/base_types.hpp.

// GeoPoint is provided in grnxx/types/geo_point.hpp.

// String is provided in grnxx/types/string.hpp.
using Text = String;

// Vector<T> are provided in grnxx/types/vector.hpp.
using BoolVector     = Vector<Bool>;
using IntVector      = Vector<Int>;
using FloatVector    = Vector<Float>;
using GeoPointVector = Vector<GeoPoint>;
using TextVector     = Vector<Text>;

}  // namespace grnxx

#endif  // GRNXX_TYPES_DATA_TYPES_HPP
