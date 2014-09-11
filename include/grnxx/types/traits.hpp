#ifndef GRNXX_TYPES_TRAITS_HPP
#define GRNXX_TYPES_TRAITS_HPP

#include "grnxx/types/base_types.hpp"
#include "grnxx/types/constants.hpp"
#include "grnxx/types/data_types.hpp"
#include "grnxx/types/geo_point.hpp"
#include "grnxx/types/string.hpp"

namespace grnxx {

template <typename T> struct TypeTraits;

template <> struct TypeTraits <Bool> {
  static DataType data_type() {
    return BOOL_DATA;
  }
  static Bool default_value() {
    return false;
  }
};

template <> struct TypeTraits <Int> {
  static DataType data_type() {
    return INT_DATA;
  }
  static Int default_value() {
    return 0;
  }
};

template <> struct TypeTraits <Float> {
  static DataType data_type() {
    return FLOAT_DATA;
  }
  static Float default_value() {
    return 0.0;
  }
};

template <> struct TypeTraits <GeoPoint> {
  static DataType data_type() {
    return GEO_POINT_DATA;
  }
  static GeoPoint default_value() {
    return GeoPoint(0, 0);
  }
};

template <> struct TypeTraits <Text> {
  static DataType data_type() {
    return TEXT_DATA;
  }
  static Text default_value() {
    return Text("", 0);
  }
};
template <> struct TypeTraits <Vector<Bool>> {
  static DataType data_type() {
    return BOOL_VECTOR_DATA;
  }
  static Vector<Bool> default_value() {
    return Vector<Bool>(0, 0);
  }
};

template <> struct TypeTraits <Vector<Int>> {
  static DataType data_type() {
    return INT_VECTOR_DATA;
  }
  static Vector<Int> default_value() {
    return Vector<Int>(nullptr, 0);
  }
};

template <> struct TypeTraits <Vector<Float>> {
  static DataType data_type() {
    return FLOAT_VECTOR_DATA;
  }
  static Vector<Float> default_value() {
    return Vector<Float>(nullptr, 0);
  }
};

template <> struct TypeTraits <Vector<GeoPoint>> {
  static DataType data_type() {
    return GEO_POINT_VECTOR_DATA;
  }
  static Vector<GeoPoint> default_value() {
    return Vector<GeoPoint>(nullptr, 0);
  }
};

template <> struct TypeTraits <Vector<Text>> {
  static DataType data_type() {
    return TEXT_VECTOR_DATA;
  }
  static Vector<Text> default_value() {
    return Vector<Text>(nullptr, 0);
  }
};

}  // namespace grnxx

#endif  // GRNXX_TYPES_TRAITS_HPP
