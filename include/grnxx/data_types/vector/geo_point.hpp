#ifndef GRNXX_DATA_TYPES_VECTOR_GEO_POINT_HPP
#define GRNXX_DATA_TYPES_VECTOR_GEO_POINT_HPP

#include "grnxx/data_types/data_type.hpp"
#include "grnxx/data_types/na.hpp"
#include "grnxx/data_types/scalar/geo_point.hpp"

namespace grnxx {

template <typename T> class Vector;

template <>
class Vector<GeoPoint> {
 public:
  // TODO
  static constexpr DataType type() {
    return GEO_POINT_VECTOR_DATA;
  }
};

using GeoPointVector = Vector<GeoPoint>;

}  // namespace grnxx

#endif  // GRNXX_DATA_TYPES_VECTOR_GEO_POINT_HPP
