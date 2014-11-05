#ifndef GRNXX_DATA_TYPES_VECTOR_FLOAT_HPP
#define GRNXX_DATA_TYPES_VECTOR_FLOAT_HPP

#include "grnxx/data_types/data_type.hpp"
#include "grnxx/data_types/na.hpp"
#include "grnxx/data_types/scalar/float.hpp"

namespace grnxx {

template <typename T> class Vector;

template <>
class Vector<Float> {
 public:
  // TODO
  constexpr DataType type() const {
    return FLOAT_VECTOR_DATA;
  }
};

using FloatVector = Vector<Float>;

}  // namespace grnxx

#endif  // GRNXX_DATA_TYPES_VECTOR_FLOAT_HPP
