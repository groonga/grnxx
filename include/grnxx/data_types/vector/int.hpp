#ifndef GRNXX_DATA_TYPES_VECTOR_INT_HPP
#define GRNXX_DATA_TYPES_VECTOR_INT_HPP

#include "grnxx/data_types/data_type.hpp"
#include "grnxx/data_types/na.hpp"
#include "grnxx/data_types/scalar/int.hpp"

namespace grnxx {

template <typename T> class Vector;

template <>
class Vector<Int> {
 public:
  // TODO
  static constexpr DataType type() {
    return INT_VECTOR_DATA;
  }
};

using IntVector = Vector<Int>;

}  // namespace grnxx

#endif  // GRNXX_DATA_TYPES_VECTOR_INT_HPP
