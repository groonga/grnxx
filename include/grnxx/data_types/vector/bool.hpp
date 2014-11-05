#ifndef GRNXX_DATA_TYPES_VECTOR_BOOL_HPP
#define GRNXX_DATA_TYPES_VECTOR_BOOL_HPP

#include "grnxx/data_types/data_type.hpp"
#include "grnxx/data_types/na.hpp"
#include "grnxx/data_types/scalar/bool.hpp"

namespace grnxx {

template <typename T> class Vector;

template <>
class Vector<Bool> {
  // TODO
};

using BoolVector = Vector<Bool>;

}  // namespace grnxx

#endif  // GRNXX_DATA_TYPES_VECTOR_BOOL_HPP
