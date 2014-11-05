#ifndef GRNXX_DATA_TYPES_VECTOR_TEXT_HPP
#define GRNXX_DATA_TYPES_VECTOR_TEXT_HPP

#include "grnxx/data_types/data_type.hpp"
#include "grnxx/data_types/scalar/text.hpp"

namespace grnxx {

template <typename T> class Vector;

template <>
class Vector<Text> {
  // TODO
};

using TextVector = Vector<Text>;

}  // namespace grnxx

#endif  // GRNXX_DATA_TYPES_VECTOR_TEXT_HPP
