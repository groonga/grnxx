#ifndef GRNXX_DATA_TYPES_VECTOR_ROW_ID_HPP
#define GRNXX_DATA_TYPES_VECTOR_ROW_ID_HPP

#include "grnxx/data_types/na.hpp"
#include "grnxx/data_types/scalar/row_id.hpp"

namespace grnxx {

template <typename T> class Vector;

template <>
class Vector<RowID> {
  // TODO
};

using RowIDVector = Vector<RowID>;

}  // namespace grnxx

#endif  // GRNXX_DATA_TYPES_VECTOR_ROW_ID_HPP
