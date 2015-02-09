#ifndef GRNXX_DATA_TYPES_NA_HPP
#define GRNXX_DATA_TYPES_NA_HPP

#include "grnxx/data_types/data_type.hpp"

namespace grnxx {

struct NA {
  constexpr DataType type() const {
    return GRNXX_NA;
  }
};

}  // namespace grnxx

#endif  // GRNXX_DATA_TYPES_NA_HPP
