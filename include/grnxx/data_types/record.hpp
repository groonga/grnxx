#ifndef GRNXX_DATA_TYPES_RECORD_HPP
#define GRNXX_DATA_TYPES_RECORD_HPP

#include "grnxx/data_types/scalar/int.hpp"
#include "grnxx/data_types/scalar/float.hpp"

namespace grnxx {

struct Record {
  Int row_id;
  Float score;

  Record() = default;
  ~Record() = default;

  Record(const Record &) = default;
  Record &operator=(const Record &) & = default;

  Record(Int row_id, Float score) : row_id(row_id), score(score) {}
};

}  // namespace grnxx

#endif  // GRNXX_DATA_TYPES_RECORD_HPP
