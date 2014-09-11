#ifndef GRNXX_TYPES_HPP
#define GRNXX_TYPES_HPP

#include "grnxx/types/base_types.hpp"
#include "grnxx/types/constants.hpp"
#include "grnxx/types/data_types.hpp"
#include "grnxx/types/datum.hpp"
#include "grnxx/types/forward.hpp"
#include "grnxx/types/geo_point.hpp"
#include "grnxx/types/options.hpp"
#include "grnxx/types/string.hpp"
#include "grnxx/types/traits.hpp"
#include "grnxx/types/vector.hpp"

namespace grnxx {

struct Record {
  Int row_id;
  Float score;

  Record() = default;
  Record(Int row_id, Float score) : row_id(row_id), score(score) {}

  Record &operator=(const Record &) & = default;
};

struct SortOrder {
  unique_ptr<Expression> expression;
  OrderType type;

  SortOrder();
  explicit SortOrder(unique_ptr<Expression> &&expression,
                     OrderType type = REGULAR_ORDER);
  SortOrder(SortOrder &&order);
  ~SortOrder();
};

}  // namespace grnxx

#endif  // GRNXX_TYPES_HPP
