#include "grnxx/types.hpp"

#include "grnxx/expression.hpp"

namespace grnxx {

void GeoPoint::fix(Int *latitude, Int *longitude) {
  // Fix the latitude to [0, 360).
  if ((*latitude <= DEGREES(-360)) || (*latitude >= DEGREES(360))) {
    *latitude %= DEGREES(360);
  }
  if (*latitude < DEGREES(0)) {
    *latitude += DEGREES(360);
  }
  // Fix the longitude to [0, 360).
  if ((*longitude <= DEGREES(-360)) || (*longitude >= DEGREES(360))) {
    *longitude %= DEGREES(360);
  }
  if (*longitude < DEGREES(0)) {
    *longitude += DEGREES(360);
  }
  // Fix the latitude in (90, 270).
  if ((*latitude > DEGREES(90)) && (*latitude < DEGREES(270))) {
    *latitude = DEGREES(180) - *latitude;
    if (*latitude < DEGREES(0)) {
      *latitude += DEGREES(360);
    }
    *longitude += DEGREES(180);
    if (*longitude >= DEGREES(360)) {
      *longitude -= DEGREES(360);
    }
  }
  // Fix the latitude to [-90, 90].
  if (*latitude >= DEGREES(270)) {
    *latitude -= DEGREES(360);
  }
  // Fix the longitude to [-180, 180).
  if (*longitude >= DEGREES(180)) {
    *longitude -= DEGREES(360);
  }
}

DBOptions::DBOptions() {}

TableOptions::TableOptions() {}

ColumnOptions::ColumnOptions() : ref_table_name("") {}

IndexOptions::IndexOptions() {}

CursorOptions::CursorOptions()
    : offset(0),
      limit(numeric_limits<Int>::max()),
      order_type(REGULAR_ORDER) {}

ExpressionOptions::ExpressionOptions()
    : block_size(1024) {}

SorterOptions::SorterOptions()
    : offset(0),
      limit(numeric_limits<Int>::max()) {}

MergerOptions::MergerOptions()
    : type(AND_MERGER),
      operator_type(PLUS_MERGER_OPERATOR),
      null_score(0.0),
      offset(0),
      limit(numeric_limits<Int>::max()) {}

PipelineOptions::PipelineOptions() {}

SortOrder::SortOrder() : expression(), type(REGULAR_ORDER) {}

SortOrder::SortOrder(SortOrder &&order)
    : expression(std::move(order.expression)),
      type(order.type) {}

SortOrder::SortOrder(unique_ptr<Expression> &&expression, OrderType type)
    : expression(std::move(expression)),
      type(type) {}

SortOrder::~SortOrder() {}

}  // namespace grnxx
