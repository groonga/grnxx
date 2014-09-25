#ifndef GRNXX_TYPES_OPTIONS_HPP
#define GRNXX_TYPES_OPTIONS_HPP

#include "grnxx/types/base_types.hpp"
#include "grnxx/types/constants.hpp"

namespace grnxx {

// Database persistent object option types.
struct DBOptions {
  DBOptions();
};

struct TableOptions {
  TableOptions();
};

struct ColumnOptions {
  // The referenced (parent) table.
  String ref_table_name;

  ColumnOptions();
};

struct IndexOptions {
  IndexOptions();
};

struct CursorOptions {
  // The first "offset" records are skipped (default: 0).
  Int offset;

  // At most "limit" records are read (default: numeric_limits<Int>::max()).
  Int limit;

  // The order of records (default: REGULAR_ORDER).
  OrderType order_type;

  CursorOptions();
};

struct ExpressionOptions {
  // Records are evaluated per block.
  Int block_size;

  ExpressionOptions();
};

struct SorterOptions {
  // The first "offset" records are skipped (default: 0).
  Int offset;

  // At most "limit" records are sorted (default: numeric_limits<Int>::max()).
  Int limit;

  SorterOptions();
};

struct MergerOptions {
  // How to merge row IDs.
  MergerType type;

  // How to merge scores.
  MergerOperatorType operator_type;

  // This value is used when a corresponding record does not exist.
  Float null_score;

  // The first "offset" records are skipped (default: 0).
  Int offset;

  // At most "limit" records are returned
  // (default: numeric_limits<Int>::max()).
  Int limit;

  MergerOptions();
};

struct PipelineOptions {
  PipelineOptions();
};

}  // namespace grnxx

#endif  // GRNXX_TYPES_OPTIONS_HPP
