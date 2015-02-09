#ifndef GRNXX_SORTER_HPP
#define GRNXX_SORTER_HPP

#include <limits>
#include <memory>

#include "grnxx/array.hpp"
#include "grnxx/constants.h"
#include "grnxx/data_types.hpp"
#include "grnxx/expression.hpp"
#include "grnxx/table.hpp"

namespace grnxx {

using SorterOrderType = grnxx_order_type;

struct SorterOrder {
  std::unique_ptr<Expression> expression;
  SorterOrderType type;
};

struct SorterOptions {
  // The first "offset" records are skipped.
  size_t offset;

  // At most "limit" records are sorted.
  size_t limit;

  SorterOptions()
      : offset(0),
        limit(std::numeric_limits<size_t>::max()) {}
};

class Sorter {
 public:
  Sorter() = default;
  virtual ~Sorter() = default;

  // Create an object for sorting records.
  //
  // On success, returns the sorter.
  // On failure, throws an exception.
  static std::unique_ptr<Sorter> create(
      Array<SorterOrder> &&orders,
      const SorterOptions &options = SorterOptions());

  // Return the associated table.
  virtual const Table *table() const = 0;

  // Set the target record set.
  //
  // Aborts sorting the old record set and starts sorting the new record set.
  //
  // On failure, throws an exception.
  virtual void reset(Array<Record> *records) = 0;

  // Progress sorting.
  //
  // On failure, throws an exception.
  virtual void progress() = 0;

  // Finish sorting.
  //
  // Assumes that all the records are ready.
  // Leaves only the result records if offset and limit are specified.
  //
  // On failure, throws an exception.
  virtual void finish() = 0;

  // Sort records.
  //
  // Calls reset() and finish() to sort records.
  //
  // On failure, throws an exception.
  virtual void sort(Array<Record> *records) = 0;
};

}  // namespace grnxx

#endif  // GRNXX_SORTER_HPP
