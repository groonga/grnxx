#ifndef GRNXX_MERGER_HPP
#define GRNXX_MERGER_HPP

#include <limits>
#include <memory>

#include "grnxx/array.hpp"
#include "grnxx/constants.h"
#include "grnxx/data_types.hpp"

namespace grnxx {

using MergerLogicalOperatorType = grnxx_merger_operator_type;
using MergerScoreOperatorType = grnxx_merger_operator_type;

struct MergerOptions {
  // How to merge records.
  MergerLogicalOperatorType logical_operator_type;
  // How to merge scores.
  MergerScoreOperatorType score_operator_type;
  // The score of a missing record is replaced with this value.
  Float missing_score;
  // The first "offset" records are skipped.
  size_t offset;
  // At most "limit" records are returned.
  size_t limit;

  MergerOptions()
      : logical_operator_type(GRNXX_MERGER_AND),
        score_operator_type(GRNXX_MERGER_PLUS),
        missing_score(0.0),
        offset(0),
        limit(std::numeric_limits<size_t>::max()) {}
};

class Merger {
 public:
  Merger() = default;
  virtual ~Merger() = default;

  // Create an object for merging record sets.
  //
  // On success, returns the merger.
  // On failure, throws an exception.
  static std::unique_ptr<Merger> create(
      const MergerOptions &options = MergerOptions());

  // Set the target record sets.
  //
  // Aborts merging the old record sets and starts merging the new record sets.
  //
  // On failure, throws an exception.
  virtual void reset(Array<Record> *input_records_1,
                     Array<Record> *input_records_2,
                     Array<Record> *output_records) = 0;

  // Progress merging.
  //
  // On failure, throws an exception.
  virtual void progress() = 0;

  // Finish merging.
  //
  // Assumes that all the records are ready.
  // Leaves only the result records if offset and limit are specified.
  //
  // On failure, throws an exception.
  virtual void finish() = 0;

  // Merge records.
  //
  // Calls reset() and finish() to merge records.
  //
  // On failure, throws an exception.
  virtual void merge(Array<Record> *input_records_1,
                     Array<Record> *input_records_2,
                     Array<Record> *output_records) = 0;
};

}  // namespace grnxx

#endif  // GRNXX_MERGER_HPP
