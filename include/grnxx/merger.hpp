#ifndef GRNXX_MERGER_HPP
#define GRNXX_MERGER_HPP

#include <limits>
#include <memory>

#include "grnxx/array.hpp"
#include "grnxx/data_types.hpp"

namespace grnxx {

enum MergerLogicalOperatorType {
  // Keep records included in both the first input stream and the second input
  // stream.
  MERGER_LOGICAL_AND,
  // Keep records included in the first input stream and/or the second input
  // stream.
  MERGER_LOGICAL_OR,
  // Keep records included in only one of the input streams.
  MERGER_LOGICAL_XOR,
  // Keep records included in the first input stream and not included in the
  // second input stream.
  MERGER_LOGICAL_MINUS,
  // Keep records included in the first input stream.
  MERGER_LOGICAL_LEFT,
  // Keep records included in the second input stream.
  MERGER_LOGICAL_RIGHT
};

enum MergerScoreOperatorType {
  // Add the first input score and the second input score.
  MERGER_SCORE_PLUS,
  // Subtract the second input score from the first input score.
  MERGER_SCORE_MINUS,
  // Multiply the first input score by the second input score.
  MERGER_SCORE_MULTIPLICATION,
  // Ignores the second input score.
  MERGER_SCORE_LEFT,
  // Ignores the first input score.
  MERGER_SCORE_RIGHT,
  // All zeros.
  MERGER_SCORE_ZERO
};

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
      : logical_operator_type(MERGER_LOGICAL_AND),
        score_operator_type(MERGER_SCORE_PLUS),
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
