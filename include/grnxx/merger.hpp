#ifndef GRNXX_MERGER_HPP
#define GRNXX_MERGER_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class Merger {
 public:
  Merger();
  virtual ~Merger();

  // Create an object for merging record arrays.
  //
  // On success, returns a poitner to the merger.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  static unique_ptr<Merger> create(
      Error *error,
      const MergerOptions &options = MergerOptions());

  // Set the target record sets.
  //
  // Aborts merging the old record sets and starts merging the new record sets.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool reset(Error *error,
                     Array<Record> *input_records_1,
                     Array<Record> *input_records_2,
                     Array<Record> *output_records) = 0;

  // Progress merging.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool progress(Error *error) = 0;

  // Finish merging.
  //
  // Assumes that all the records are ready.
  // Leaves only the result records if offset and limit are specified.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool finish(Error *error) = 0;

  // Merge records.
  //
  // Calls reset() and finish() to merge records.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool merge(Error *error,
                     Array<Record> *input_records_1,
                     Array<Record> *input_records_2,
                     Array<Record> *output_records) = 0;
};

}  // namespace grnxx

#endif  // GRNXX_MERGER_HPP
