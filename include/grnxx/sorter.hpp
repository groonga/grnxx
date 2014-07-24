#ifndef GRNXX_SORTER_HPP
#define GRNXX_SORTER_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class Sorter {
 public:
  ~Sorter();

  // Create an object for sorting records.
  //
  // On success, returns a poitner to the sorter.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  static unique_ptr<Sorter> create(
      Error *error,
      unique_ptr<OrderSet> &&order_set,
      const SorterOptions &options);

  // Set the target record set.
  //
  // Aborts sorting the old record set and starts sorting the new record set.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool reset(Error *error, RecordSet *record_set);

  // Progress sorting.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool progress(Error *error);

  // Finish sorting.
  //
  // Assumes that all the records are ready.
  // Leaves only the result records if offset and limit are specified.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool finish(Error *error);

  // Sort records.
  //
  // Calls reset() and finish() to sort records.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool sort(Error *error, RecordSet *record_set);

 private:
  RecordSet *record_set_;

  Sorter();
};

}  // namespace grnxx

#endif  // GRNXX_SORTER_HPP
