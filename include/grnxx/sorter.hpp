#ifndef GRNXX_SORTER_HPP
#define GRNXX_SORTER_HPP

#include "grnxx/types.hpp"

namespace grnxx {
namespace sorter {

class Node;

}  // namespace sorter

class Sorter {
 public:
  using Node = sorter::Node;

  ~Sorter();

  // Return the associated table.
  const Table *table() const {
    return table_;
  }

  // Create an object for sorting records.
  //
  // On success, returns a poitner to the sorter.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  static unique_ptr<Sorter> create(
      Error *error,
      Array<SortOrder> &&orders,
      const SorterOptions &options = SorterOptions());

  // Set the target record set.
  //
  // Aborts sorting the old record set and starts sorting the new record set.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool reset(Error *error, Array<Record> *records);

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
  bool sort(Error *error, Array<Record> *records);

 private:
  const Table *table_;
  unique_ptr<Node> head_;
  Array<Record> *records_;
  Int offset_;
  Int limit_;

  Sorter();
};

}  // namespace grnxx

#endif  // GRNXX_SORTER_HPP
