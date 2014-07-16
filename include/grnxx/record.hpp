#ifndef GRNXX_RECORD_HPP
#define GRNXX_RECORD_HPP

#include <vector>

#include "grnxx/types.hpp"

namespace grnxx {

struct Record {
  Int row_id;
  Float score;

  Record() = default;
  Record(Int row_id, Float score) : row_id(row_id), score(score) {}
};

// TODO: RecordSet should be reimplemented not to use std::vector?
class RecordSet {
 public:
  RecordSet() : records_() {}
  ~RecordSet() {}

  // Return the number of records.
  Int size() const {
    return static_cast<Int>(records_.size());
  }

  // Append a record.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool append(Error *error, const Record &record) {
    try {
      records_.push_back(record);
      return true;
    } catch (...) {
      report_error(error);
      return false;
    }
  }

  // Return the record identified by "i".
  //
  // If "i" is invalid, the result is undefined.
  Record get(Int i) const {
    return records_[i];
  }

  // Set a record.
  //
  // If "i" is invalid, the result is undefined.
  void set(Int i, Record record) {
    records_[i] = record;
  }

  // Resize the set.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool resize(Error *error, Int size) {
    try {
      records_.resize(size);
      return true;
    } catch (...) {
      report_error(error);
      return false;
    }
  }

  // Clear all the records.
  void clear() {
    records_.clear();
  }

 private:
  std::vector<Record> records_;

  // Report a no memory error.
  void report_error(Error *error);
};

}  // namespace grnxx

#endif  // GRNXX_RECORD_HPP
