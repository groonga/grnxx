#ifndef GRNXX_RECORD_HPP
#define GRNXX_RECORD_HPP

#include "grnxx/array.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

struct Record {
  Int row_id;
  Float score;

  Record() = default;
  Record(Int row_id, Float score) : row_id(row_id), score(score) {}
};

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
    return records_.push_back(error, record);
  }

  // If "i" is invalid, the result of the following functions is undefined.

  // Return the "i"-th record.
  Record get(Int i) const {
    return records_[i];
  }
  // Return the row ID of the "i"-th record.
  Int get_row_id(Int i) const {
    return records_[i].row_id;
  }
  // Return the score of the "i"-th record.
  Float get_score(Int i) const {
    return records_[i].score;
  }

  // Set the "i"-th record.
  void set(Int i, Record record) {
    records_[i] = record;
  }
  // Set the row ID of the "i"-th record.
  void set_row_id(Int i, Int row_id) {
    records_[i].row_id = row_id;
  }
  // Set the score of the "i"-th record.
  void set_score(Int i, Float score) {
    records_[i].score = score;
  }

  // Resize the set.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool resize(Error *error, Int size) {
    return records_.resize(error, size);
  }

  // Clear all the records.
  void clear() {
    records_.clear();
  }

 private:
  Array<Record> records_;
};

}  // namespace grnxx

#endif  // GRNXX_RECORD_HPP
