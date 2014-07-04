#ifndef GRNXX_RECORD_HPP
#define GRNXX_RECORD_HPP

#include "grnxx/types.hpp"

namespace grnxx {

struct Record {
  Int row_id;
  Float score;
};

class RecordSet {
 public:
  RecordSet();
  ~RecordSet();

  // Return the associated table.
  Table *table() const {
    return table_;
  }
  // Return the number of records.
  size_t num_records() const {
    return size_;
  }

  // Return the record identified by "i".
  //
  // The result is undefined if "i" is invalid.
  Record get(size_t i) const {
    return records_[i];
  }

 private:
  Table table_;
  size_t size_;
  size_t capacity_;
  unique_ptr<Record[]> records_;
};

}  // namespace grnxx

#endif  // GRNXX_RECORD_HPP
