#ifndef GRNXX_CURSOR_HPP
#define GRNXX_CURSOR_HPP

#include "grnxx/array.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Cursor {
 public:
  virtual ~Cursor() {}

  // Return the associated table.
  const Table *table() const {
    return table_;
  }

  // Read the next records.
  //
  // Reads at most "max_count" records and stores the records into "*records".
  //
  // On success, returns the number of records read.
  // On failure, returns -1 and stores error information into "*error" if
  // "error" != nullptr.
  virtual Int read(Error *error,
                   Int max_count,
                   Array<Record> *records) = 0;

  // Read all the remaining records.
  //
  // On success, returns the number of records read.
  // On failure, returns -1 and stores error information into "*error" if
  // "error" != nullptr.
  virtual Int read_all(Error *error, Array<Record> *records) {
    return read(error, numeric_limits<Int>::max(), records);
  }

 protected:
  const Table *table_;

  explicit Cursor(const Table *table) : table_(table) {}
};

}  // namespace grnxx

#endif  // GRNXX_CURSOR_HPP
