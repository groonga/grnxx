#ifndef GRNXX_CURSOR_HPP
#define GRNXX_CURSOR_HPP

#include "grnxx/types.hpp"

namespace grnxx {

struct CursorResult {
  bool is_ok;
  Int count;
};

class Cursor {
 public:
  virtual ~Cursor() {}

  // Return the associated table.
  const Table *table() const {
    return table_;
  }

  // Read the next records.
  //
  // Reads at most "max_count" records into "*records".
  //
  // On success, returns true and the number of records read.
  // On failure, returns false and the number of records read and stores error
  // information into "*error" if "error" != nullptr.
  virtual CursorResult read(Error *error,
                            Int max_count,
                            Array<Record> *records);

  // Read the next records.
  //
  // Reads at most "records.size()" records into "records".
  //
  // On success, returns true and the number of records read.
  // On failure, returns false and the number of records read and stores error
  // information into "*error" if "error" != nullptr.
  virtual CursorResult read(Error *error,
                            ArrayRef<Record> records) = 0;

  // Read all the remaining records.
  //
  // Reads records into "*records".
  //
  // On success, returns true and the number of records read.
  // On failure, returns false and the number of records read and stores error
  // information into "*error" if "error" != nullptr.
  virtual CursorResult read_all(Error *error,
                                Array<Record> *records);

 protected:
  const Table *table_;

  explicit Cursor(const Table *table) : table_(table) {}
};

}  // namespace grnxx

#endif  // GRNXX_CURSOR_HPP
