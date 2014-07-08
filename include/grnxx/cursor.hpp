#ifndef GRNXX_CURSOR_HPP
#define GRNXX_CURSOR_HPP

#include "grnxx/types.hpp"

namespace grnxx {

struct CursorOptions {
  // The first "offset" records are skipped.
  Int offset;

  // At most "limit" records are read.
  Int limit;

  // The order of records.
  OrderType order_type;

  // Initialize the options.
  CursorOptions();
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
  // Reads at most "max_count" records and stores the records into
  // "*record_set".
  //
  // On success, returns the number of records read on success.
  // On failure, returns -1 and stores error information into "*error" if
  // "error" != nullptr.
  virtual Int read(Error *error,
                   Int max_count,
                   RecordSet *record_set) = 0;

 protected:
  const Table *table_;

  explicit Cursor(const Table *table) : table_(table) {}
};

}  // namespace grnxx

#endif  // GRNXX_CURSOR_HPP
