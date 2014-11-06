#ifndef GRNXX_CURSOR_HPP
#define GRNXX_CURSOR_HPP

#include <cstddef>
#include <limits>

#include "grnxx/array.hpp"
#include "grnxx/data_types.hpp"

namespace grnxx {

enum CursorOrderType {
  // The natural order (the ascending order in most cases).
  CURSOR_REGULAR_ORDER,
  // The reverse order (the descending order in most cases).
  CURSOR_REVERSE_ORDER
};

struct CursorOptions {
  // The first "offset" records are skipped.
  size_t offset;

  // At most "limit" records are read.
  size_t limit;

  // The order of records.
  CursorOrderType order_type;

  CursorOptions()
      : offset(0),
        limit(std::numeric_limits<size_t>::max()),
        order_type(CURSOR_REGULAR_ORDER) {}
};

class Cursor {
 public:
  Cursor() = default;
  virtual ~Cursor() = default;

  // Read the next records.
  //
  // Reads at most "max_count" records into "*records".
  //
  // On success, returns the number of records read.
  // On failure, throws an exception.
  virtual size_t read(size_t max_count, Array<Record> *records);

  // Read the next records.
  //
  // Reads at most "records.size()" records into "records".
  //
  // On success, returns the number of records read.
  // On failure, throws an exception.
  virtual size_t read(ArrayRef<Record> records) = 0;

  // Read all the remaining records.
  //
  // Reads records into "*records".
  //
  // On success, returns the number of records read.
  // On failure, throws an exception.
  virtual size_t read_all(Array<Record> *records);
};

}  // namespace grnxx

#endif  // GRNXX_CURSOR_HPP
