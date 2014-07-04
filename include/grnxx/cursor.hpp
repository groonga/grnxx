#ifndef GRNXX_CURSOR_HPP
#define GRNXX_CURSOR_HPP

#include "grnxx/types.hpp"

namespace grnxx {

struct CursorOptions {
};

class Cursor {
 public:
  Cursor() {}
  virtual ~Cursor() {}

  // Skip at most the next "count" records.
  //
  // Returns the number of skipped records on success.
  // On failure, returns -1 and stores error information into "*error" if
  // "error" != nullptr.
  virtual size_t seek(Error *error, size_t count) = 0;

  // Read at most the next "count" records.
  //
  // Records are stored into "*record_set". 
  //
  // Returns the number of read records on success.
  // On failure, returns -1 and stores error information into "*error" if
  // "error" != nullptr.
  virtual size_t read(Error *error, size_t count, RecordSet *record_set) = 0;
};

}  // namespace grnxx

#endif  // GRNXX_CURSOR_HPP
