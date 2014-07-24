#include "grnxx/record.hpp"

#include "grnxx/error.hpp"

namespace grnxx {

void RecordSet::report_error(Error *error) {
  GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
}

}  // namespace
