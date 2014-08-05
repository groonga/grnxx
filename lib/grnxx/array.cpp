#include "grnxx/array.hpp"

#include "grnxx/error.hpp"

namespace grnxx {

void ArrayErrorReporter::report_memory_error(Error *error) {
  GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
}

}  // namespace grnxx
