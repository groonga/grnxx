#include "grnxx/array.hpp"

#include "grnxx/error.hpp"

namespace grnxx {

void ArrayHelper::report_memory_error(Error *error) {
  GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
}

void ArrayHelper::report_empty_error(Error *error) {
  GRNXX_ERROR_SET(error, INVALID_OPERATION, "Empty vector");
}

}  // namespace grnxx
