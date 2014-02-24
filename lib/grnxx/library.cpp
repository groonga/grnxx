#include "grnxx/library.hpp"

#include "grnxx/version.h"

namespace grnxx {

const char *Library::version() {
  return GRNXX_VERSION;
}

}  // namespace grnxx
