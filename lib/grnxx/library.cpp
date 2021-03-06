#include "grnxx/library.hpp"

#include "../config.h"
#include "grnxx/version.h"

namespace grnxx {

const char *Library::package() {
  return PACKAGE;
}

const char *Library::version() {
  return GRNXX_VERSION;
}

}  // namespace grnxx
