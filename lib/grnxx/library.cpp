#include "grnxx/library.hpp"

#include "../config.h"
#include "grnxx/version.h"

namespace grnxx {

// ライブラリの名前を返す．
const char *Library::name() {
  return PACKAGE;
}

// ライブラリのバージョンを返す．
const char *Library::version() {
  return GRNXX_VERSION;
}

}  // namespace grnxx
