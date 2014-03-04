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

// 可変長整数型が有効であれば true を返し，そうでなければ false を返す．
bool Library::enable_varint() {
#ifdef GRNXX_ENABLE_VARIABLE_INTEGER_TYPE
  return true;
#else  // GRNXX_ENABLE_VARIABLE_INTEGER_TYPE
  return false;
#endif  // GRNXX_ENABLE_VARIABLE_INTEGER_TYPE
}

}  // namespace grnxx
