#ifndef GRNXX_LIBRARY_HPP
#define GRNXX_LIBRARY_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class Library {
 public:
  // ライブラリの名前を返す．
  static const char *package();
  // ライブラリのバージョンを返す．
  static const char *version();
};

}  // namespace grnxx

#endif  // GRNXX_LIBRARY_HPP
