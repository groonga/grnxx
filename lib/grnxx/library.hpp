#ifndef GRNXX_LIBRARY_HPP
#define GRNXX_LIBRARY_HPP

namespace grnxx {

// ライブラリ．
class Library {
 public:
  // ライブラリの名前を返す．
  static const char *name();

  // ライブラリのバージョンを返す．
  static const char *version();
};

}  // namespace grnxx

#endif  // GRNXX_LIBRARY_HPP
