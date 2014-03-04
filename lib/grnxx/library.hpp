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

  // 可変長整数型が有効であれば true を返し，そうでなければ false を返す．
  static bool enable_varint();
};

}  // namespace grnxx

#endif  // GRNXX_LIBRARY_HPP
