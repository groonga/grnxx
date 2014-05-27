#ifndef GRNXX_ERROR_HPP
#define GRNXX_ERROR_HPP

#include "grnxx/types.hpp"

namespace grnxx {

// 以下の実装では， Error のサイズが大きいため，値渡しには向かない．
// また，再帰呼出しするような関数では，内部で Error を定義するのは危険かもしれない．
// スタック領域を使い切れば致命的なエラーになる．
// その代わり，基本的にはスタック領域を使うため，
// メモリ確保によるオーバーヘッドは発生しないというメリットがある．
//
// Error の中身はポインタだけにし，必要なときにメモリを確保するという実装も考えられる．
// メッセージの長さに制限を持たせる必要がなくなるものの，
// 代わりに Trivial でないコンストラクタとデストラクタが必要になる．
//
// find() の Not found や insert_row() の Key already exists みたいなのにも
// 使うのであれば， message_ の生成がネックになりうる．
class Error {
 public:
  int line() const;
  const char *file() const;
  const char *function() const;
  const char *message() const;

// private:
//  constexpr int MESSAGE_SIZE = 256;

//  int line_;              // __LINE__
//  const char *file_;      // __FILE__
//  const char *function_;  // __FUNCTION__ or __func__
//  char message_[MESSAGE_SIZE];
};

}  // namespace grnxx

#endif  // GRNXX_ERROR_HPP
