#ifndef GRNXX_CURSOR_HPP
#define GRNXX_CURSOR_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class Cursor {
 public:
  Cursor();
  virtual ~Cursor();

  // カーソルの位置を進めて移動量を返す．
  // 途中で終端に到達したときは，そこで移動を停止し，それまでの移動量を返す．
  virtual int64_t seek(int64_t count) = 0;

  // カーソルの位置を進めて移動量を返す．
  // 途中で終端に到達したときは，そこで移動を停止し，それまでの移動量を返す．
  // 移動中に取得した行 ID は *row_ids に格納する．
  virtual int64_t read(int64_t count, RowID *row_ids) = 0;
};

}  // namespace grnxx

#endif  // GRNXX_CURSOR_HPP
