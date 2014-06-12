#ifndef GRNXX_CURSOR_HPP
#define GRNXX_CURSOR_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class Cursor {
 public:
  Cursor();
  virtual ~Cursor();

  // カーソルの位置を最大で count 進める．
  // 成功すれば移動量を返す．
  // 失敗したときは *error にその内容を格納し， -1 を返す．
  //
  // 途中で終端に到達したときは count より小さい値を返す．
  virtual int64_t seek(Error *error, int64_t count) = 0;

  // カーソルの位置を最大で count 進める．
  // 成功すれば移動量を返す．
  // 失敗したときは *error にその内容を格納し， -1 を返す．
  //
  // カーソルの移動中に取得した行は *record_set の末尾に追加する．
  //
  // 途中で終端に到達したときは count より小さい値を返す．
  virtual int64_t read(Error *error, int64_t count, RecordSet *record_set) = 0;
};

}  // namespace grnxx

#endif  // GRNXX_CURSOR_HPP
