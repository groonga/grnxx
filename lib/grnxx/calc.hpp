#ifndef GRNXX_CALC_HPP
#define GRNXX_CALC_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class Calc {
 public:
  // 演算器を初期化する．
  Calc() {}
  // 演算器を破棄する．
  virtual ~Calc() {}

  // 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
  virtual Int64 filter(RowID *row_ids, Int64 num_row_ids) = 0;

  // 条件がひとつでもあれば true を返し，そうでなければ false を返す．
  virtual bool empty() const = 0;
};

class CalcHelper {
 public:
  // 演算器を作成する．
  static Calc *create(const Table *table, const String &query);
};

}  // namespace grnxx

#endif  // GRNXX_CALC_HPP
