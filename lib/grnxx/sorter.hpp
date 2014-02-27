#ifndef GRNXX_SORTER_HPP
#define GRNXX_SORTER_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class Sorter {
 public:
  // 整列器を初期化する．
  Sorter() {}
  // 整列器を破棄する．
  virtual ~Sorter() {}

  // 与えられた行の一覧を整列する．
  // 整列の結果が保証されるのは [offset, offset + limit) の範囲に限定される．
  virtual void sort(RowID *row_ids, Int64 num_row_ids,
                    Int64 offset = 0, Int64 limit = INT64_MAX) = 0;
};

class SorterHelper {
 public:
  // 演算器を作成する．
  static Sorter *create(const Table *table, const String &query);
};

}  // namespace grnxx

#endif  // GRNXX_SORTER_HPP
