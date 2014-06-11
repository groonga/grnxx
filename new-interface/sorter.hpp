#ifndef GRNXX_SORTER_HPP
#define GRNXX_SORTER_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class Sorter {
 public:
  Sorter();
  virtual ~Sorter();

  // 所属するテーブルを取得する．
  virtual Table *table() const = 0;

  // 行の一覧を整列する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 整列の結果が保証されるのは [offset, offset + limit) の範囲である．
  // なお，行 ID を整列条件に加えれば安定な整列になる．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 演算において例外が発生する．
  // - リソースを確保できない．
  virtual bool sort(Error *error,
                    RowSet *row_set,
                    int64_t offset,
                    int64_t limit);
};

}  // namespace grnxx

#endif  // GRNXX_SORTER_HPP
