#ifndef GRNXX_MERGER_HPP
#define GRNXX_MERGER_HPP

#include "grnxx/types.hpp"

namespace grnxx {

enum MergeStatus {
  MERGE_CONTINUE,
  MERGE_FINISH
};

class Merger {
 public:
  Merger();
  virtual ~Merger();

  // 所属するテーブルを取得する．
  virtual Table *table() const = 0;

  // 行の一覧を合成する．
  // 成功すれば出力された行数を返す．
  // 失敗したときは *error にその内容を格納し， -1 を返す．
  //
  // 入力がまだ残っているときは MERGE_CONTINUE,
  // 入力がもう残っていないときは MERGE_FINISH を指定する．
  //
  // lhs_row_set, rhs_row_set を入力として，
  // 合成した結果を result_row_set に出力する．
  // 合成に使用された行は lhs_row_set, rhs_row_set から取り除かれる．
  // そのため，空になった方の入力に行を補充することで合成を継続できる．
  //
  // 入力は行 ID 昇順もしくは降順になっているものとする．
  // また，入力はそれぞれ重複を含まないものとする．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - スコアが合成によって不正な値になる．
  // - リソースが確保できない．
  virtual int64_t merge(Error *error,
                        RowSet *lhs_row_set,
                        RowSet *rhs_row_set,
                        RowSet *result_row_set,
                        MergeStatus status) const = 0;
};

}  // namespace grnxx

#endif  // GRNXX_MERGER_HPP
