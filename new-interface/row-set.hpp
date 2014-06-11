#ifndef GRNXX_ROW_SET_HPP
#define GRNXX_ROW_SET_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class RowSet {
 public:
  RowSet();
  ~RowSet();

  // 所属するテーブルを取得する．
  Table *table() const;
  // 行数を取得する．
  int64_t num_rows() const;

  // 行 ID を取得する．
  RowID get_row_id(int64_t i) const;
  // スコアを取得する．
  double get_score(int64_t i) const;

  // スコアを正規化する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // TODO: 具体的な正規化方法を決める．
  //       最大値ですべてのスコアを除算するというのが簡単そうである．
  //       正規化後の最大値を指定できると便利かもしれない．
  //       ほかにも何か必要な正規化があるかどうか．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 最大値が 0.0, negative, infinity である．
  bool normalize(Error *error, const NormalizeOptions &options);

  // TODO: Sorter を使うより RowSet::sort() の方が良い？

  // 整列する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // TODO: 整列条件の指定方法を決める．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - リソースが足りない．
  // - 演算において例外が発生する．
  bool sort(Error *error, const SortConditions &conditions);

  // TODO: Grouper を使うより RowSet::group() の方が良い？

  // グループ化する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - オプションが不正である．
  // - リソースが確保できない．
  bool group(Error *error,
             GroupSet *group_set,
             const Expression &expression,
             GroupOptions &options) const;
};

}  // namespace grnxx

#endif  // GRNXX_ROW_SET_HPP
