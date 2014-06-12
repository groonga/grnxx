#ifndef GRNXX_RECORD_SET_HPP
#define GRNXX_RECORD_SET_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class RecordSet {
 public:
  RecordSet();
  ~RecordSet();

  // 所属するテーブルを取得する．
  Table *table() const;
  // レコード ID の最小値を取得する．
  RecordID min_record_id() const;
  // レコード ID の最大値を取得する．
  RecordID max_record_id() const;

  // 行 ID を取得する．
  RowID get_row_id(RecordID record_id) const;
  // スコアを取得する．
  double get_score(RecordID record_id) const;

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
  // - 最大値が 0.0, negative, infinity のいずれかである．
  bool normalize(Error *error, const NormalizeOptions &options);

  // TODO: Sorter を使うより RecordSet::sort() の方が良い？

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

  // TODO: Grouper を使うより RecordSet::group() の方が良い？

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

#endif  // GRNXX_RECORD_SET_HPP
