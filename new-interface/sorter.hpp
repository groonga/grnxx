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

  // TODO: 条件を一気に設定するようにした方が使いやすい可能性がある．
  //       その場合， Table::create_sorter() で指定することも考慮すべきである．

  // TODO: reverse_order を bool で受け取るよりは，
  //       enum で受け取る方がわかりやすい．

  // 前提条件を追加する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 何らかの条件にしたがって整列済みのときに指定する．
  // 新しい条件は末尾に追加される．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 式の評価結果が大小関係を持たない型になる．
  // - リソースを確保できない．
  virtual bool add_precondition(const Expression *expression,
                                bool reverse_order,
                                Error *error) const = 0;

  // 整列条件を追加する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 新しい条件は末尾に追加されるため，
  // 優先順位の高い整列条件から順に追加しなければならない．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 式の評価結果が大小関係を持たない型になる．
  // - リソースを確保できない．
  virtual bool add_condition(const Expression *expression,
                             bool reverse_order,
                             Error *error) const = 0;

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
  virtual bool sort(int64_t num_row_ids, RowID *row_ids,
                    int64_t offset, int64_t limit,
                    Error *error);
};

}  // namespace grnxx

#endif  // GRNXX_SORTER_HPP
