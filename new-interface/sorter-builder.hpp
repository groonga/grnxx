#ifndef GRNXX_SORTER_HPP
#define GRNXX_SORTER_HPP

#include "grnxx/types.hpp"

namespace grnxx {

enum SortOrder {
  ASCENDING_ORDER,
  DESCENDING_ORDER
};

class Sorter {
 public:
  Sorter();
  virtual ~Sorter();

  // 所属するテーブルを取得する．
  virtual Table *table() const = 0;

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
  virtual bool add_precondition(Error *error,
                                const Expression &expression,
                                SortOrder order) const = 0;

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
  virtual bool add_condition(Error *error,
                             const Expression &expression,
                             SortOrder order) const = 0;

  // すべての条件を破棄する．
  virtual void clear();

  // 指定された条件に対応する整列器を作成する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - リソースを確保できない．
  virtual std::unique_ptr<Sorter> create_sorter(
      Error *error,
      const SorterOptions &options) const;
};

}  // namespace grnxx

#endif  // GRNXX_SORTER_HPP
