#ifndef GRNXX_SORTER_HPP
#define GRNXX_SORTER_HPP

#include "grnxx/types.hpp"

namespace grnxx {

enum SortOrder {
  ASCENDING_ORDER,
  DESCENDING_ORDER
};

struct SorterOptions {
  // 整列の結果が保証されるのは [offset, offset + limit) の範囲である．
  // なお，行 ID を整列条件に加えれば安定な整列になる．
  int64_t offset;
  int64_t limit;

  SorterOptions();
};

class Sorter {
 public:
  Sorter();
  virtual ~Sorter();

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
                                std::unique_ptr<Expression> &&expression,
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
                             std::unique_ptr<Expression> &&expression,
                             SortOrder order) const = 0;

  // すべての条件を破棄する．
  virtual void clear();

  // 構築中の整列器を完成させ，その所有権を取得する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 所有権を返すため，保持している条件などは破棄する．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 整列条件が何も指定されていない．
  // - オプションが不正である．
  // - リソースを確保できない．
  virtual std::unique_ptr<Sorter> release(
      Error *error,
      const SorterOptions &options) const = 0;
};

}  // namespace grnxx

#endif  // GRNXX_SORTER_HPP
