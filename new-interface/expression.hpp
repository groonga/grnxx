#ifndef GRNXX_EXPRESSION_HPP
#define GRNXX_EXPRESSION_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class ExpressionNode;

class Expression {
 public:
  Expression();
  virtual ~Expression();

  // 評価結果の型を取得する．
  virtual DataType data_type() const = 0;

  // TODO: 実際の使い方に合わせて修正する．
  //
  // 行の一覧に対する評価結果を取得する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 演算において例外が発生する．
  //  - オーバーフローやアンダーフローが発生する．
  //  - ゼロによる除算が発生する．
  //  - NaN が発生する．
  //   - TODO: これらの取り扱いについては検討の余地がある．
  // - リソースが確保できない．
  virtual bool evaluate(Error *error,
                        const RecordSet &record_set,
                        Data *values) = 0;
};

}  // namespace grnxx

#endif  // GRNXX_EXPRESSION_HPP
