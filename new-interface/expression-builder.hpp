#ifndef GRNXX_EXPRESSION_BUILDER_HPP
#define GRNXX_EXPRESSION_BUILDER_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class ExpressionBuilder {
 public:
  ExpressionBuilder();
  virtual ~ExpressionBuilder();

  // 所属するテーブルを取得する．
  virtual Table *table() const = 0;
  // 評価結果の型を取得する．
  virtual DataType data_type() const = 0;

  // 定数に対応するノードを作成する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された定数が異常値である．
  // - リソースを確保できない．
  virtual ExpressionNode *create_datum_node(const Datum &datum,
                                            Error *error) = 0;

  // カラムに対応するノードを作成する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定されたカラムが存在しない．
  // - リソースを確保できない．
  virtual ExpressionNode *create_column_node(const char *column_name,
                                             Error *error) = 0;

  // 演算子に対応するノードを作成する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 演算子と引数が対応していない．
  //  - 演算子が求める引数の型・数と実際の引数の型・数が異なる．
  // - リソースを確保できない．
  virtual ExpressionNode *create_operator_node(OperatorType operator_type,
                                               int64_t num_args,
                                               ExpressionNode **args,
                                               Error *error) = 0;

  // すべてのノードを破棄する．
  virtual void clear();

  // 最後に作成したノードを根とする構文木に対応する式を作成する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - リソースを確保できない．
  virtual std::unique_ptr<Expression> create_expression(Error *error) const;
};

}  // namespace grnxx

#endif  // GRNXX_EXPRESSION_BUILDER_HPP
