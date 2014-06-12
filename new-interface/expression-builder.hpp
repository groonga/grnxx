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
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 作成されたノードはスタックに積まれる．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された定数が不正である．
  // - リソースを確保できない．
  virtual bool create_datum_node(Error *error,
                                 const Datum &datum) = 0;

  // カラムに対応するノードを作成する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // column_name に "_id" を指定すれば行 ID に対応するノード，
  // "_score" を指定すればスコアに対応するノードを作成する．
  //
  // 作成されたノードはスタックに積まれる．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定されたカラムが存在しない．
  // - リソースを確保できない．
  virtual bool create_column_node(Error *error,
                                  const char *column_name) = 0;

  // 演算子に対応するノードを作成する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // スタックに積まれているノードを降ろして被演算子とするため，
  // 被演算子を作成した後に演算子を作成しなければならない．
  // これは後置式（逆ポーランド記法）の考え方にもとづく．
  //
  // 作成されたノードはスタックに積まれる．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 引数がスタックに存在しない．
  // - 演算子と引数が対応していない．
  //  - 演算子が求める引数の型と実際の引数の型が異なる．
  // - リソースを確保できない．
  virtual bool create_operator_node(
      Error *error,
      OperatorType operator_type) = 0;

  // 保持しているノードやスタックを破棄する．
  virtual void clear();

  // 構築された式を取得する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 保持しているノードやスタックは破棄される．
  //
  // TODO: 内部で作成した Expression の所有権を返すだけにしたい．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - ノードがひとつもない．
  // - スタックに二つ以上のノードが存在する．
  //  - 式が完成していないことを示す．
  // - リソースを確保できない．
  virtual std::unique_ptr<Expression> release(Error *error);
};

}  // namespace grnxx

#endif  // GRNXX_EXPRESSION_BUILDER_HPP
