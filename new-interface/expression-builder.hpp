#ifndef GRNXX_EXPRESSION_BUILDER_HPP
#define GRNXX_EXPRESSION_BUILDER_HPP

#include "grnxx/types.hpp"

namespace grnxx {

// 後置記法（逆ポーランド記法）に基づいて式のオブジェクトを構築する．
class ExpressionBuilder {
 public:
  ExpressionBuilder();
  virtual ~ExpressionBuilder();

  // 所属するテーブルを取得する．
  virtual Table *table() const = 0;
  // 評価結果の型を取得する．
  virtual DataType data_type() const = 0;

  // 定数をスタックに積む．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された定数が不正である．
  // - リソースを確保できない．
  virtual bool push_datum(Error *error, const Datum &datum) = 0;

  // カラムをスタックに積む．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // column_name に "_id" を指定すれば行 ID に対応する擬似カラム，
  // "_score" を指定すればスコアに対応する擬似カラムをスタックに積む．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定されたカラムが存在しない．
  // - リソースを確保できない．
  virtual bool push_column(Error *error, const char *column_name) = 0;

  // 演算子に対応する被演算子をスタックから降ろし，演算子をスタックに積む．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 被演算子はあらかじめスタックに積んでおく必要があり，
  // 被演算子の型や数に問題があるときは失敗する．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 演算子と被演算子が対応していない．
  //  - 被演算子がスタックに存在しない．
  //  - 演算子が求める引数の型と被演算子の型が異なる．
  // - リソースを確保できない．
  virtual bool push_operator(Error *error, OperatorType operator_type) = 0;

  // 保持しているノードやスタックを破棄する．
  virtual void clear() = 0;

  // 構築中の式を完成させ，その所有権を取得する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 所有権を返すため，保持しているノードやスタックは破棄する．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - スタックの要素数が一つでない．
  //  - 何も積まれていない．
  //  - 積まれたものが使われずに残っている．
  //   - 式が完成していないことを示す．
  // - リソースを確保できない．
  virtual std::unique_ptr<Expression> release(
      Error *error,
      const ExpressionOptions &options) = 0;
};

}  // namespace grnxx

#endif  // GRNXX_EXPRESSION_BUILDER_HPP
