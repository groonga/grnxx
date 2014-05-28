#ifndef GRNXX_EXPRESSION_HPP
#define GRNXX_EXPRESSION_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class ExpressionNode;

class Expression {
 public:
  Expression();
  virtual ~Expression();

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

  // 行の一覧をフィルタにかける．
  // 成功すればフィルタにかけて残った行数を返す．
  // 失敗したときは *error にその内容を格納し， -1 を返す．
  //
  // 評価結果が真になる行のみを残し，前方に詰めて隙間をなくす．
  // フィルタにかける前後で順序関係は維持される．
  //
  // 有効でない行 ID を渡したときの動作は未定義である．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 評価結果の型が真偽値でない．
  // - 演算において例外が発生する．
  //  - オーバーフローやアンダーフローが発生する．
  //  - ゼロによる除算が発生する．
  //  - NaN が発生する．
  //   - TODO: これらの取り扱いについては検討の余地がある．
  virtual int64_t filter(int64_t num_row_ids,
                         RowID *row_ids,
                         Error *error) = 0;

  // 行の一覧に対する評価結果を取得する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // TODO: 汎用型を配列にするのは効率が悪いため，ほかの方法が望ましい．
  //       専用の型を用意することも含めて検討したい．
  //
  // TODO: 整列などにも使うことを考えれば，ネイティブな型を返すインタフェースが欲しい．
  //       少なくとも内部インタフェースとして用意する必要がある．
  //
  // 有効でない行 ID を渡したときの動作は未定義である．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 演算において例外が発生する．
  //  - オーバーフローやアンダーフローが発生する．
  //  - ゼロによる除算が発生する．
  //  - NaN が発生する．
  //   - TODO: これらの取り扱いについては検討の余地がある．
  virtual bool evaluate(int64_t num_row_ids,
                        const RowID *row_ids,
                        Datum *values,
                        Error *error) = 0;
};

}  // namespace grnxx

#endif  // GRNXX_EXPRESSION_HPP
