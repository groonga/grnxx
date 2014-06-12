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

  // 行の一覧をフィルタにかける．
  // 成功すればフィルタにかけて残った行数を返す．
  // 失敗したときは *error にその内容を格納し， -1 を返す．
  //
  // 評価結果が真になる行のみを残し，前方に詰めて隙間をなくす．
  // フィルタにかける前後で順序関係は維持される．
  //
  // 先頭の offset 件はそのままにする．
  // 返り値はフィルタをかけて残った行数から offset を引いたものになる．
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
  virtual int64_t filter(Error *error,
                         RecordSet *record_set,
                         int64_t offset) = 0;

  // スコアを調整する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 評価結果を *record_set に格納する．
  // 式の構築において _score を指定することにより，
  // 既存のスコアを入力として使うこともできる．
  //
  // 先頭の offset 件はそのままにする．
  //
  // 有効でない行 ID を渡したときの動作は未定義である．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 評価結果をスコアに変換できない．
  // - 演算において例外が発生する．
  //  - オーバーフローやアンダーフローが発生する．
  //  - ゼロによる除算が発生する．
  //  - NaN が発生する．
  //   - TODO: これらの取り扱いについては検討の余地がある．
  virtual bool adjust(Error *error,
                      RecordSet *record_set,
                      int64_t offset) = 0;

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
                        int64_t offset,
                        int64_t limit,
                        Data *values) = 0;
};

}  // namespace grnxx

#endif  // GRNXX_EXPRESSION_HPP
