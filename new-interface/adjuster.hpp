#ifndef GRNXX_ADJUSTER_HPP
#define GRNXX_ADJUSTER_HPP

#include "grnxx/types.hpp"

namespace grnxx {

struct AdjusterOptions {
  AdjusterOptions();
};

class Adjuster {
 public:
  Adjuster();
  virtual ~Adjuster();

  // スコアの調整器を作成する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 返り値は std::unique_ptr なので自動的に delete される．
  // 自動で delete されて困るときは release() で生のポインタを取り出す必要がある．
  //
  // 新しいスコアは expression により求められる．
  // 式の構築において _score を指定すれば，古いスコアを入力として使える．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された式の評価結果が真偽値ではない．
  // - オプションが不正である．
  // - リソースが確保できない．
  static std::unique_ptr<Adjuster> create(
      Error *error,
      std::unique_ptr<Expression> &&expression,
      const AdjusterOptions &options);

  // レコード一覧のスコアを調整する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // レコード一覧のスコアを新しいスコアに置き換える．
  // 新しいスコアをレコード一覧に保存する．
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
                      RecordSet *record_set) = 0;
};

}  // namespace grnxx

#endif  // GRNXX_ADJUSTER_HPP
