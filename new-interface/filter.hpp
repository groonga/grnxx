#ifndef GRNXX_FILTER_HPP
#define GRNXX_FILTER_HPP

#include "grnxx/types.hpp"

namespace grnxx {

struct FilterOptions {
  // フィルタを通過するようなレコードの内，先頭の offset 件は破棄する．
  // 0 以上でなければならない．
  int64_t offset;

  // フィルタを通過したレコードが limit 件に到達した時点で残りの入力は破棄する．
  // 1 以上でなければならない．
  int64_t limit;

  FilterOptions();
};

class Filter {
 public:
  Filter();
  virtual ~Filter();

  // フィルタを作成する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 返り値は std::unique_ptr なので自動的に delete される．
  // 自動で delete されて困るときは release() で生のポインタを取り出す必要がある．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された式の評価結果が真偽値ではない．
  // - オプションが不正である．
  // - リソースが確保できない．
  static std::unique_ptr<Filter> create(
      Error *error,
      std::unique_ptr<Expression> &&expression,
      const FilterOptions &options);

  // レコードの一覧をフィルタにかける．
  // 成功すればフィルタにかけて残ったレコード数を返す．
  // 失敗したときは *error にその内容を格納し， -1 を返す．
  //
  // 評価結果が真になるレコードのみを残し，前方に詰めて隙間をなくす．
  // フィルタにかける前後で順序関係は維持される．
  //
  // 有効でない行 ID を渡したときの動作は未定義である．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 演算において例外が発生する．
  //  - オーバーフローやアンダーフローが発生する．
  //  - ゼロによる除算が発生する．
  //  - NaN が発生する．
  //   - TODO: これらの取り扱いについては検討の余地がある．
  virtual int64_t filter(Error *error,
                         RecordSet *record_set) = 0;
};

}  // namespace grnxx

#endif  // GRNXX_FILTER_HPP
