#ifndef GRNXX_INDEX_HPP
#define GRNXX_INDEX_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class Index {
 public:
  Index();
  virtual ~Index();

  // 所属するカラムを取得する．
  virtual Column *column() const = 0;
  // 名前を取得する．
  virtual const std::string &name() const = 0;
  // 種類を取得する．
  virtual IndexType type() const = 0;

  // 指定された条件を持たす行の ID を取得するためのカーソルを作成する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 返り値は std::unique_ptr なので自動的に delete される．
  // 自動で delete されて困るときは release() で生のポインタを取り出す必要がある．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - オプションが不正である．
  // - リソースが確保できない．
  virtual std::unique_ptr<Cursor> create_cursor(
      const CursorOptions &options) const = 0;
};

}  // namespace grnxx

#endif  // GRNXX_INDEX_HPP
