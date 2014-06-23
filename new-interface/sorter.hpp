#ifndef GRNXX_SORTER_HPP
#define GRNXX_SORTER_HPP

#include "grnxx/types.hpp"

namespace grnxx {

struct SorterOptions {
  // 整列結果から上位 offset 件を破棄する．
  // 0 以上でなければならない．
  int64_t offset;
  // 整列結果から最大 limit 件を取得する．
  // 1 以上でなければならない．
  int64_t limit;

  SorterOptions();
};

class Sorter {
 public:
  Sorter();
  virtual ~Sorter();

  // 整列器を作成する．
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
  static std::unique_ptr<Sorter> create(
      Error *error,
      std::unique_ptr<Order> &&order,
      const SorterOptions &options);

  // 整列の対象となるレコードの一覧を設定する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 整列の途中で呼び出したときは，途中経過を破棄して新たな整列を開始する．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 不正なレコードの一覧が指定された．
  virtual bool reset(Error *error, RecordSet *record_set) = 0;

  // 整列を進める．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // offset, limit の指定がないなど，入力がすべて揃ってからでなければ
  // 整列に取り掛かれないときは何もせずに成功する．
  //
  // 整列済みの範囲以外を新たな入力として整列を進める．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 対象が設定されていない．
  // - 整列が既に完了している．
  // - 演算で例外が発生する．
  virtual bool progress(Error *error) = 0;

  // 整列の仕上げをおこなう．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 入力がすべて揃ったものとして整列の仕上げをおこなう．
  // offset, limit の指定があるときは，有効な範囲だけが残る．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 対象が設定されていない．
  // - 整列が既に完了している．
  // - 演算で例外が発生する．
  // - リソースを確保できない．
  virtual bool finish(Error *error) = 0;

  // レコードの一覧を整列する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // reset(), finish() を呼び出すことで整列をおこなう．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 不正なレコードの一覧が指定された．
  // - 演算で例外が発生する．
  // - リソースを確保できない．
  virtual bool sort(Error *error, RecordSet *record_set) = 0;
};

}  // namespace grnxx

#endif  // GRNXX_SORTER_HPP
