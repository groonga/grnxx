#ifndef GRNXX_MERGER_HPP
#define GRNXX_MERGER_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class Merger {
 public:
  Merger();
  virtual ~Merger();

  // 合成の入出力となるレコードの一覧を設定する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 合成の途中で呼び出したときは，途中経過を破棄して新たな合成を開始する．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 不正なレコードの一覧が指定された．
  virtual bool reset(Error *error,
                     RecordSet *lhs_record_set,
                     RecordSet *rhs_record_set,
                     RecordSet *result_record_set);

  // 合成を進める．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 入力が行 ID 順でないなど，入力がすべて揃ってからでなければ
  // 合成に取り掛かれないときは何もせずに成功する．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 入出力が設定されていない．
  // - 合成が既に完了している．
  // - 演算で例外が発生する．
  // - 不正なレコードの一覧が指定された．
  virtual bool progress(Error *error) = 0;

  // 合成の仕上げをおこなう．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 入力がすべて揃ったものとして合成の仕上げをおこなう．
  // offset, limit の指定があるときは，有効な範囲だけが残る．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 入出力が設定されていない．
  // - 合成が既に完了している．
  // - 演算で例外が発生する．
  // - リソースを確保できない．
  virtual bool finish(Error *error) = 0;

  // レコードの一覧を合成する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // reset(), finish() を呼び出すことで合成をおこなう．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 不正なレコードの一覧が指定された．
  // - 演算で例外が発生する．
  // - リソースを確保できない．
  virtual bool merge(Error *error,
                     RecordSet *lhs_record_set,
                     RecordSet *rhs_record_set,
                     RecordSet *result_record_set,
};

}  // namespace grnxx

#endif  // GRNXX_MERGER_HPP
