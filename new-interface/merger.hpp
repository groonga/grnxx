#ifndef GRNXX_MERGER_HPP
#define GRNXX_MERGER_HPP

#include "grnxx/types.hpp"

namespace grnxx {

// TODO: オプションの名前などは後で調整する．

enum MergeLogicalOperator {
  // 両方に含まれるレコードを残す．
  MERGE_LOGICAL_AND,

  // どちらか一方もしくは両方に含まれるレコードを残す．
  MERGE_LOGICAL_OR,

  // 一方には含まれていて，もう一方には含まれていないものを残す．
  MERGE_LOGICAL_XOR,

  // 一つ目の入力には含まれていて，二つ目の入力には含まれていないものを残す．
  MERGE_LOGICAL_SUB,

  // 一つ目の入力をそのまま残して，スコアの合成のみをおこなう．
  MERGE_LOGICAL_LHS
};

enum MergeScoreOperator {
  // スコアを加算する．
  MERGE_SCORE_ADD,

  // スコアを減算する．
  MERGE_SCORE_SUB,

  // スコアを乗算する．
  MERGE_SCORE_MUL

  // 減算は Adjuster との組み合わせでも実現できるものの，
  // Merger でサポートした方が便利かつ効率的になる．
};

enum MergeResultOrder {
  // 出力の順序は一つ目の入力に準拠する．
  MERGE_ORDER_AS_IS,

  // 出力の順序は任意とする．
  MERGE_ORDER_ARBITRARY
};

struct MergerOptions {
  // レコードの合成方法．
  MergeLogicalOperator logical_operator;

  // スコアの合成に用いる演算子．
  MergeLogicalOperator score_operator;

  // 出力の順序．
  MergeResultOrder result_order;

  MergerOptions();
};

class Merger {
 public:
  Merger();
  virtual ~Merger();

  // TODO: 入力の順序によって実装を切り替える．
  //
  // TODO: 一気に合成するときは，入力の数によって実装を切り替えたい．
  //
  // 合成器を作成する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 返り値は std::unique_ptr なので自動的に delete される．
  // 自動で delete されて困るときは release() で生のポインタを取り出す必要がある．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - オプションが不正である．
  // - リソースが確保できない．
  static std::unique_ptr<Merger> create(Error *error,
                                        std::unique_ptr<Order> &&lhs_order,
                                        std::unique_ptr<Order> &&rhs_order,
                                        const MergerOptions &options);

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
                     RecordSet *result_record_set) = 0;

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
                     RecordSet *result_record_set) = 0;
};

}  // namespace grnxx

#endif  // GRNXX_MERGER_HPP
