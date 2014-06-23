#ifndef GRNXX_PIPELINE_BUILDER_HPP
#define GRNXX_PIPELINE_BUILDER_HPP

#include "grnxx/types.hpp"

namespace grnxx {

// 後置記法（逆ポーランド記法）に基づいてパイプラインを構築する．
class PipelineBuilder {
 public:
  PipelineBuilder();
  virtual ~PipelineBuilder();

  // 所属するテーブルを取得する．
  virtual Table *table() const = 0;

  // カーソルをスタックに積む．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定されたカーソルが不正である．
  // - リソースを確保できない．
  virtual bool push_cursor(Error *error,
                           std::unique_ptr<Cursor> &&cursor) = 0;

  // 入力をスタックから降ろし，代わりにフィルタを積む．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 入力はあらかじめスタックに積んでおく必要がある．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定されたフィルタが不正である．
  // - 入力が存在しない．
  // - リソースを確保できない．
  virtual bool push_filter(Error *error,
                           std::unique_ptr<Expression> &&expression,
                           const FilterOptions &options) = 0;

  // 入力をスタックから降ろし，代わりにスコアの Adjuster を積む．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 入力はあらかじめスタックに積んでおく必要がある．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された Adjuster が不正である．
  // - 入力が存在しない．
  // - リソースを確保できない．
  virtual bool push_adjuster(Error *error,
                             std::unique_ptr<Expression> &&expression,
                             const AdjusterOptions &options) = 0;

  // TODO: 将来的な検討案．
  //
  // 入力をスタックから降ろし，代わりにスコアの Normalizer を積む．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 入力はあらかじめスタックに積んでおく必要がある．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された Normalizer が不正である．
  // - 入力が存在しない．
  // - リソースを確保できない．
//  virtual bool push_normalizer(Error *error,
//                               std::unique_ptr<Normalizer> &&normalizer) = 0;

  // 入力をスタックから降ろし，代わりに整列器を積む．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 入力はあらかじめスタックに積んでおく必要がある．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された整列器が不正である．
  // - 入力が存在しない．
  // - リソースを確保できない．
  virtual bool push_sorter(Error *error,
                           std::unique_ptr<Order> &&order,
                           const SorterOptions &options) = 0;

  // 二つの入力をスタックから降ろし，代わりに合成器を積む．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 入力はあらかじめスタックに積んでおく必要がある．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された合成器が不正である．
  // - 入力が存在しない．
  // - リソースを確保できない．
  virtual bool push_merger(Error *error,
                           const MergerOptions &options) = 0;

  // 保持しているノードやスタックを破棄する．
  virtual void clear() = 0;

  // 構築中のパイプラインを完成させ，その所有権を取得する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 所有権を返すため，保持しているカーソルなどは破棄する．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - スタックの要素数が一つでない．
  //  - 何も積まれていない．
  //  - 積まれたものが使われずに残っている．
  //   - パイプラインが完成していないことを示す．
  // - リソースを確保できない．
  virtual std::unique_ptr<Pipeline> release(Error *error) = 0;
};

}  // namespace grnxx

#endif  // GRNXX_PIPELINE_BUILDER_HPP
