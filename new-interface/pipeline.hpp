#ifndef GRNXX_PIPELINE_HPP
#define GRNXX_PIPELINE_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class Pipeline {
 public:
  Pipeline();
  virtual ~Pipeline();

  // パイプラインを通してレコードの一覧を取得する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // パイプラインは使い捨てなので，二回目の呼び出しは失敗する．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 演算において例外が発生する．
  // - リソースを確保できない．
  virtual bool flush(Error *error,
                     RecordSet *record_set);
};

}  // namespace grnxx

#endif  // GRNXX_PIPELINE_HPP
