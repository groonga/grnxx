#ifndef GRNXX_RECORD_SET_HPP
#define GRNXX_RECORD_SET_HPP

#include "grnxx/types.hpp"

namespace grnxx {

struct Record {
  RowID row_id;
  double score;
};

class RecordSet {
 public:
  RecordSet();
  ~RecordSet();

  // 所属するテーブルを取得する．
  Table *table() const;
  // レコード数を取得する．
  int64_t num_records() const;

  // レコードを取得する．
  // 不正なレコード ID を指定したときの動作は未定義である．
  Record get(int64_t i) const;

  // 行 ID を取得する．
  // 不正なレコード ID を指定したときの動作は未定義である．
  RowID get_row_id(int64_t i) const;

  // スコアを取得する．
  // 不正なレコード ID を指定したときの動作は未定義である．
  double get_score(int64_t i) const;
};

}  // namespace grnxx

#endif  // GRNXX_RECORD_SET_HPP
