#ifndef GRNXX_GROUP_SET_HPP
#define GRNXX_GROUP_SET_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class GroupSet {
 public:
  GroupSet();
  ~GroupSet();

  // 所属するテーブルを取得する．
  Table *table() const;
  // グループ数を取得する．
  int64_t num_groups() const;

  // 行数を取得する．
  int64_t get_num_rows(int64_t i) const;
  // 保存してある行の一覧を取得する．
  RowSet *get_row_set(int64_t i) const;

  // 整列する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // TODO: 整列条件の指定方法を決める．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - リソースが足りない．
  // - 演算において例外が発生する．
  bool sort(Error *error, const GroupSortConditions &conditions);
};

}  // namespace grnxx

#endif  // GRNXX_GROUP_SET_HPP
