#ifndef GRNXX_SORTER_IMPL_HPP
#define GRNXX_SORTER_IMPL_HPP

#include "grnxx/sorter.hpp"
#include "grnxx/string.hpp"

namespace grnxx {

// 整列器を構成するノード．
// 整列条件のひとつと対応する．
class SorterNode {
 public:
  // ノードを初期化する．
  SorterNode() : next_(nullptr) {}
  // ノードを破棄する．
  virtual ~SorterNode() {}

  // 次のノードを返す．
  SorterNode *next() {
    return next_;
  }
  // 次のノードを設定する．
  void set_next(SorterNode *next) {
    next_ = next;
  }

  // 与えられた行の一覧を整列する．
  // 整列の結果が保証されるのは [begin, end) の範囲に限定される．
  virtual void sort(RowID *row_ids, Int64 num_row_ids,
                    Int64 begin, Int64 end) = 0;

 private:
  SorterNode *next_;
};

// 整列器．
class SorterImpl : public Sorter {
 public:
  // 整列器を初期化する．
  SorterImpl();
  // 整列器を破棄する．
  ~SorterImpl();

  // 指定された文字列に対応する整列器を作成する．
  // 文字列には，コンマ区切りでカラムを指定することができる．
  // カラム名の先頭に '-' を付けると降順になる．
  bool parse(const Table *table, String query);

  // 与えられた行の一覧を整列する．
  // 整列の結果が保証されるのは [offset, offset + limit) の範囲に限定される．
  void sort(RowID *row_ids, Int64 num_row_ids,
            Int64 offset = 0, Int64 limit = -1);

 protected:
  const Table *table_;
  std::vector<std::unique_ptr<SorterNode>> nodes_;

  // 指定された名前のカラムを整列条件に加える．
  // 先頭に '-' を付けると降順になる．
  // 成功すれば true を返し，失敗すれば false を返す．
  bool append_column(String column_name);
};

}  // namespace grnxx

#endif  // GRNXX_SORTER_IMPL_HPP
