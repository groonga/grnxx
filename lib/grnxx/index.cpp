#include "grnxx/index.hpp"

#include "grnxx/column_impl.hpp"

#include <ostream>

namespace grnxx {
namespace {

template <typename T>
class TreeMapIndex : public Index {
 public:
  using RowIDSet = std::set<RowID>;
  using Map = std::map<T, RowIDSet>;

  // 索引を初期化する．
  TreeMapIndex(IndexID index_id,
               const String &index_name,
               Column *column);

  // 指定されたデータを登録する．
  void insert(RowID row_id);
  // 指定されたデータを削除する．
  void remove(RowID row_id);

  // 行の一覧を取得できるカーソルを作成して返す．
  RowIDCursor *find_all(bool reverse_order);
  // 指定された値が格納された行の一覧を取得できるカーソルを作成して返す．
  RowIDCursor *find_equal(const Datum &datum, bool reverse_order);
  // 指定された範囲の値が格納された行の一覧を取得できるカーソルを作成して返す．
  RowIDCursor *find_less(const Datum &datum, bool less_equal,
                         bool reverse_order);
  // 指定された範囲の値が格納された行の一覧を取得できるカーソルを作成して返す．
  RowIDCursor *find_greater(const Datum &datum, bool greater_equal,
                            bool reverse_order);
  // 指定された範囲の値が格納された行の一覧を取得できるカーソルを作成して返す．
  RowIDCursor *find_between(const Datum &begin, const Datum &end,
                            bool greater_equal, bool less_equal,
                            bool reverse_order);

  class Cursor : public RowIDCursor {
   public:
    Cursor(typename Map::iterator begin, typename Map::iterator end);

    // 行 ID を最大 size 個取得して buf に格納し，取得した行 ID の数を返す．
    // buf が nullptr のときは取得した行 ID をそのまま捨てる．
    Int64 get_next(RowID *buf, Int64 size);

   private:
    typename Map::iterator map_it_;
    typename Map::iterator map_end_;
    typename RowIDSet::iterator set_it_;
    typename RowIDSet::iterator set_end_;
  };

  class ReverseCursor : public RowIDCursor {
   public:
    ReverseCursor(typename Map::iterator begin, typename Map::iterator end);

    // 行 ID を最大 size 個取得して buf に格納し，取得した行 ID の数を返す．
    // buf が nullptr のときは取得した行 ID をそのまま捨てる．
    Int64 get_next(RowID *buf, Int64 size);

   private:
    typename Map::reverse_iterator map_it_;
    typename Map::reverse_iterator map_end_;
    typename RowIDSet::iterator set_it_;
    typename RowIDSet::iterator set_end_;
  };

  RowIDCursor *create_cursor(typename Map::iterator begin,
                             typename Map::iterator end, bool reverse_order);

 private:
  Map map_;
  ColumnImpl<T> *column_;
};

// 索引を初期化する．
template <typename T>
TreeMapIndex<T>::TreeMapIndex(IndexID index_id,
                              const String &index_name,
                              Column *column)
    : Index(index_id, index_name, column, TREE_MAP),
      map_(),
      column_(static_cast<ColumnImpl<T> *>(column)) {}

// 指定されたデータを登録する．
template <typename T>
void TreeMapIndex<T>::insert(RowID row_id) {
  map_[column_->get(row_id)].insert(row_id);
}

// 指定されたデータを削除する．
template <typename T>
void TreeMapIndex<T>::remove(RowID row_id) {
  auto map_it = map_.find(column_->get(row_id));
  if (map_it == map_.end()) {
    return;
  }
  auto &set = map_it->second;
  auto set_it = set.find(row_id);
  if (set_it == set.end()) {
    return;
  }
  set.erase(set_it);
  if (set.empty()) {
    map_.erase(map_it);
  }
}

// 行の一覧を取得できるカーソルを作成して返す．
template <typename T>
RowIDCursor *TreeMapIndex<T>::find_all(bool reverse_order) {
  return create_cursor(map_.begin(), map_.end(), reverse_order);
}

// 指定された値が格納された行の一覧を取得できるカーソルを作成して返す．
template <typename T>
RowIDCursor *TreeMapIndex<T>::find_equal(const Datum &datum,
                                         bool reverse_order) {
  auto range = map_.equal_range(static_cast<T>(datum));
  return create_cursor(range.first, range.second, reverse_order);
}

// 指定された範囲の値が格納された行の一覧を取得できるカーソルを作成して返す．
template <typename T>
RowIDCursor *TreeMapIndex<T>::find_less(const Datum &datum,
                                        bool less_equal,
                                        bool reverse_order) {
  typename Map::iterator map_end;
  if (less_equal) {
    map_end = map_.upper_bound(static_cast<T>(datum));
  } else {
    map_end = map_.lower_bound(static_cast<T>(datum));
  }
  return create_cursor(map_.begin(), map_end, reverse_order);
}

// 指定された範囲の値が格納された行の一覧を取得できるカーソルを作成して返す．
template <typename T>
RowIDCursor *TreeMapIndex<T>::find_greater(const Datum &datum,
                                           bool greater_equal,
                                           bool reverse_order) {
  typename Map::iterator map_begin;
  if (greater_equal) {
    map_begin = map_.lower_bound(static_cast<T>(datum));
  } else {
    map_begin = map_.upper_bound(static_cast<T>(datum));
  }
  return create_cursor(map_begin, map_.end(), reverse_order);
}

// 指定された範囲の値が格納された行の一覧を取得できるカーソルを作成して返す．
template <typename T>
RowIDCursor *TreeMapIndex<T>::find_between(const Datum &begin,
                                           const Datum &end,
                                           bool greater_equal,
                                           bool less_equal,
                                           bool reverse_order) {
  T begin_key = static_cast<T>(begin);
  T end_key = static_cast<T>(end);
  typename Map::iterator map_begin;
  if (greater_equal) {
    map_begin = map_.lower_bound(begin_key);
  } else {
    map_begin = map_.upper_bound(begin_key);
  }
  typename Map::iterator map_end;
  if (less_equal) {
    map_end = map_.upper_bound(end_key);
  } else {
    map_end = map_.lower_bound(end_key);
  }
  return create_cursor(map_begin, map_end, reverse_order);
}

template <typename T>
TreeMapIndex<T>::Cursor::Cursor(typename Map::iterator begin,
                                typename Map::iterator end)
    : RowIDCursor(),
      map_it_(begin),
      map_end_(end),
      set_it_(),
      set_end_() {
  if (begin != end) {
    set_it_ = begin->second.begin();
    set_end_ = begin->second.end();
  }
}

// 行 ID を最大 size 個取得して buf に格納し，取得した行 ID の数を返す．
// buf が nullptr のときは取得した行 ID をそのまま捨てる．
template <typename T>
Int64 TreeMapIndex<T>::Cursor::get_next(RowID *buf, Int64 size) {
  if (map_it_ == map_end_) {
    return 0;
  }
  Int64 count = 0;
  while (count < size) {
    if (set_it_ == set_end_) {
      ++map_it_;
      if (map_it_ == map_end_) {
        return count;
      }
      set_it_ = map_it_->second.begin();
      set_end_ = map_it_->second.end();
    }
    if (buf) {
      buf[count] = *set_it_;
    }
    ++set_it_;
    ++count;
  }
  return count;
}

template <typename T>
TreeMapIndex<T>::ReverseCursor::ReverseCursor(typename Map::iterator begin,
                                              typename Map::iterator end)
    : RowIDCursor(),
      map_it_(end),
      map_end_(begin),
      set_it_(),
      set_end_() {
  if (map_it_ != map_end_) {
    set_it_ = map_it_->second.begin();
    set_end_ = map_it_->second.end();
  }
}

// 行 ID を最大 size 個取得して buf に格納し，取得した行 ID の数を返す．
// buf が nullptr のときは取得した行 ID をそのまま捨てる．
template <typename T>
Int64 TreeMapIndex<T>::ReverseCursor::get_next(RowID *buf, Int64 size) {
  if (map_it_ == map_end_) {
    return 0;
  }
  Int64 count = 0;
  while (count < size) {
    if (set_it_ == set_end_) {
      ++map_it_;
      if (map_it_ == map_end_) {
        return count;
      }
      set_it_ = map_it_->second.begin();
      set_end_ = map_it_->second.end();
    }
    if (buf) {
      buf[count] = *set_it_;
    }
    ++set_it_;
    ++count;
  }
  return count;
}

template <typename T>
RowIDCursor *TreeMapIndex<T>::create_cursor(typename Map::iterator begin,
                                            typename Map::iterator end,
                                            bool reverse_order) {
  if (reverse_order) {
    return new ReverseCursor(begin, end);
  } else {
    return new Cursor(begin, end);
  }
}

// String では更新時に本体のアドレスが移動してしまい，
// std::map では対応できないので， std::string を使っている．
template <>
class TreeMapIndex<String> : public Index {
 public:
  using RowIDSet = std::set<RowID>;
  using Map = std::map<std::string, RowIDSet>;

  // 索引を初期化する．
  TreeMapIndex(IndexID index_id,
               const String &index_name,
               Column *column);

  // 指定されたデータを登録する．
  void insert(RowID row_id);
  // 指定されたデータを削除する．
  void remove(RowID row_id);


  // 行の一覧を取得できるカーソルを作成して返す．
  RowIDCursor *find_all(bool reverse_order);
  // 指定された値が格納された行の一覧を取得できるカーソルを作成して返す．
  RowIDCursor *find_equal(const Datum &datum, bool reverse_order);
  // 指定された範囲の値が格納された行の一覧を取得できるカーソルを作成して返す．
  RowIDCursor *find_less(const Datum &datum, bool less_equal,
                         bool reverse_order);
  // 指定された範囲の値が格納された行の一覧を取得できるカーソルを作成して返す．
  RowIDCursor *find_greater(const Datum &datum, bool greater_equal,
                            bool reverse_order);
  // 指定された範囲の値が格納された行の一覧を取得できるカーソルを作成して返す．
  RowIDCursor *find_between(const Datum &begin, const Datum &end,
                            bool greater_equal, bool less_equal,
                            bool reverse_order);

  class Cursor : public RowIDCursor {
   public:
    Cursor(Map::iterator begin, Map::iterator end);

    // 行 ID を最大 size 個取得して buf に格納し，取得した行 ID の数を返す．
    // buf が nullptr のときは取得した行 ID をそのまま捨てる．
    Int64 get_next(RowID *buf, Int64 size);

   private:
    Map::iterator map_it_;
    Map::iterator map_end_;
    RowIDSet::iterator set_it_;
    RowIDSet::iterator set_end_;
  };

  class ReverseCursor : public RowIDCursor {
   public:
    ReverseCursor(Map::iterator begin, Map::iterator end);

    // 行 ID を最大 size 個取得して buf に格納し，取得した行 ID の数を返す．
    // buf が nullptr のときは取得した行 ID をそのまま捨てる．
    Int64 get_next(RowID *buf, Int64 size);

   private:
    Map::reverse_iterator map_it_;
    Map::reverse_iterator map_end_;
    RowIDSet::iterator set_it_;
    RowIDSet::iterator set_end_;
  };

  RowIDCursor *create_cursor(typename Map::iterator begin,
                             typename Map::iterator end, bool reverse_order);

 private:
  Map map_;
  ColumnImpl<String> *column_;
};

// 索引を初期化する．
TreeMapIndex<String>::TreeMapIndex(IndexID index_id,
                                   const String &index_name,
                                   Column *column)
    : Index(index_id, index_name, column, TREE_MAP),
      map_(),
      column_(static_cast<ColumnImpl<String> *>(column)) {}

// 指定されたデータを登録する．
void TreeMapIndex<String>::insert(RowID row_id) {
  String datum = column_->get(row_id);
  std::string key(reinterpret_cast<const char *>(datum.data()), datum.size());
  map_[key].insert(row_id);
}

// 指定されたデータを削除する．
void TreeMapIndex<String>::remove(RowID row_id) {
  String datum = column_->get(row_id);
  std::string key(reinterpret_cast<const char *>(datum.data()), datum.size());
  auto map_it = map_.find(key);
  if (map_it == map_.end()) {
    return;
  }
  auto &set = map_it->second;
  auto set_it = set.find(row_id);
  if (set_it == set.end()) {
    return;
  }
  set.erase(set_it);
  if (set.empty()) {
    map_.erase(map_it);
  }
}

// 行の一覧を取得できるカーソルを作成して返す．
RowIDCursor *TreeMapIndex<String>::find_all(bool reverse_order) {
  return create_cursor(map_.begin(), map_.end(), reverse_order);
}

// 指定された値が格納された行の一覧を取得できるカーソルを作成して返す．
RowIDCursor *TreeMapIndex<String>::find_equal(const Datum &datum,
                                              bool reverse_order) {
  auto range = map_.equal_range(static_cast<std::string>(datum));
  return create_cursor(range.first, range.second, reverse_order);
}

// 指定された範囲の値が格納された行の一覧を取得できるカーソルを作成して返す．
RowIDCursor *TreeMapIndex<String>::find_less(const Datum &datum,
                                             bool less_equal,
                                             bool reverse_order) {
  typename Map::iterator map_end;
  if (less_equal) {
    map_end = map_.upper_bound(static_cast<std::string>(datum));
  } else {
    map_end = map_.lower_bound(static_cast<std::string>(datum));
  }
  return create_cursor(map_.begin(), map_end, reverse_order);
}

// 指定された範囲の値が格納された行の一覧を取得できるカーソルを作成して返す．
RowIDCursor *TreeMapIndex<String>::find_greater(const Datum &datum,
                                                bool greater_equal,
                                                bool reverse_order) {
  typename Map::iterator map_begin;
  if (greater_equal) {
    map_begin = map_.lower_bound(static_cast<std::string>(datum));
  } else {
    map_begin = map_.upper_bound(static_cast<std::string>(datum));
  }
  return create_cursor(map_begin, map_.end(), reverse_order);
}

// 指定された範囲の値が格納された行の一覧を取得できるカーソルを作成して返す．
RowIDCursor *TreeMapIndex<String>::find_between(const Datum &begin,
                                                const Datum &end,
                                                bool greater_equal,
                                                bool less_equal,
                                                bool reverse_order) {
  std::string begin_key = static_cast<std::string>(begin);
  std::string end_key = static_cast<std::string>(end);
  Map::iterator map_begin;
  if (greater_equal) {
    map_begin = map_.lower_bound(begin_key);
  } else {
    map_begin = map_.upper_bound(begin_key);
  }
  Map::iterator map_end;
  if (less_equal) {
    map_end = map_.upper_bound(end_key);
  } else {
    map_end = map_.lower_bound(end_key);
  }
  return create_cursor(map_begin, map_end, reverse_order);
}

TreeMapIndex<String>::Cursor::Cursor(Map::iterator begin, Map::iterator end)
    : RowIDCursor(),
      map_it_(begin),
      map_end_(end),
      set_it_(),
      set_end_() {
  if (begin != end) {
    set_it_ = begin->second.begin();
    set_end_ = begin->second.end();
  }
}

// 行 ID を最大 size 個取得して buf に格納し，取得した行 ID の数を返す．
// buf が nullptr のときは取得した行 ID をそのまま捨てる．
Int64 TreeMapIndex<String>::Cursor::get_next(RowID *buf, Int64 size) {
  if (map_it_ == map_end_) {
    return 0;
  }
  Int64 count = 0;
  while (count < size) {
    if (set_it_ == set_end_) {
      ++map_it_;
      if (map_it_ == map_end_) {
        return count;
      }
      set_it_ = map_it_->second.begin();
      set_end_ = map_it_->second.end();
    }
    if (buf) {
      buf[count] = *set_it_;
    }
    ++set_it_;
    ++count;
  }
  return count;
}

TreeMapIndex<String>::ReverseCursor::ReverseCursor(Map::iterator begin,
                                                   Map::iterator end)
    : RowIDCursor(),
      map_it_(end),
      map_end_(begin),
      set_it_(),
      set_end_() {
  if (map_it_ != map_end_) {
    set_it_ = map_it_->second.begin();
    set_end_ = map_it_->second.end();
  }
}

// 行 ID を最大 size 個取得して buf に格納し，取得した行 ID の数を返す．
// buf が nullptr のときは取得した行 ID をそのまま捨てる．
Int64 TreeMapIndex<String>::ReverseCursor::get_next(RowID *buf, Int64 size) {
  if (map_it_ == map_end_) {
    return 0;
  }
  Int64 count = 0;
  while (count < size) {
    if (set_it_ == set_end_) {
      ++map_it_;
      if (map_it_ == map_end_) {
        return count;
      }
      set_it_ = map_it_->second.begin();
      set_end_ = map_it_->second.end();
    }
    if (buf) {
      buf[count] = *set_it_;
    }
    ++set_it_;
    ++count;
  }
  return count;
}

RowIDCursor *TreeMapIndex<String>::create_cursor(typename Map::iterator begin,
                                                 typename Map::iterator end,
                                                 bool reverse_order) {
  if (reverse_order) {
    return new ReverseCursor(begin, end);
  } else {
    return new Cursor(begin, end);
  }
}

class TreeMapIndexHelper {
 public:
  static Index *create(IndexID index_id,
                       const String &index_name,
                       Column *column);
};

Index *TreeMapIndexHelper::create(IndexID index_id,
                                  const String &index_name,
                                  Column *column) {
  switch (column->data_type()) {
    case BOOLEAN: {
      return new TreeMapIndex<Boolean>(index_id, index_name, column);
    }
    case INTEGER: {
      return new TreeMapIndex<Int64>(index_id, index_name, column);
    }
    case FLOAT: {
      return new TreeMapIndex<Float>(index_id, index_name, column);
    }
    case STRING: {
      return new TreeMapIndex<String>(index_id, index_name, column);
    }
  }
  return nullptr;
}

}  // namespace

// 索引を初期化する．
Index::Index(IndexID id, const String &name, Column *column, IndexType type)
    : id_(id),
      name_(reinterpret_cast<const char *>(name.data()), name.size()),
      column_(column),
      type_(type) {}

// 索引を破棄する．
Index::~Index() {}

// 指定された ID, 名前，対処カラム，種類の索引を作成して返す．
Index *IndexHelper::create(IndexID index_id,
                           const String &index_name,
                           Column *column,
                           IndexType index_type) {
  switch (index_type) {
    case TREE_MAP: {
      return TreeMapIndexHelper::create(index_id, index_name, column);
    }
  }
  return nullptr;
}

}  // namespace grnxx
