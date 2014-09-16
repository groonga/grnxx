#ifndef GRNXX_TREE_INDEX_HPP
#define GRNXX_TREE_INDEX_HPP

#include <cmath>
#include <map>
#include <set>

#include "grnxx/index.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

template <typename T> class TreeIndex;

// TODO: This is a test implementation.
template <>
class TreeIndex<Bool> : public Index {
 public:
  using Value = Bool;
  using Set = std::set<Int>;
  using Map = std::map<Value, Set>;

  static unique_ptr<TreeIndex> create(Error *error,
                                      Column *column,
                                      String name,
                                      const IndexOptions &options);

  ~TreeIndex();

  unique_ptr<Cursor> create_cursor(
      Error *error,
      const Datum &datum,
      const CursorOptions &options = CursorOptions()) const;

  unique_ptr<Cursor> create_cursor(
      Error *error,
      const IndexRange &range,
      const CursorOptions &options) const;

  bool insert(Error *error, Int row_id, const Datum &value);
  bool remove(Error *error, Int row_id, const Datum &value);

 private:
  mutable Map map_;

  TreeIndex() : Index(), map_() {}
};

// TODO: This is a test implementation.
template <>
class TreeIndex<Int> : public Index {
 public:
  using Value = Int;
  using Set = std::set<Int>;
  using Map = std::map<Value, Set>;

  static unique_ptr<TreeIndex> create(Error *error,
                                      Column *column,
                                      String name,
                                      const IndexOptions &options);

  ~TreeIndex();

  unique_ptr<Cursor> create_cursor(
      Error *error,
      const Datum &datum,
      const CursorOptions &options = CursorOptions()) const;

  unique_ptr<Cursor> create_cursor(
      Error *error,
      const IndexRange &range,
      const CursorOptions &options) const;

  bool insert(Error *error, Int row_id, const Datum &value);
  bool remove(Error *error, Int row_id, const Datum &value);

 private:
  mutable Map map_;

  TreeIndex() : Index(), map_() {}
};

// TODO: This is a test implementation.
template <>
class TreeIndex<Float> : public Index {
 public:
  struct Less {
    bool operator()(Float lhs, Float rhs) const {
      // Numbers are prior to NaN.
      if (std::isnan(lhs)) {
        return false;
      } else if (std::isnan(rhs)) {
        return true;
      }
      return lhs < rhs;
    }
  };

  using Value = Float;
  using Set = std::set<Int>;
  using Map = std::map<Value, Set, Less>;

  static unique_ptr<TreeIndex> create(Error *error,
                                      Column *column,
                                      String name,
                                      const IndexOptions &options);

  ~TreeIndex();

  unique_ptr<Cursor> create_cursor(
      Error *error,
      const Datum &datum,
      const CursorOptions &options = CursorOptions()) const;

  unique_ptr<Cursor> create_cursor(
      Error *error,
      const IndexRange &range,
      const CursorOptions &options) const;

  bool insert(Error *error, Int row_id, const Datum &value);
  bool remove(Error *error, Int row_id, const Datum &value);

 private:
  mutable Map map_;

  TreeIndex() : Index(), map_() {}
};

// TODO: This is a test implementation.
template <>
class TreeIndex<Text> : public Index {
 public:
  using Value = Text;
  using Set = std::set<Int>;
  using Map = std::map<std::string, Set>;

  static unique_ptr<TreeIndex> create(Error *error,
                                      Column *column,
                                      String name,
                                      const IndexOptions &options);

  ~TreeIndex();

  unique_ptr<Cursor> create_cursor(
      Error *error,
      const Datum &datum,
      const CursorOptions &options = CursorOptions()) const;

  unique_ptr<Cursor> create_cursor(
      Error *error,
      const IndexRange &range,
      const CursorOptions &options) const;

  bool insert(Error *error, Int row_id, const Datum &value);
  bool remove(Error *error, Int row_id, const Datum &value);

 private:
  mutable Map map_;

  TreeIndex() : Index(), map_() {}
};

}  // namespace grnxx

#endif  // GRNXX_TREE_INDEX_HPP
