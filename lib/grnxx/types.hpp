#ifndef GRNXX_TYPES_HPP
#define GRNXX_TYPES_HPP

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iosfwd>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace grnxx {

// 真偽値．
using Boolean = bool;

// 符号付き整数．
using Int8    = std::int8_t;
using Int16   = std::int16_t;
using Int32   = std::int32_t;
using Int64   = std::int64_t;

// 符号なし整数．
using UInt8   = std::uint8_t;
using UInt16  = std::uint16_t;
using UInt32  = std::uint32_t;
using UInt64  = std::uint64_t;

// 浮動小数点数．
using Float   = double;

// 文字列．
class String;

// 前方宣言．
class Database;
class Table;
class Column;
class Index;
class Calc;
class Sorter;

// 各種 ID．
using TableID  = Int64;
using ColumnID = Int64;
using IndexID  = Int64;
using RowID    = Int64;

// 各種 ID の最小値．
constexpr TableID  MIN_TABLE_ID  = 1;
constexpr ColumnID MIN_COLUMN_ID = 1;
constexpr IndexID  MIN_INDEX_ID  = 1;
constexpr RowID    MIN_ROW_ID    = 1;

// 行 ID を取得するカーソルのインタフェース．
class RowIDCursor {
 public:
  // カーソルを初期化する．
  explicit RowIDCursor() {}
  // カーソルを破棄する．
  virtual ~RowIDCursor() {}

  // 行 ID を最大 size 個取得して buf に格納し，取得した行 ID の数を返す．
  // buf が nullptr のときは取得した行 ID をそのまま捨てる．
  virtual Int64 get_next(RowID *buf, Int64 size) = 0;
};

// データ型の種類．
enum DataType {
  BOOLEAN,  // 真偽値．
  INTEGER,  // 整数．
  FLOAT,    // 浮動小数点数．
  STRING    // 文字列．
};

// データ型の情報．
template <typename T> struct TypeTraits;
template <> struct TypeTraits<Boolean> {
  static DataType data_type() {
    return BOOLEAN;
  }
};
template <> struct TypeTraits<Int64> {
  static DataType data_type() {
    return INTEGER;
  }
};
template <> struct TypeTraits<Float> {
  static DataType data_type() {
    return FLOAT;
  }
};
template <> struct TypeTraits<String> {
  static DataType data_type() {
    return STRING;
  }
};

// 索引の種類．
enum IndexType {
  TREE_MAP
};

std::ostream &operator<<(std::ostream &stream, IndexType index_type);

}  // namespace grnxx

#endif  // GRNXX_TYPES_HPP
