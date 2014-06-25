#ifndef GRNXX_TYPES_HPP
#define GRNXX_TYPES_HPP

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iosfwd>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace grnxx {

using std::int8_t;
using std::int16_t;
using std::int32_t;
using std::int64_t;

using std::uint8_t;
using std::uint16_t;
using std::uint32_t;
using std::uint64_t;

using std::size_t;

using TableID  = int64_t;
using ColumnID = int64_t;
using IndexID  = int64_t;
using RowID    = int64_t;

constexpr RowID NULL_ROW_ID = RowID(0);
constexpr RowID MIN_ROW_ID  = RowID(1);
constexpr RowID MAX_ROW_ID  = (RowID(1) << 40) - 1;

struct DBOptions;
struct TableOptions;
struct ColumnOptions;
struct IndexOptions;

class DB;
class Table;
class Column;
class Index;
class Datum;

enum DataType {
  // 無効な型．
  INVALID_DATA,

  // 真偽値．
  BOOLEAN_DATA,
  // 64-bit 符号あり整数．
  INTEGER_DATA,
  // 倍精度浮動小数点数．
  FLOAT_DATA,
  // 時刻（The Unix Epoch からの経過マイクロ秒数）．
  TIME_DATA,
  // 経緯度（ミリ秒単位）．
  GEO_POINT_DATA,
  // 文字列．
  TEXT_DATA,
  // 参照．
  REFERENCE_DATA,

  // 上述の型を配列にしたもの．
  BOOLEAN_ARRAY_DATA,
  INTEGER_ARRAY_DATA,
  FLOAT_ARRAY_DATA,
  TIME_ARRAY_DATA,
  GEO_POINT_ARRAY_DATA,
  TEXT_ARRAY_DATA,
  REFERENCE_ARRAY_DATA
};

std::ostream &operator<<(std::ostream &stream, DataType data_type);

enum IndexType {
  // 木構造にもとづく索引．
  // 範囲検索をサポートする．
  TREE_INDEX,

  // ハッシュ表にもとづく索引．
  // TREE_INDEX と比べると，データの登録・削除と完全一致検索が速い代わり，
  // 範囲検索などには向かない（あるいは使えない）．
  HASH_INDEX

  // TODO: 全文検索用の索引は，こちらの索引に加えるか，まったく別の索引として扱う．
};

std::ostream &operator<<(std::ostream &stream, IndexType index_type);

}  // namespace grnxx

#endif  // GRNXX_TYPES_HPP
