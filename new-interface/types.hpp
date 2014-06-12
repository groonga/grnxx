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

enum ColumnType {
  ID_COLUMN,
  BOOL_COLUMN,
  INT_COLUMN,
  FLOAT_COLUMN,
  TEXT_COLUMN,
  REF_COLUMN,
  BOOL_ARRAY_COLUMN,
  INT_ARRAY_COLUMN,
  FLOAT_ARRAY_COLUMN,
  TEXT_ARRAY_COLUMN,
  REF_ARRAY_COLUMN,
  INDEX_COLUMN
};

std::ostream &operator<<(std::ostream &stream, ColumnType column_type);

enum DatumType {
  NULL_DATUM,
  BOOL_DATUM,
  INT_DATUM,
  FLOAT_DATUM,
  TEXT_DATUM,
  BOOL_ARRAY_DATUM,
  INT_ARRAY_DATUM,
  FLOAT_ARRAY_DATUM,
  TEXT_ARRAY_DATUM
};

std::ostream &operator<<(std::ostream &stream, DatumType datum_type);

enum IndexType {
  TREE_INDEX,
  HASH_INDEX
};

std::ostream &operator<<(std::ostream &stream, IndexType index_type);

}  // namespace grnxx

#endif  // GRNXX_TYPES_HPP
