#ifndef GRNXX_TYPES_FORWARD_HPP
#define GRNXX_TYPES_FORWARD_HPP

namespace grnxx {

// TODO: Error should be provided in types.hpp.

// Error information.
class Error;

// Database persistent object types.
class DB;
class Table;
class Column;
class Index;

// TODO: Datum should be provided in types.hpp.

// Database temporary object types.
class Datum;
class Cursor;
class Expression;
class ExpressionBuilder;
class Sorter;

}  // namespace grnxx

#endif  // GRNXX_TYPES_FORWARD_HPP
