#include "grnxx/cursor.hpp"

namespace grnxx {

CursorOptions::CursorOptions()
    : offset(0),
      limit(numeric_limits<Int>::max()),
      order_type(REGULAR_ORDER) {}

}  // namespace grnxx
