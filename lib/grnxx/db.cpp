#include "grnxx/db.hpp"

#include <new>

#include "grnxx/impl/db.hpp"

namespace grnxx {

std::unique_ptr<DB> open_db(const String &path, const DBOptions &) try {
  if (path.size() != 0) {
    throw "Not supported yet";  // TODO
  }
  return std::unique_ptr<impl::DB>(new impl::DB);
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

void remove_db(const String &, const DBOptions &) {
  throw "Not supported yet";  // TODO
}

}  // namespace grnxx
