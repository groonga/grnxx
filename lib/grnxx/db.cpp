#include "grnxx/db.hpp"

#include "grnxx/impl/db.hpp"

namespace grnxx {

unique_ptr<DB> open_db(Error *error,
                       const StringCRef &path,
                       const DBOptions &) {
  if (path.size() != 0) {
    // TODO: Named DB is not supported yet.
    GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
    return nullptr;
  }
  unique_ptr<impl::DB> db(new (nothrow) impl::DB);
  if (!db) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return unique_ptr<DB>(db.release());
}

bool remove_db(Error *error, const StringCRef &, const DBOptions &) {
  // TODO: Named DB is not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
  return false;
}

}  // namespace grnxx
