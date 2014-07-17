#include "grnxx/index.hpp"

#include "grnxx/cursor.hpp"

namespace grnxx {

Index::~Index() {}

unique_ptr<Cursor> Index::create_cursor(
    Error *error,
    const CursorOptions &options) const {
  // TODO: Not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supoprted yet");
  return nullptr;
}

unique_ptr<Index> Index::create(Error *error,
                                Column *column,
                                String name,
                                IndexType type,
                                const IndexOptions &options) {
  // TODO: Not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supoprted yet");
  return nullptr;
}

bool Index::rename(Error *error, String new_name) {
  return name_.assign(error, new_name);
}

bool Index::is_removable() {
  // TODO: Not supported yet.
  return true;
}

Index::Index()
    : column_(nullptr),
      name_(),
      type_() {}

}  // namespace grnxx
