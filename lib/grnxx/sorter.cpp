#include "grnxx/sorter.hpp"

#include "grnxx/error.hpp"

namespace grnxx {
namespace {

// TODO

}  // namespace

Sorter::~Sorter() {}

unique_ptr<Sorter> Sorter::create(
    Error *error,
    unique_ptr<OrderSet> &&order_set,
    const SorterOptions &options) {
  unique_ptr<Sorter> sorter(new (nothrow) Sorter);
  if (!sorter) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return sorter;
}

bool Sorter::reset(Error *error, RecordSet *record_set) {
  record_set_ = record_set;
  return true;
}

bool Sorter::progress(Error *error) {
  // TODO: Incremental sorting is not supported yet.
  return true;
}

bool Sorter::finish(Error *error) {
  if (!record_set_) {
    // Nothing to do.
    return true;
  }
  // TODO: Sorting is not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
  return false;
}

bool Sorter::sort(Error *error, RecordSet *record_set) {
  return reset(error, record_set) && finish(error);
}

Sorter::Sorter() : record_set_(nullptr) {}

}  // namespace grnxx
