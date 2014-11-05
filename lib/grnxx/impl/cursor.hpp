#ifndef GRNXX_IMPL_CURSOR_HPP
#define GRNXX_IMPL_CURSOR_HPP

#include "grnxx/cursor.hpp"

namespace grnxx {
namespace impl {

class EmptyCursor : public Cursor {
 public:
  // -- Public API --

  EmptyCursor() = default;
  ~EmptyCursor() = default;

  size_t read(ArrayRef<Record>) {
    return 0;
  }
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_CURSOR_HPP
