#ifndef GRNXX_MEMORY_HPP
#define GRNXX_MEMORY_HPP

#include <memory>

namespace grnxx {

// Smart pointer.
using std::unique_ptr;

// An object to make memory allocation (new) return nullptr on failure.
using std::nothrow;

}  // namespace grnxx

#endif  // GRNXX_MEMORY_HPP
