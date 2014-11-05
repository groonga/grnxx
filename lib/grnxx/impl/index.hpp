#ifndef GRNXX_IMPL_INDEX_HPP
#define GRNXX_IMPL_INDEX_HPP

#include <memory>

#include "grnxx/cursor.hpp"
#include "grnxx/index.hpp"

namespace grnxx {
namespace impl {

using ColumnInterface = grnxx::Column;
using IndexInterface = grnxx::Index;

class Index : public IndexInterface {
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_INDEX_HPP
