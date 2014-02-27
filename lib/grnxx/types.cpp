#include "grnxx/types.hpp"

#include <ostream>

namespace grnxx {

std::ostream &operator<<(std::ostream &stream, DataType data_type) {
  switch (data_type) {
    case BOOLEAN: {
      return stream << "BOOLEAN";
    }
    case INTEGER: {
      return stream << "INTEGER";
    }
    case FLOAT: {
      return stream << "FLOAT";
    }
    case STRING: {
      return stream << "STRING";
    }
  }
  return stream << "N/A";
}

std::ostream &operator<<(std::ostream &stream, IndexType index_type) {
  switch (index_type) {
    case TREE_MAP: {
      return stream << "TREE_MAP";
    }
  }
  return stream << "N/A";
}

}  // namespace grnxx
