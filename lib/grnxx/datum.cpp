#include "grnxx/datum.hpp"

#include <ostream>

namespace grnxx {

std::ostream &Datum::write_to(std::ostream &stream) const {
  switch (type_) {
    case BOOLEAN: {
      return stream << (boolean_ ? "TRUE" : "FALSE");
    }
    case INTEGER: {
      return stream << integer_;
    }
    case FLOAT: {
      if (std::isnan(float_)) {
        return stream << "NAN";
      } else if (std::isinf(float_)) {
        return stream << (std::signbit(float_) ? "-INF" : "INF");
      } else {
        return stream << float_;
      }
    }
    case STRING: {
      return stream << '"' << string_ << '"';
    }
    default: {
      return stream << "N/A";
    }
  }
}

std::ostream &operator<<(std::ostream &stream, const Datum &datum) {
  return datum.write_to(stream);
}

}  // namespace grnxx
