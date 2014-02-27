#include "grnxx/string.hpp"

#include <ostream>

namespace grnxx {

std::ostream &operator<<(std::ostream &stream, const String &string) {
  stream.write(reinterpret_cast<const char *>(string.data()), string.size());
  return stream;
}

}  // namespace grnxx
