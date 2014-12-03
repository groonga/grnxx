// TODO: Exception classes and utility macros should be provided for detailed
//       error handling.

#include "grnxx/error.hpp"

#include <cstdio>
#include <cstdarg>

namespace grnxx {

bool Error::set_message(const char *format, ...) {
  va_list args;
  va_start(args, format);
  if (std::vsnprintf(message_, MESSAGE_BUF_SIZE, format, args) < 0) {
    message_[0] = '\0';
    return false;
  }
  va_end(args);
  return true;
}

}  // namespace grnxx
