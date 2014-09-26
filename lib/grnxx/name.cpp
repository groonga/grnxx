#include "grnxx/name.hpp"

namespace grnxx {
namespace {

bool is_alpha(int c) {
  return ((c >= 'A') && (c <= 'Z')) || ((c >= 'a') && (c <= 'z'));
}

bool is_digit(int c) {
  return (c >= '0') && (c <= '9');
}

bool is_allowed_symbol(int c) {
  return c == '_';
}

}  // namespace

bool Name::assign(Error *error, const StringCRef &name) {
  if (!test(error, name)) {
    return false;
  }
  return string_.assign(error, name);
}

bool Name::test(Error *error, const StringCRef &name) {
  if ((name.size() < MIN_SIZE) || (name.size() > MAX_SIZE)) {
    GRNXX_ERROR_SET(error, INVALID_NAME,
                    "Invalid name size: size = %" PRIi64, name.size());
    return false;
  }
  if (!is_alpha(name[0]) && !is_digit(name[0])) {
    GRNXX_ERROR_SET(error, INVALID_NAME,
                    "Name must start with an alphanumeric character");
    return false;
  }
  for (Int i = 1; i < name.size(); ++i) {
    if (!is_alpha(name[i]) &&
        !is_digit(name[i]) &&
        !is_allowed_symbol(name[i])) {
      GRNXX_ERROR_SET(error, INVALID_NAME,
                      "Name contains invalid characters");
      return false;
    }
  }
  return true;
}

}  // namespace grnxx
