#ifndef GRNXX_ERROR_HPP
#define GRNXX_ERROR_HPP

#include "grnxx/types.hpp"

namespace grnxx {

enum ErrorCode {
  NO_ERROR,           // No error occurred.
  NOT_FOUND,          // The target does not found.
  ALREADY_EXISTS,     // The target already exists.
  NOT_REMOVABLE,      // The target is not removable.
  BROKEN,             // The database is broken.
  NO_MEMORY,          // Memory allocation failed.
  INVALID_NAME,       // The string is invalid as an object name.
  NO_KEY_COLUMN,      // The table has no key column.
  INVALID_ARGUMENT,   // Invalid argument.
  INVALID_OPERATION,  // Invalid operation.
  INVALID_OPERAND,    // Invalid operand.
  NOT_SUPPORTED_YET   // The operation is not supported yet.
};

// Many functions take a pointer to an Error object as the first argument
// (except an implicit this argument) for returning error information on
// failure. On success, the Error object will not be modified. If the pointer
// is nullptr, the error information will not be generated even on failure.
class Error {
 public:
  // The error code of an initialized Error object is NO_ERROR.
  Error()
      : code_(NO_ERROR),
        line_(0),
        file_(""),
        function_(""),
        message_() {
    message_[0] = '\0';
  }

  // Error code.
  ErrorCode code() const {
    return code_;
  }
  // Line number (__LINE__).
  int line() const {
    return code_;
  }
  // File name (__FILE__).
  const char *file() const {
    return file_;
  }
  // Function name (__func__, __FUNCTION__, or __PRETTY_FUNCTION__).
  const char *function() const {
    return function_;
  }
  // Error message.
  const char *message() const {
    return message_;
  }

  // Set an error code.
  void set_code(ErrorCode code) {
    code_ = code;
  }
  // Set a line number.
  void set_line(int line) {
    line_ = line;
  }
  // Set a file name.
  void set_file(const char *file) {
    file_ = file;
  }
  // Set a function name.
  void set_function(const char *function) {
    function_ = function;
  }
  // Generate an error message with the printf syntax.
  // If the expected message is too long, the result will be truncated.
  // Return true on success.
  bool set_message(const char *format, ...)
      __attribute__ ((format (printf, 2, 3)));

 private:
  static constexpr Int MESSAGE_BUF_SIZE = 256;

  ErrorCode code_;
  int line_;
  const char *file_;
  const char *function_;
  char message_[MESSAGE_BUF_SIZE];
};

}  // namespace grnxx

// Set error information.
#define GRNXX_ERROR_SET(error, code, format, ...) \
  (((error) != nullptr) && ((error)->set_code(code), \
                            (error)->set_line(__LINE__), \
                            (error)->set_file(__FILE__), \
                            (error)->set_function(__PRETTY_FUNCTION__), \
                            (error)->set_message(format, ## __VA_ARGS__)))

#endif  // GRNXX_ERROR_HPP
