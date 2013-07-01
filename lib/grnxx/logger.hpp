/*
  Copyright (C) 2012  Brazil, Inc.

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public
  License as published by the Free Software Foundation; either
  version 2.1 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public
  License along with this library; if not, write to the Free Software
  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
*/
#ifndef GRNXX_LOGGER_HPP
#define GRNXX_LOGGER_HPP

#include "grnxx/features.hpp"

#include "grnxx/flags_impl.hpp"
#include "grnxx/string_builder.hpp"
#include "grnxx/types.hpp"

#define GRNXX_ERROR()   GRNXX_LOGGER(::grnxx::ERROR_LOGGER)
#define GRNXX_WARNING() GRNXX_LOGGER(::grnxx::WARNING_LOGGER)
#define GRNXX_NOTICE()  GRNXX_LOGGER(::grnxx::NOTICE_LOGGER)

#define GRNXX_LOGGER(level)\
  ((level) > ::grnxx::Logger::max_level()) ? (void)0 :\
  ::grnxx::Logger::Voidify() &\
  ::grnxx::Logger(__FILE__, __LINE__, __func__, (level)).builder()

namespace grnxx {

constexpr size_t LOGGER_BUF_SIZE = 4096;

enum LoggerLevel {
  ERROR_LOGGER   = 0x0000,
  WARNING_LOGGER = 0x0001,
  NOTICE_LOGGER  = 0x0002
};

class Logger;
typedef FlagsImpl<Logger> LoggerFlags;

constexpr LoggerFlags LOGGER_DEFAULT        = LoggerFlags::define(0x0000);

constexpr LoggerFlags LOGGER_WITH_DATE_TIME = LoggerFlags::define(0x0001);
constexpr LoggerFlags LOGGER_WITH_LOCATION  = LoggerFlags::define(0x0002);
constexpr LoggerFlags LOGGER_WITH_LEVEL     = LoggerFlags::define(0x0004);
constexpr LoggerFlags LOGGER_WITH_ALL       = LoggerFlags::define(0x0007);

constexpr LoggerFlags LOGGER_ENABLE_COUT    = LoggerFlags::define(0x0100);
constexpr LoggerFlags LOGGER_ENABLE_CERR    = LoggerFlags::define(0x0200);
constexpr LoggerFlags LOGGER_ENABLE_CLOG    = LoggerFlags::define(0x0400);

constexpr LoggerFlags LOGGER_ENABLE_AUTO_RESIZE = LoggerFlags::define(0x1000);

class Logger {
 public:
  Logger(const char *file, int line, const char *func, int level);
  ~Logger();

  static bool open(const char *path);
  static void close();

  static LoggerFlags flags() {
    return flags_;
  }
  static int max_level() {
    return max_level_;
  }
  static int backtrace_level() {
    return backtrace_level_;
  }

  static void set_flags(LoggerFlags value) {
    flags_ = value;
  }
  static void set_max_level(int value) {
    max_level_ = value;
  }
  static void set_backtrace_level(int value) {
    backtrace_level_ = value;
  }

  struct NewLine {};
  struct Backtrace {};

  static NewLine new_line() {
    return NewLine();
  }
  static Backtrace backtrace() {
    return Backtrace();
  }

  class Voidify {
   public:
    void operator&(StringBuilder &) const {}
  };

  StringBuilder &builder() {
    return builder_;
  }

  void append_line_header();

 private:
  char buf_[LOGGER_BUF_SIZE];
  StringBuilder builder_;
  const char * const file_;
  const int line_;
  const char * const func_;
  const int level_;

  static LoggerFlags flags_;
  static int max_level_;
  static int backtrace_level_;

  Logger(const Logger &);
  Logger &operator=(const Logger &);
};

// These operators must not be called with a regular (non-logger) builder.
StringBuilder &operator<<(StringBuilder &builder, const Logger::NewLine &);
StringBuilder &operator<<(StringBuilder &builder, const Logger::Backtrace &);

}  // namespace grnxx

#endif  // GRNXX_LOGGER_HPP
