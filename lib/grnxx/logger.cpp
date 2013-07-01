/*
  Copyright (C) 2012-2013  Brazil, Inc.

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
#include "grnxx/logger.hpp"

#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <new>
#include <string>
#include <utility>
#include <vector>

#include "grnxx/backtrace.hpp"
#include "grnxx/lock.hpp"
#include "grnxx/system_clock.hpp"

namespace grnxx {

class LoggerSingleton {
 public:
  static bool write(const StringBuilder &builder);

  static bool open(const char *path);
  static void close();

 private:
  std::unique_ptr<char[]> path_;
  std::unique_ptr<std::ofstream> file_;

  // These variables may be used even after the instance termination.
  static volatile bool initialized_;
  static LoggerSingleton * volatile instance_;
  static Mutex mutex_;

  LoggerSingleton() : path_(), file_() {}
  ~LoggerSingleton() {
    Lock lock(&mutex_);
    instance_ = nullptr;
  }

  static void initialize_once() {
    if (!initialized_) {
      // C++11 guarantees that a static local variable is initialized once.
      // However, some compilers don't provide the guarantee.
      static Mutex mutex(MUTEX_UNLOCKED);
      Lock lock(&mutex);
      if (!initialized_) {
        initialize();
      }
    }
  }
  static void initialize() {
    static LoggerSingleton instance;
    instance_ = &instance;
    initialized_ = true;
  }

  LoggerSingleton(const LoggerSingleton &) = delete;
  LoggerSingleton &operator=(const LoggerSingleton &) = delete;
};

volatile bool LoggerSingleton::initialized_           = false;
LoggerSingleton * volatile LoggerSingleton::instance_ = nullptr;
Mutex LoggerSingleton::mutex_(MUTEX_UNLOCKED);

bool LoggerSingleton::write(const StringBuilder &builder) {
  initialize_once();
  Lock lock(&mutex_);
  if (!instance_) {
    return false;
  }
  static constexpr LoggerFlags OUTPUT_FLAGS =
      LOGGER_ENABLE_COUT | LOGGER_ENABLE_CERR | LOGGER_ENABLE_CLOG;
  const LoggerFlags flags = Logger::flags();
  if (!(flags & OUTPUT_FLAGS)) {
    if (!instance_->file_ || !*instance_->file_) {
      return false;
    }
  }
  if (instance_->file_ && *instance_->file_) {
    instance_->file_->write(builder.c_str(), builder.length()) << std::endl;
  }
  if (flags & LOGGER_ENABLE_COUT) {
    std::cout.write(builder.c_str(), builder.length()) << '\n';
  }
  if (flags & LOGGER_ENABLE_CERR) {
    std::cerr.write(builder.c_str(), builder.length()) << '\n';
  }
  if (flags & LOGGER_ENABLE_CLOG) {
    std::clog.write(builder.c_str(), builder.length()) << '\n';
  }
  return true;
}

bool LoggerSingleton::open(const char *path) {
  if (!path) {
    return false;
  }
  initialize_once();
  Lock lock(&mutex_);
  if (!instance_) {
    return false;
  }
  try {
    const size_t buf_size = std::strlen(path) + 1;
    std::unique_ptr<char[]> path_clone(new (std::nothrow) char[buf_size]);
    if (!path_clone) {
      return false;
    }
    std::memcpy(path_clone.get(), path, buf_size);
    std::unique_ptr<std::ofstream> file_dummy(
        new (std::nothrow) std::ofstream(path_clone.get(),
            std::ios::out | std::ios::app | std::ios::binary));
    if (!file_dummy || !*file_dummy) {
      return false;
    }
    instance_->path_ = std::move(path_clone);
    instance_->file_.swap(file_dummy);
    return true;
  } catch (...) {
    return false;
  }
}

void LoggerSingleton::close() {
  initialize_once();
  Lock lock(&mutex_);
  if (instance_) {
    instance_->file_.reset();
    instance_->path_.reset();
  }
}

LoggerFlags Logger::flags_   = LOGGER_DEFAULT;
int Logger::max_level_       = NOTICE_LOGGER;
int Logger::backtrace_level_ = ERROR_LOGGER;

Logger::Logger(const char *file, int line, const char *func, int level)
    : buf_(),
      builder_(buf_, string_builder_flags()),
      file_(file),
      line_(line),
      func_(func),
      level_(level) {
  append_line_header();
}

Logger::~Logger() {
  if (level_ <= backtrace_level()) {
    builder_ << backtrace();
  }
  LoggerSingleton::write(builder_);
}

bool Logger::open(const char *path) {
  return LoggerSingleton::open(path);
}

void Logger::close() {
  return LoggerSingleton::close();
}

void Logger::append_line_header() {
  if (!builder_) {
    return;
  }
  const LoggerFlags flags = Logger::flags();
  if (flags & LOGGER_WITH_DATE_TIME) {
    builder_ << SystemClock::now().local_time() << ": ";
  }
  if (flags & LOGGER_WITH_LOCATION) {
    builder_ << file_ << ':' << line_ << ": In " << func_ << "(): ";
  }
  if (flags & LOGGER_WITH_LEVEL) {
    switch (level_) {
      case ERROR_LOGGER: {
        builder_ << "error: ";
        break;
      }
      case WARNING_LOGGER: {
        builder_ << "warning: ";
        break;
      }
      case NOTICE_LOGGER: {
        builder_ << "notice: ";
        break;
      }
      default: {
        builder_ << "n/a (" << level_ << "): ";
        break;
      }
    }
  }
}

StringBuilderFlags Logger::string_builder_flags() {
  StringBuilderFlags flags = STRING_BUILDER_NOEXCEPT;
  if (Logger::flags() & LOGGER_ENABLE_AUTO_RESIZE) {
    flags |= STRING_BUILDER_AUTO_RESIZE;
  }
  return flags;
}

StringBuilder &operator<<(StringBuilder &builder, const Logger::NewLine &) {
  if (!builder) {
    return builder;
  }
  builder << '\n';
  Logger * const logger = reinterpret_cast<Logger *>(
      (reinterpret_cast<char *>(&builder) - LOGGER_BUF_SIZE));
  logger->append_line_header();
  return builder;
}

StringBuilder &operator<<(StringBuilder &builder, const Logger::Backtrace &) {
  if (!builder) {
    return builder;
  }
  std::vector<std::string> backtrace;
  if (Backtrace::pretty_backtrace(1, &backtrace)) {
    for (size_t i = 0; i < backtrace.size(); ++i) {
      builder << Logger::new_line() << backtrace[i].c_str();
    }
  }
  return builder;
}

}  // namespace grnxx
