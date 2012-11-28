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
#include <cassert>
#include <iomanip>
#include <iostream>

#include "logger.hpp"
#include "string_format.hpp"
#include "time.hpp"

void test_align() {
  grnxx::StringBuilder builder;

  char buf[8];
  builder = grnxx::StringBuilder(buf);

  assert(builder << grnxx::StringFormat::align(
         "ABC", 6, '-', grnxx::STRING_FORMAT_ALIGNMENT_LEFT));
  assert(builder.str() == "ABC---");

  builder = grnxx::StringBuilder(buf);

  assert(builder << grnxx::StringFormat::align(
         "ABC", 6, '-', grnxx::STRING_FORMAT_ALIGNMENT_RIGHT));
  assert(builder.str() == "---ABC");

  builder = grnxx::StringBuilder(buf);

  assert(builder << grnxx::StringFormat::align(
         "ABC", 6, '-', grnxx::STRING_FORMAT_ALIGNMENT_CENTER));
  assert(builder.str() == "-ABC--");
}

void test_align_left() {
  grnxx::StringBuilder builder;

  char buf[8];
  builder = grnxx::StringBuilder(buf);

  assert(builder << grnxx::StringFormat::align_left(123, 5));
  assert(builder.str() == "123  ");

  builder = grnxx::StringBuilder(buf);

  assert(builder << grnxx::StringFormat::align_left(234, 5, 'X'));
  assert(builder.str() == "234XX");

  builder = grnxx::StringBuilder(buf);

  assert(!(builder << grnxx::StringFormat::align_left(345, 10, 'x')));
  assert(builder.str() == "345xxxx");
}

void test_align_right() {
  grnxx::StringBuilder builder;

  char buf[8];
  builder = grnxx::StringBuilder(buf);

  assert(builder << grnxx::StringFormat::align_right(456, 5));
  assert(builder.str() == "  456");

  builder = grnxx::StringBuilder(buf);

  assert(builder << grnxx::StringFormat::align_right(567, 5, 'X'));
  assert(builder.str() == "XX567");

  builder = grnxx::StringBuilder(buf);

  assert(!(builder << grnxx::StringFormat::align_right(678, 8, 'x')));
  assert(builder.str() == "xxxxx67");
}

void test_align_center() {
  grnxx::StringBuilder builder;

  char buf[8];
  builder = grnxx::StringBuilder(buf);

  assert(builder << grnxx::StringFormat::align_center(789, 5));
  assert(builder.str() == " 789 ");

  builder = grnxx::StringBuilder(buf);

  assert(builder << grnxx::StringFormat::align_center(890, 5, 'X'));
  assert(builder.str() == "X890X");

  builder = grnxx::StringBuilder(buf);

  assert(!(builder << grnxx::StringFormat::align_center(901, 8, 'x')));
  assert(builder.str() == "xx901xx");
}

class StreamBuffer : public std::streambuf {
 public:
  StreamBuffer(void *buf_ptr, std::size_t buf_size) : std::streambuf() {
    setp(static_cast<char *>(buf_ptr),
         static_cast<char *>(buf_ptr) + buf_size);
  }

  virtual int overflow(int) {
    return EOF;
  }
};

class Stream : public std::ostream {
 public:
  Stream(void *buf_ptr, std::size_t buf_size)
    : std::ostream(), stream_buffer_(buf_ptr, buf_size) {
    rdbuf(&stream_buffer_);
  }

 private:
  StreamBuffer stream_buffer_;
};

void benchmark() {
  static const std::uint32_t LOOP_COUNT = 1 << 16;

  char buf[1024] = "";
  grnxx::Time start, end;


  start = grnxx::Time::now();
  for (std::uint32_t i = 0; i < LOOP_COUNT; ++i) {
    std::snprintf(buf, sizeof(buf), "%d", __LINE__);
  }
  end = grnxx::Time::now();
  GRNXX_NOTICE() << "std::snprintf(int): elapsed [ns]: "
                 << ((end - start).nanoseconds() / LOOP_COUNT);

  start = grnxx::Time::now();
  for (std::uint32_t i = 0; i < LOOP_COUNT; ++i) {
    std::snprintf(buf, sizeof(buf), "%04d", __LINE__);
  }
  end = grnxx::Time::now();
  GRNXX_NOTICE() << "std::snprintf(align_right): elapsed [ns]: "
                 << ((end - start).nanoseconds() / LOOP_COUNT);

  start = grnxx::Time::now();
  for (std::uint32_t i = 0; i < LOOP_COUNT; ++i) {
    std::snprintf(buf, sizeof(buf), "%s:%d: %s: In %s(): %s",
                  __FILE__, __LINE__, "error", __func__, "failed");
  }
  end = grnxx::Time::now();
  GRNXX_NOTICE() << "std::snprintf(complex): elapsed [ns]: "
                 << ((end - start).nanoseconds() / LOOP_COUNT);


  start = grnxx::Time::now();
  for (std::uint32_t i = 0; i < LOOP_COUNT; ++i) {
    Stream stream(buf, sizeof(buf));
    stream << __LINE__;
  }
  end = grnxx::Time::now();
  GRNXX_NOTICE() << "std::ostream(int): elapsed [ns]: "
                 << ((end - start).nanoseconds() / LOOP_COUNT);

  start = grnxx::Time::now();
  for (std::uint32_t i = 0; i < LOOP_COUNT; ++i) {
    Stream stream(buf, sizeof(buf));
    stream << std::setw(4) << std::setfill('0') << __LINE__;
  }
  end = grnxx::Time::now();
  GRNXX_NOTICE() << "std::ostream(align_right): elapsed [ns]: "
                 << ((end - start).nanoseconds() / LOOP_COUNT);

  start = grnxx::Time::now();
  for (std::uint32_t i = 0; i < LOOP_COUNT; ++i) {
    Stream stream(buf, sizeof(buf));
    stream << __FILE__ << ':' << __LINE__ << ": " << "error" << ": In "
           << __func__ << "(): " << "failed";
  }
  end = grnxx::Time::now();
  GRNXX_NOTICE() << "std::ostream(complex): elapsed [ns]: "
                 << ((end - start).nanoseconds() / LOOP_COUNT);


  start = grnxx::Time::now();
  for (std::uint32_t i = 0; i < LOOP_COUNT; ++i) {
    grnxx::StringBuilder(buf).builder() << __LINE__;
  }
  end = grnxx::Time::now();
  GRNXX_NOTICE() << "grnxx::StringBuilder(int): elapsed [ns]: "
                 << ((end - start).nanoseconds() / LOOP_COUNT);

  start = grnxx::Time::now();
  for (std::uint32_t i = 0; i < LOOP_COUNT; ++i) {
    grnxx::StringBuilder(buf).builder()
        << grnxx::StringFormat::align_right(__LINE__, 4, '0');
  }
  end = grnxx::Time::now();
  GRNXX_NOTICE() << "grnxx::StringBuilder(align_right): elapsed [ns]: "
                 << ((end - start).nanoseconds() / LOOP_COUNT);

  start = grnxx::Time::now();
  for (std::uint32_t i = 0; i < LOOP_COUNT; ++i) {
    grnxx::StringBuilder(buf).builder()
        << __FILE__ << ':' << __LINE__ << ": "
        << "error" << ": In " << __func__ << "(): " << "failed";
  }
  end = grnxx::Time::now();
  GRNXX_NOTICE() << "grnxx::StringBuilder(complex): elapsed [ns]: "
                 << ((end - start).nanoseconds() / LOOP_COUNT);
}

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_align();
  test_align_left();
  test_align_right();
  test_align_center();
  benchmark();

  return 0;
}
