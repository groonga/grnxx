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

#include "grnxx/bytes.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/string_builder.hpp"
#include "grnxx/time.hpp"

void test_basic_operations() {
  {
    grnxx::StringBuilder builder;
    assert(builder);
    assert(builder.bytes() == "");

    assert(!builder.append('X'));
    assert(builder.bytes() == "");
  }

  {
    char buf[4];
    grnxx::StringBuilder builder(buf);
    assert(builder);
    assert(builder.bytes() == "");

    assert(builder.append('0'));
    assert(builder.append('1'));
    assert(builder.append('2'));
    assert(!builder.append('3'));
    assert(builder.bytes() == "012");
  }

  {
    char buf[4];
    grnxx::StringBuilder builder(buf, 3);
    assert(!builder.append("0123", 4));
    assert(builder.bytes() == "01");
  }

  {
    grnxx::StringBuilder builder(grnxx::STRING_BUILDER_AUTO_RESIZE);
    assert(builder.append('X', 3));
    assert(builder.append('Y', 2));
    assert(builder.append('Z', 1));
    assert(builder.append('-', 0));
    assert(builder.bytes() == "XXXYYZ");

    assert(builder.resize(4).bytes() == "XXXY");
    assert(builder.resize(1000).length() == 1000);
  }

  {
    grnxx::StringBuilder builder(grnxx::STRING_BUILDER_AUTO_RESIZE);
    assert(builder);
    assert(builder.bytes() == "");

    const size_t STRING_LENGTH = 1 << 20;
    for (size_t i = 0; i < STRING_LENGTH; ++i) {
      assert(builder.append('X'));
    }
    assert(builder.bytes().size() == STRING_LENGTH);
  }
}

void test_char() {
  grnxx::StringBuilder builder(grnxx::STRING_BUILDER_AUTO_RESIZE);
  builder << '0';
  builder << '1';
  builder << '2';
  builder << '3';
  assert(builder.bytes() == "0123");
}

void test_integer() {
  grnxx::StringBuilder builder(grnxx::STRING_BUILDER_AUTO_RESIZE);
  builder << 0;
  assert(builder.bytes() == "0");

  builder.clear();
  builder << 0U;
  assert(builder.bytes() == "0");

  builder.clear();
  builder << std::numeric_limits<std::int8_t>::min() << '/'
          << std::numeric_limits<std::int8_t>::max() << ','
          << std::numeric_limits<std::uint8_t>::min() << '/'
          << std::numeric_limits<std::uint8_t>::max();
  assert(builder.bytes() == "-128/127,0/255");

  builder.clear();
  builder << std::numeric_limits<std::int16_t>::min() << '/'
          << std::numeric_limits<std::int16_t>::max() << ','
          << std::numeric_limits<std::uint16_t>::min() << '/'
          << std::numeric_limits<std::uint16_t>::max();
  assert(builder.bytes() == "-32768/32767,0/65535");

  builder.clear();
  builder << std::numeric_limits<std::int32_t>::min() << '/'
          << std::numeric_limits<std::int32_t>::max() << ','
          << std::numeric_limits<std::uint32_t>::min() << '/'
          << std::numeric_limits<std::uint32_t>::max();
  assert(builder.bytes() == "-2147483648/2147483647,0/4294967295");

  builder.clear();
  builder << std::numeric_limits<std::int64_t>::min() << '/'
          << std::numeric_limits<std::int64_t>::max() << ','
          << std::numeric_limits<std::uint64_t>::min() << '/'
          << std::numeric_limits<std::uint64_t>::max();
  assert(builder.bytes() ==
         "-9223372036854775808/9223372036854775807,0/18446744073709551615");
}

void test_floating_point_number() {
  grnxx::StringBuilder builder(grnxx::STRING_BUILDER_AUTO_RESIZE);
  builder << 0.0;
  assert(builder.bytes() == "0.000000");

  builder.clear();
  builder << 16.5;
  assert(builder.bytes() == "16.500000");

  builder.clear();
  builder << 2.75F;
  assert(builder.bytes() == "2.750000");

  builder.clear();
  builder << (1.0 / 0.0) << '/' << (-1.0 / 0.0) << '/' << (0.0 / 0.0);
  assert(builder.bytes() == "inf/-inf/nan");
}

void test_bool() {
  grnxx::StringBuilder builder(grnxx::STRING_BUILDER_AUTO_RESIZE);
  builder << true << '/' << false;
  assert(builder.bytes() == "true/false");
}

void test_void_pointer() {
  grnxx::StringBuilder builder(grnxx::STRING_BUILDER_AUTO_RESIZE);
  builder << reinterpret_cast<void *>(0x13579BDF);
  if (sizeof(void *) == 4) {
    assert(builder.bytes() == "0x13579BDF");
  } else {
    assert(builder.bytes() == "0x0000000013579BDF");
  }

  builder.clear();
  builder << static_cast<void *>(nullptr);
  assert(builder.bytes() == "nullptr");
}

void test_zero_terminated_string() {
  grnxx::StringBuilder builder(grnxx::STRING_BUILDER_AUTO_RESIZE);
  builder << "Hello, " << "world!";
  assert(builder.bytes() == "Hello, world!");

  builder.clear();
  builder << static_cast<char *>(nullptr);
  assert(builder.bytes() == "nullptr");
}

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_basic_operations();
  test_char();
  test_integer();
  test_floating_point_number();
  test_bool();
  test_void_pointer();
  test_zero_terminated_string();

  return 0;
}
