/*
  Copyright (C) 2013  Brazil, Inc.

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
#include <type_traits>

#include "grnxx/logger.hpp"
#include "grnxx/types.hpp"

namespace {

void test_nullptr_t() {
  assert((std::is_same<std::nullptr_t, grnxx::nullptr_t>::value));
}

void test_size_t() {
  assert((std::is_same<std::size_t, grnxx::size_t>::value));

  assert(sizeof(void *) == sizeof(grnxx::size_t));

  assert(std::is_unsigned<grnxx::size_t>::value);
}

void test_intXX_t() {
  assert((std::is_same<std::int8_t, grnxx::int8_t>::value));
  assert((std::is_same<std::uint8_t, grnxx::uint8_t>::value));
  assert((std::is_same<std::int16_t, grnxx::int16_t>::value));
  assert((std::is_same<std::uint16_t, grnxx::uint16_t>::value));
  assert((std::is_same<std::int32_t, grnxx::int32_t>::value));
  assert((std::is_same<std::uint32_t, grnxx::uint32_t>::value));
  assert((std::is_same<std::int64_t, grnxx::int64_t>::value));
  assert((std::is_same<std::uint64_t, grnxx::uint64_t>::value));

  assert(1 == sizeof(grnxx::int8_t));
  assert(1 == sizeof(grnxx::uint8_t));
  assert(2 == sizeof(grnxx::int16_t));
  assert(2 == sizeof(grnxx::uint16_t));
  assert(4 == sizeof(grnxx::int32_t));
  assert(4 == sizeof(grnxx::uint32_t));
  assert(8 == sizeof(grnxx::int64_t));
  assert(8 == sizeof(grnxx::uint64_t));

  assert(std::is_signed<grnxx::int8_t>::value);
  assert(std::is_signed<grnxx::int16_t>::value);
  assert(std::is_signed<grnxx::int32_t>::value);
  assert(std::is_signed<grnxx::int64_t>::value);

  assert(std::is_unsigned<grnxx::uint8_t>::value);
  assert(std::is_unsigned<grnxx::uint16_t>::value);
  assert(std::is_unsigned<grnxx::uint32_t>::value);
  assert(std::is_unsigned<grnxx::uint64_t>::value);
}

void test_intptr_t() {
  assert((std::is_same<std::intptr_t, grnxx::intptr_t>::value));
  assert((std::is_same<std::uintptr_t, grnxx::uintptr_t>::value));

  assert(sizeof(void *) == sizeof(grnxx::intptr_t));
  assert(sizeof(void *) == sizeof(grnxx::uintptr_t));

  assert(std::is_signed<grnxx::intptr_t>::value);
  assert(std::is_unsigned<grnxx::uintptr_t>::value);
}

}  // namespace

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);


  test_nullptr_t();
  test_size_t();
  test_intXX_t();
  test_intptr_t();

  return 0;
}
