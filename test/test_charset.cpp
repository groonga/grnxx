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

#include "logger.hpp"
#include "charset.hpp"

void test_ascii() {
  const grnxx::Slice query = "Hello, world!";

  const grnxx::Charset *charset = grnxx::Charset::open(grnxx::CHARSET_EUC_JP);
  grnxx::Slice query_left = query;
  while (query_left) {
    const grnxx::Slice next = charset->get_char(query);
    assert(next.size() == 1);
    query_left.remove_prefix(next.size());
  }

  charset = grnxx::Charset::open(grnxx::CHARSET_SHIFT_JIS);
  query_left = query;
  while (query_left) {
    const grnxx::Slice next = charset->get_char(query);
    assert(next.size() == 1);
    query_left.remove_prefix(next.size());
  }

  charset = grnxx::Charset::open(grnxx::CHARSET_UTF_8);
  query_left = query;
  while (query_left) {
    const grnxx::Slice next = charset->get_char(query);
    assert(next.size() == 1);
    query_left.remove_prefix(next.size());
  }
}

void test_euc_jp() {
  const grnxx::Slice query = "\xCA\xB8\xBB\xFA\xCE\xF3";

  const grnxx::Charset *charset = grnxx::Charset::open(grnxx::CHARSET_EUC_JP);
  grnxx::Slice query_left = query;
  while (query_left) {
    const grnxx::Slice next = charset->get_char(query);
    assert(next.size() == 2);
    query_left.remove_prefix(next.size());
  }
}

void test_shift_jis() {
  const grnxx::Slice query = "\x95\xB6\x8E\x9A\x97\xF1";

  const grnxx::Charset *charset =
      grnxx::Charset::open(grnxx::CHARSET_SHIFT_JIS);
  grnxx::Slice query_left = query;
  while (query_left) {
    const grnxx::Slice next = charset->get_char(query);
    assert(next.size() == 2);
    query_left.remove_prefix(next.size());
  }
}

void test_utf_8() {
  const grnxx::Slice query = "\xE6\x96\x87\xE5\xAD\x97\xE5\x88\x97";

  const grnxx::Charset *charset = grnxx::Charset::open(grnxx::CHARSET_UTF_8);
  grnxx::Slice query_left = query;
  while (query_left) {
    const grnxx::Slice next = charset->get_char(query);
    assert(next.size() == 3);
    query_left.remove_prefix(next.size());
  }
}

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  test_ascii();
  test_euc_jp();
  test_shift_jis();
  test_utf_8();

  return 0;
}
