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
#include <cassert>
#include <cerrno>

#include "grnxx/errno.hpp"
#include "grnxx/logger.hpp"

int main() {
  grnxx::Logger::set_flags(grnxx::LOGGER_WITH_ALL |
                           grnxx::LOGGER_ENABLE_COUT);
  grnxx::Logger::set_max_level(grnxx::NOTICE_LOGGER);

  // Errnos required by the ISO C standard.
  GRNXX_NOTICE() << "EDOM = " << grnxx::Errno(EDOM);
  GRNXX_NOTICE() << "EILSEQ = " << grnxx::Errno(EILSEQ);
  GRNXX_NOTICE() << "ERANGE = " << grnxx::Errno(ERANGE);

  // An undefined errno.
  GRNXX_NOTICE() << "-123456789 = " << grnxx::Errno(-123456789);
  return 0;
}