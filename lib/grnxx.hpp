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
#ifndef GRNXX_GRNXX_HPP
#define GRNXX_GRNXX_HPP

#include "basic.hpp"

namespace grnxx {

class GRNXX_EXPORT Grnxx {
 public:
  // The e-mail address for bug reports.
  static const char *bugreport();

  // The version.
  static const char *version();

 private:
  Grnxx();
  ~Grnxx();

  Grnxx(const Grnxx &);
  Grnxx &operator=(const Grnxx &);
};

}  // namespace grnxx

#endif  // GRNXX_GRNXX_HPP
