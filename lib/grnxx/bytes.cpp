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
#include "grnxx/bytes.hpp"

#include "grnxx/string_builder.hpp"

namespace grnxx {

StringBuilder &operator<<(StringBuilder &builder, const Bytes &bytes) {
  // TODO: StringBuilder should support const uint_8 *.
  return builder.append(reinterpret_cast<const char *>(bytes.ptr()),
                        bytes.size());
}

}  // namespace