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
#include "grnxx/io/chunk.hpp"

#include "grnxx/string_builder.hpp"

namespace grnxx {
namespace io {

StringBuilder &ChunkInfo::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  return builder << "{ id = " << id()
                 << ", file_id = " << file_id()
                 << ", offset = " << offset()
                 << ", size = " << size() << " }";
}

}  // namespace io
}  // namespace grnxx