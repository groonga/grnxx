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
#include "block.hpp"

#include <ostream>

namespace grnxx {
namespace io {

StringBuilder &operator<<(StringBuilder &builder, BlockStatus status) {
  switch (status) {
    case BLOCK_PHANTOM: {
      return builder << "BLOCK_PHANTOM";
    }
    case BLOCK_ACTIVE: {
      return builder << "BLOCK_ACTIVE";
    }
    case BLOCK_FROZEN: {
      return builder << "BLOCK_FROZEN";
    }
    case BLOCK_IDLE: {
      return builder << "BLOCK_IDLE";
    }
  }
  return builder << "n/a";
}

std::ostream &operator<<(std::ostream &stream, BlockStatus status) {
  char buf[32];
  StringBuilder builder(buf);
  builder << status;
  return stream.write(builder.c_str(), builder.length());
}

StringBuilder &BlockInfo::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  builder << "{ id = " << id()
          << ", status = " << status();

  if (status_ != BLOCK_PHANTOM) {
    builder << ", chunk_id = " << chunk_id()
            << ", offset = " << offset()
            << ", size = " << size()
            << ", next_block_id = " << next_block_id()
            << ", prev_block_id = " << prev_block_id();
  }

  switch (status_) {
    case BLOCK_PHANTOM: {
      builder << ", next_phantom_block_id = " << next_phantom_block_id();
      break;
    }
    case BLOCK_ACTIVE: {
      break;
    }
    case BLOCK_FROZEN: {
      builder << ", next_frozen_block_id = " << next_frozen_block_id()
              << ", frozen_stamp = " << frozen_stamp();
      break;
    }
    case BLOCK_IDLE: {
      builder << ", next_idle_block_id = " << next_idle_block_id()
              << ", prev_idle_block_id = " << prev_idle_block_id();
      break;
    }
  }
  return builder << " }";
}

}  // namespace io
}  // namespace grnxx
