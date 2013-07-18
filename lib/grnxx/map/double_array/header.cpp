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
#include "grnxx/map/double_array/header.hpp"

#include "grnxx/storage.hpp"

namespace grnxx {
namespace map {
namespace double_array {

Header::Header()
    : map_type(MAP_DOUBLE_ARRAY),
      max_key_id(MAP_MIN_KEY_ID - 1),
      num_keys(0),
      nodes_storage_node_id(STORAGE_INVALID_NODE_ID),
      siblings_storage_node_id(STORAGE_INVALID_NODE_ID),
      blocks_storage_node_id(STORAGE_INVALID_NODE_ID),
      entries_storage_node_id(STORAGE_INVALID_NODE_ID),
      pool_storage_node_id(STORAGE_INVALID_NODE_ID),
      next_key_id(MAP_MIN_KEY_ID),
      num_blocks(0),
      num_phantoms(0),
      num_zombies(0),
      latest_blocks() {
  for (uint64_t i = 0; i <= BLOCK_MAX_LEVEL; ++i) {
    latest_blocks[i] = BLOCK_INVALID_ID;
  }
}

}  // namespace double_array
}  // namespace map
}  // namespace grnxx
