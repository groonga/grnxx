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
#include "grnxx/map/hash_table/header.hpp"

#include "grnxx/map/hash_table/link_array.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {
namespace map {
namespace hash_table {

Header::Header()
    : map_type(MAP_HASH_TABLE),
      key_ids_storage_node_id(STORAGE_INVALID_NODE_ID),
      old_key_ids_storage_node_id(STORAGE_INVALID_NODE_ID),
      keys_storage_node_id(STORAGE_INVALID_NODE_ID),
      bits_storage_node_id(STORAGE_INVALID_NODE_ID),
      links_storage_node_id(STORAGE_INVALID_NODE_ID),
      max_key_id(MAP_MIN_KEY_ID - 1),
      num_keys(0),
      num_key_ids(0),
      latest_link(INVALID_LINK),
      mutex(MUTEX_UNLOCKED) {}

}  // namespace hash_table
}  // namespace map
}  // namespace grnxx
