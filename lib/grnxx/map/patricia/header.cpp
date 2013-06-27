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
#include "grnxx/map/patricia/header.hpp"

#include "grnxx/storage.hpp"

namespace grnxx {
namespace map {
namespace patricia {

Header::Header()
    : map_type(MAP_PATRICIA),
      next_node_id(2),
      nodes_storage_node_id(STORAGE_INVALID_NODE_ID),
      keys_storage_node_id(STORAGE_INVALID_NODE_ID),
      cache_storage_node_id(STORAGE_INVALID_NODE_ID) {}

}  // namespace patricia
}  // namespace map
}  // namespace grnxx
