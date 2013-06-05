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
#ifndef GRNXX_MAP_HASH_TABLE_HEADER_HPP
#define GRNXX_MAP_HASH_TABLE_HEADER_HPP

#include "grnxx/features.hpp"

#include "grnxx/map.hpp"
#include "grnxx/mutex.hpp"
#include "grnxx/types.hpp"

namespace grnxx {
namespace map {
namespace hash_table {

struct Header {
  MapType map_type;
  uint32_t key_ids_storage_node_id;
  uint32_t old_key_ids_storage_node_id;
  uint32_t keys_storage_node_id;
  uint32_t bits_storage_node_id;
  uint32_t links_storage_node_id;
  int64_t max_key_id;
  uint64_t num_keys;
  uint64_t num_key_ids;
  uint64_t latest_link;
  Mutex mutex;

  Header();
};

}  // namespace hash_table
}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_HASH_TABLE_HEADER_HPP
