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
#include "grnxx/storage/header.hpp"

#include <cstring>

#include "grnxx/grnxx.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {
namespace storage {
namespace {

constexpr char FORMAT_STRING[] = "grnxx::Storage";

}  // namespace

Header::Header()
    : common_header(),
      max_file_size(0),
      max_num_files(0),
      num_body_chunks(0),
      num_small_body_chunks(0),
      reserved_0(0),
      num_nodes(0),
      num_active_or_unlinked_nodes(0),
      max_num_nodes(0),
      reserved_1(0),
      body_usage(0),
      body_size(0),
      total_size(0),
      latest_phantom_node_id(STORAGE_INVALID_NODE_ID),
      latest_unlinked_node_id(STORAGE_INVALID_NODE_ID),
      oldest_idle_node_ids(),
      data_mutex(),
      file_mutex(),
      reserved_2{} {
  for (size_t i = 0; i < NUM_IDLE_NODE_LISTS; ++i) {
    oldest_idle_node_ids[i] = STORAGE_INVALID_NODE_ID;
  }
}

Header::operator bool() const {
  return common_header.format() == FORMAT_STRING;
}

void Header::validate() {
  common_header = CommonHeader(FORMAT_STRING);
}

}  // namespace storage
}  // namespace grnxx
