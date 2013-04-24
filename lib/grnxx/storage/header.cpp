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

#include "grnxx/version.h"
#include "grnxx/storage.hpp"

#define GRNXX_STORAGE_HEADER_FORMAT  "grnxx::Storage"
#define GRNXX_STORAGE_HEADER_VERSION GRNXX_VERSION

namespace grnxx {
namespace storage {
namespace {

// For comparison.
constexpr char HEADER_FORMAT[HEADER_FORMAT_SIZE] = GRNXX_STORAGE_HEADER_FORMAT;

}  // namespace

Header::Header()
    : format{ GRNXX_STORAGE_HEADER_FORMAT },
      version{ GRNXX_STORAGE_HEADER_VERSION },
      max_num_files(0),
      max_file_size(0),
      total_size(0),
      num_files(0),
      num_node_chunks(0),
      num_nodes(0),
      max_num_nodes(0),
      latest_phantom_node_id(INVALID_NODE_ID),
      latest_unlinked_node_id(INVALID_NODE_ID),
      oldest_idle_node_ids(),
      inter_process_data_mutex(MUTEX_UNLOCKED),
      inter_process_file_mutex(MUTEX_UNLOCKED) {
  static constexpr size_t NUM_LISTS =
      sizeof(oldest_idle_node_ids) / sizeof(*oldest_idle_node_ids);
  for (size_t i = 0; i < NUM_LISTS; ++i) {
    oldest_idle_node_ids[i] = INVALID_NODE_ID;
  }
}

bool Header::is_valid() const {
  return std::memcmp(format, HEADER_FORMAT, HEADER_FORMAT_SIZE) == 0;
}

}  // namespace storage
}  // namespace grnxx
