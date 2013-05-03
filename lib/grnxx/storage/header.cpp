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
    : format{},
      version{},
      max_file_size(0),
      max_num_files(0),
      num_body_chunks(0),
      num_nodes(0),
      total_size(0),
      max_num_nodes(0),
      latest_phantom_node_id(STORAGE_INVALID_NODE_ID),
      latest_unlinked_node_id(STORAGE_INVALID_NODE_ID),
      oldest_idle_node_ids(),
      data_mutex(MUTEX_UNLOCKED),
      file_mutex(MUTEX_UNLOCKED),
      reserved{} {
  std::memcpy(version, GRNXX_STORAGE_HEADER_VERSION, HEADER_VERSION_SIZE);
  for (size_t i = 0; i < NUM_IDLE_NODE_LISTS; ++i) {
    oldest_idle_node_ids[i] = STORAGE_INVALID_NODE_ID;
  }
}

bool Header::is_valid() const {
  return std::memcmp(format, HEADER_FORMAT, HEADER_FORMAT_SIZE) == 0;
}

void Header::validate() {
  std::memcpy(format, HEADER_FORMAT, HEADER_FORMAT_SIZE);
}

}  // namespace storage
}  // namespace grnxx
