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
#ifndef GRNXX_STORAGE_HEADER_HPP
#define GRNXX_STORAGE_HEADER_HPP

#include "grnxx/features.hpp"

#include "grnxx/common_header.hpp"
#include "grnxx/mutex.hpp"
#include "grnxx/types.hpp"

namespace grnxx {
namespace storage {

// The number of bytes allocated to a header.
// sizeof(Header) must not be greater than this value.
constexpr size_t HEADER_SIZE         = 512;

constexpr size_t NUM_IDLE_NODE_LISTS = 64;

struct Header {
  // The file format and the grnxx version.
  CommonHeader common_header;
  // The maximum size of each file.
  uint64_t max_file_size;
  // The maximum number of files.
  uint16_t max_num_files;
  // The number of body chunks.
  uint16_t num_body_chunks;
  // The number of small body chunks.
  uint16_t num_small_body_chunks;
  uint16_t reserved_0;
  // The number of nodes.
  uint32_t num_nodes;
  // The number of active or unlinked nodes.
  uint32_t num_active_or_unlinked_nodes;
  // The upper limit of the number of nodes.
  // This value is extended when a node header chunk is added.
  uint32_t max_num_nodes;
  uint32_t reserved_1;
  // The total usage of body chunks.
  uint64_t body_usage;
  // The total size of body chunks.
  uint64_t body_size;
  // The total size including headers.
  uint64_t total_size;
  // The ID of the latest phantom node.
  // STORAGE_INVALID_NODE_ID indicates that there are no phantom nodes.
  uint32_t latest_phantom_node_id;
  // The ID of the latest unlinked node.
  // STORAGE_INVALID_NODE_ID indicates that there are no unlinked nodes.
  uint32_t latest_unlinked_node_id;
  // The IDs of the oldest idle nodes.
  // STORAGE_INVALID_NODE_ID indicates that the idle node list is empty.
  uint32_t oldest_idle_node_ids[NUM_IDLE_NODE_LISTS];
  // A mutex object for exclusively updating data.
  Mutex data_mutex;
  // A mutex object for exclusively update files.
  Mutex file_mutex;
  uint8_t reserved_2[88];

  // Initialize the members except "format".
  Header();

  // Return true if the header seems to be correct.
  explicit operator bool() const;

  // Initialize "format".
  void validate();
};

static_assert(sizeof(Header) == HEADER_SIZE, "sizeof(Header) != HEADER_SIZE");

}  // namespace storage
}  // namespace grnxx

#endif  // GRNXX_STORAGE_HEADER_HPP
