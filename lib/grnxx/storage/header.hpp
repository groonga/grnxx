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

#include "grnxx/basic.hpp"
#include "grnxx/mutex.hpp"

namespace grnxx {
namespace storage {

// The number of bytes allocated to a header.
// sizeof(Header) must not be greater than this value.
constexpr size_t HEADER_SIZE         = 512;

// The buffer size allocated for the format identifier.
constexpr size_t HEADER_FORMAT_SIZE  = 32;
// The buffer size allocated for the version string.
constexpr size_t HEADER_VERSION_SIZE = 32;

struct Header {
  // The identifier for checking the file format.
  char format[HEADER_FORMAT_SIZE];
  // The grnxx version.
  char version[HEADER_VERSION_SIZE];
  // The maximum number of files.
  uint32_t max_num_files;
  // The maximum size of each file.
  uint64_t max_file_size;
  // The total size including headers.
  uint64_t total_size;
  // The number of files.
  uint32_t num_files;
  // The number of node chunks.
  uint32_t num_node_chunks;
  // The number of nodes.
  uint32_t num_nodes;
  // The upper limit of the number of nodes.
  // This value is extended when a node header chunk is added.
  uint32_t max_num_nodes;
  // The ID of the latest phantom node.
  uint32_t latest_phantom_node_id;
  // The ID of the latest unlinked node.
  uint32_t latest_unlinked_node_id;
  // The IDs of the oldest idle nodes.
  uint32_t oldest_idle_node_ids[64];
  // A mutex object for exclusively updating data.
  Mutex inter_process_data_mutex;
  // A mutex object for exclusively update files.
  Mutex inter_process_file_mutex;

  // Initialize the members.
  Header();

  // Return true if the header seems to be correct.
  bool is_valid() const;
};

static_assert(sizeof(Header) <= HEADER_SIZE, "sizeof(Header) > HEADER_SIZE");

}  // namespace storage
}  // namespace grnxx

#endif  // GRNXX_STORAGE_HEADER_HPP
