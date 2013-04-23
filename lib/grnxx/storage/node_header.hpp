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
#ifndef GRNXX_STORAGE_NODE_HEADER_HPP
#define GRNXX_STORAGE_NODE_HEADER_HPP

#include "grnxx/storage.hpp"

namespace grnxx {
namespace storage {

using NodeStatus = StorageNodeStatus;

struct NodeHeader {
  // The ID of this node.
  uint32_t id;
  // The status of this node.
  NodeStatus status;
  // (Non-phantom)
  // For calculating the actual offset and size, see also "offset" and "size".
  uint8_t bits;
  // (Non-phantom)
  // The ID of the chunk to which this node belongs.
  uint16_t chunk_id;
  // (Non-phantom)
  // The offset of this node in chunk.
  // The actual offset is "offset" << "bits".
  uint32_t offset;
  // (Non-phantom)
  // The size of this node. The actual size is "size" << "bits".
  uint32_t size;
  // (Non-phantom)
  // The ID of the next node in chunk.
  // INVALID_ID indicates that this node is the last node in chunk.
  uint32_t next_node_id;
  // (Non-phantom)
  // The ID of the previous node in chunk.
  // INVALID_ID indicates that this node is the first node in chunk.
  uint32_t prev_node_id;
  union {
    // (Phantom)
    // The ID of the next phantom node.
    uint32_t next_phantom_node_id;
    // (Active, marked, or unlinked)
    // The ID of the latest child node.
    // INVALID_ID indicates that this node has no children.
    uint32_t child_id;
    // (Idle)
    // The ID of the next idle node.
    uint32_t next_idle_node_id;
  };
  union {
    // (Active or marked)
    // The ID of the next sibling node.
    // INVALID_ID indicates that this node has no elder siblings.
    uint32_t sibling_node_id;
    // (Unlinked)
    // The ID of the next unlinked node.
    // INVALID_ID indicates that this node is the last unlinked node.
    uint32_t next_unlinked_node_id;
    // (Idle)
    // The ID of the previous idle node.
    uint32_t prev_idle_node_id;
  };
  // The last modified time.
  Time modified_time;
  // Reserved for future use.
  uint8_t reserved[8];
  // User data.
  uint8_t user_data[16];

  // Initialize the members.
  NodeHeader();
};

}  // namespace storage
}  // namespace grnxx

#endif  // GRNXX_STORAGE_NODE_HEADER_HPP
