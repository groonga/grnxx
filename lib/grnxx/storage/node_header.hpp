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

#include "grnxx/features.hpp"

#include "grnxx/storage.hpp"
#include "grnxx/time.hpp"
#include "grnxx/types.hpp"

namespace grnxx {
namespace storage {

constexpr size_t NODE_HEADER_SIZE = 64;

struct NodeHeader {
  // The node ID.
  uint32_t id;
  // The node status.
  StorageNodeStatus status;
  uint8_t reserved_0;
  // (Non-phantom)
  // The ID of the chunk to which the node belongs.
  uint16_t chunk_id;
  // (Non-phantom)
  // The offset in chunk.
  uint64_t offset;
  // (Non-phantom)
  // The body size.
  uint64_t size;
  // (Non-phantom)
  // The ID of the next node in chunk.
  // STORAGE_INVALID_NODE_ID indicates that the node is the last node in chunk.
  uint32_t next_node_id;
  // (Non-phantom)
  // The ID of the previous node in chunk.
  // STORAGE_INVALID_NODE_ID indicates that the node is the first node in chunk.
  uint32_t prev_node_id;
  // (Active)
  // The ID of the from node.
  // STORAGE_INVALID_NODE_ID indicates that the node is the root node.
  uint32_t from_node_id;
  uint32_t reserved_1;
  union {
    // (Phantom)
    // The ID of the next phantom node.
    uint32_t next_phantom_node_id;
    // (Active or unlinked)
    // The ID of the latest child node.
    // STORAGE_INVALID_NODE_ID indicates that the node has no children.
    uint32_t child_node_id;
    // (Idle)
    // The ID of the next idle node.
    // id == next_idle_node_id indicates that the node is the only member of
    // an idle node list.
    uint32_t next_idle_node_id;
  };
  union {
    // (Active)
    // The ID of the next sibling node.
    // STORAGE_INVALID_NODE_ID indicates that the node has no elder siblings.
    uint32_t sibling_node_id;
    // (Unlinked)
    // The ID of the next unlinked node.
    // STORAGE_INVALID_NODE_ID indicates that the node is the last unlinked
    // node.
    uint32_t next_unlinked_node_id;
    // (Idle)
    // The ID of the previous idle node.
    // id == prev_idle_node_id indicates that the node is the only member of
    // an idle node list.
    uint32_t prev_idle_node_id;
  };
  // The last modified time.
  Time modified_time;
  // User data.
  uint8_t user_data[8];

  // Initialize the members.
  explicit NodeHeader(uint32_t id);
};

static_assert(sizeof(NodeHeader) == NODE_HEADER_SIZE,
              "sizeof(NodeHeader) != NODE_HEADER_SIZE");

}  // namespace storage
}  // namespace grnxx

#endif  // GRNXX_STORAGE_NODE_HEADER_HPP
