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
#ifndef GRNXX_MAP_PATRICIA_NODE_HPP
#define GRNXX_MAP_PATRICIA_NODE_HPP

#include "grnxx/features.hpp"

#include "grnxx/bytes.hpp"
#include "grnxx/types.hpp"

namespace grnxx {
namespace map {
namespace patricia {

constexpr uint64_t NODE_INVALID_OFFSET = 0;

enum NodeStatus : uint64_t {
  NODE_DEAD     = 0,
  NODE_LEAF     = 1,
  NODE_BRANCH   = 2,
  NODE_TERMINAL = 3
};

// The internal structure is as follows:
// - Common
//   62-63 ( 2): status (DEAD, LEAF, BRANCH, TERMINAL)
// - Leaf: LEAF
//    0-39 (40): key_id
//   40-61 (22): reserved
// - Branch or Terminal: BRANCH || TERMINAL
//   16-57 (42): offset
//   58-61 ( 4): reserved
// - Branch: BRANCH
//    0-15 (16): bit_pos
// - Terminal: TERMINAL
//    0-15 (16): bit_size
// where 0 is the LSB and 63 is the MSB.

class Node {
  static constexpr uint64_t STATUS_MASK    = (1ULL << 2) - 1;
  static constexpr uint8_t  STATUS_SHIFT   = 62;

  static constexpr uint64_t KEY_ID_MASK    = (1ULL << 40) - 1;
  static constexpr uint8_t  KEY_ID_SHIFT   = 0;

  static constexpr uint64_t OFFSET_MASK    = (1ULL << 42) - 1;
  static constexpr uint8_t  OFFSET_SHIFT   = 16;

  static constexpr uint64_t BIT_POS_MASK   = (1ULL << 16) - 1;
  static constexpr uint8_t  BIT_POS_SHIFT  = 0;

  static constexpr uint64_t BIT_SIZE_MASK  = (1ULL << 16) - 1;
  static constexpr uint8_t  BIT_SIZE_SHIFT = 0;

 public:

  Node() = default;

  // Create a node that has neither descendants nor an associated key.
  static Node dead_node() {
    return Node((NODE_DEAD << STATUS_SHIFT));
  }
  // Create a node that has an associated key that is identified by "key_id".
  static Node leaf_node(int64_t key_id) {
    return Node((NODE_LEAF << STATUS_SHIFT) |
                ((key_id & KEY_ID_MASK) << KEY_ID_SHIFT));
  }
  // Create a node that works as a 0/1 branch.
  // If key["bit_pos"] == 0, the next node ID is "offset".
  // Otherwise, the next node ID is "offset" + 1.
  static Node branch_node(uint64_t bit_pos, uint64_t offset) {
    return Node((NODE_BRANCH << STATUS_SHIFT) |
                ((bit_pos & BIT_POS_MASK) << BIT_POS_SHIFT) |
                ((offset & OFFSET_MASK) << OFFSET_SHIFT));
  }
  // Create a node that works as a short/long branch.
  // If key_size <= "bit_size", the next node ID is "offset".
  // Otherwise, the next node ID is "offset" + 1.
  static Node terminal_node(uint64_t bit_size, uint64_t offset) {
    return Node((NODE_TERMINAL << STATUS_SHIFT) |
                ((bit_size & BIT_SIZE_MASK) << BIT_SIZE_SHIFT) |
                ((offset & OFFSET_MASK) << OFFSET_SHIFT));
  }

  // Return the node status.
  uint64_t status() const {
    return (value_ >> STATUS_SHIFT) & STATUS_MASK;
  }

  // Return the associated key ID.
  int64_t key_id() const {
    return (value_ >> KEY_ID_SHIFT) & KEY_ID_MASK;
  }

  // Return the offset to the next nodes.
  uint64_t offset() const {
    return (value_ >> OFFSET_SHIFT) & OFFSET_MASK;
  }

  // Return the position of the branch.
  uint64_t bit_pos() const {
    return (value_ >> BIT_POS_SHIFT) & BIT_POS_MASK;
  }

  // Return the branch condition.
  uint64_t bit_size() const {
    return (value_ >> BIT_SIZE_SHIFT) & BIT_SIZE_MASK;
  }

 private:
  uint64_t value_;

  explicit Node(uint64_t value) : value_(value) {}
};

}  // namespace patricia
}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_PATRICIA_NODE_HPP
