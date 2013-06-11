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
#ifndef GRNXX_MAP_DOUBLE_ARRAY_NODE_HPP
#define GRNXX_MAP_DOUBLE_ARRAY_NODE_HPP

#include "grnxx/features.hpp"

#include "grnxx/types.hpp"

namespace grnxx {
namespace map {
namespace double_array {

constexpr uint64_t NODE_TERMINAL_LABEL = 0x100;
constexpr uint64_t NODE_MAX_LABEL      = NODE_TERMINAL_LABEL;
constexpr uint64_t NODE_INVALID_LABEL  = NODE_MAX_LABEL + 1;
constexpr uint64_t NODE_INVALID_OFFSET = 0;

// The internal structure is as follows:
// - Common
//      62 ( 1): is_phantom
//      63 ( 1): is_origin
// - Phantom: is_phantom
//    0- 8 ( 9): next
//    9-17 ( 9): prev
//   18-61 (44): reserved
// - NonPhantom: !is_phantom
//    0- 8 ( 9): label
//      60 ( 1): has_sibling
//      61 ( 1): is_leaf
// - Leaf: !is_phantom && is_leaf
//    9-48 (40): key_id
//   49-59 (11): reserved
// - NonLeaf: !is_phantom && !is_leaf
//    9-17 ( 9): child
//   18-59 (42): offset
// where 0 is the LSB and 63 is the MSB.

class Node {
  static constexpr uint64_t IS_PHANTOM_FLAG  = 1ULL << 62;
  static constexpr uint64_t IS_ORIGIN_FLAG   = 1ULL << 63;

  static constexpr uint64_t NEXT_MASK        = (1ULL << 9) - 1;
  static constexpr uint8_t  NEXT_SHIFT       = 0;
  static constexpr uint64_t PREV_MASK        = (1ULL << 9) - 1;
  static constexpr uint8_t  PREV_SHIFT       = 9;

  static constexpr uint64_t LABEL_MASK       = (1ULL << 9) - 1;
  static constexpr uint8_t  LABEL_SHIFT      = 0;
  static constexpr uint64_t HAS_SIBLING_FLAG = 1ULL << 60;
  static constexpr uint64_t IS_LEAF_FLAG     = 1ULL << 61;

  static constexpr uint64_t KEY_ID_MASK      = (1ULL << 40) - 1;
  static constexpr uint8_t  KEY_ID_SHIFT     = 9;

  static constexpr uint64_t CHILD_MASK       = (1ULL << 9) - 1;
  static constexpr uint8_t  CHILD_SHIFT      = 9;
  static constexpr uint64_t OFFSET_MASK      = (1ULL << 42) - 1;
  static constexpr uint8_t  OFFSET_SHIFT     = 18;

 public:
  Node() = default;

  // Create a phantom node.
  static Node phantom_node(uint64_t next, uint64_t prev) {
    return Node(IS_PHANTOM_FLAG | ((next & NEXT_MASK) << NEXT_SHIFT) |
                                  ((prev & PREV_MASK) << PREV_SHIFT));
  }

  // Return true iff this node is a phantom node.
  bool is_phantom() const {
    return value_ & IS_PHANTOM_FLAG;
  }
  // Return true iff the ID of this node is used as an offset.
  bool is_origin() const {
    return value_ & IS_ORIGIN_FLAG;
  }

  // Return the ID of the next phantom node in the same block.
  uint64_t next() const {
    return (value_ >> NEXT_SHIFT) & NEXT_MASK;
  }
  // Return the ID of the prev phantom node in the same block.
  uint64_t prev() const {
    return (value_ >> PREV_SHIFT) & PREV_MASK;
  }

  // Return the label.
  // Note that a phantom node returns an invalid label.
  uint64_t label() const {
    return (value_ >> LABEL_SHIFT) &
           ((IS_PHANTOM_FLAG >> LABEL_SHIFT) | LABEL_MASK);
  }
  // Return true iff this node has a sibling with a greater label.
  bool has_sibling() const {
    return value_ & HAS_SIBLING_FLAG;
  }
  // Return true iff this node is a leaf node.
  bool is_leaf() const {
    return value_ & IS_LEAF_FLAG;
  }

  // Return the associated key ID.
  uint64_t key_id() const {
    return (value_ >> KEY_ID_SHIFT) & KEY_ID_MASK;
  }

  // Return the ID of the child node with the least label.
  uint64_t child() const {
    return (value_ >> CHILD_SHIFT) & CHILD_MASK;
  }
  // Return the offset to child nodes.
  uint64_t offset() const {
    return (value_ >> OFFSET_SHIFT) & OFFSET_MASK;
  }

  void unset_is_phantom() {
    value_ = (value_ & IS_ORIGIN_FLAG) |
             (NODE_INVALID_LABEL << LABEL_SHIFT) |
             (NODE_INVALID_LABEL << CHILD_SHIFT) |
             (NODE_INVALID_OFFSET << OFFSET_SHIFT);
  }
  void set_is_origin(bool is_origin) {
    if (is_origin) {
      value_ |= IS_ORIGIN_FLAG;
    } else {
      value_ &= ~IS_ORIGIN_FLAG;
    }
  }

  void set_next(uint64_t next) {
    value_ = (value_ & ~(NEXT_MASK << NEXT_SHIFT)) |
             ((next & NEXT_MASK) << NEXT_SHIFT);
  }
  void set_prev(uint64_t prev) {
    value_ = (value_ & ~(PREV_MASK << PREV_SHIFT)) |
             ((prev & PREV_MASK) << PREV_SHIFT);
  }
  void set_next_and_prev(uint64_t next, uint64_t prev) {
    constexpr uint64_t NEXT_AND_PREV_MASK =
        (NEXT_MASK << NEXT_SHIFT) | (PREV_MASK << PREV_SHIFT);
    value_ = (value_ & ~NEXT_AND_PREV_MASK) |
             ((next & NEXT_MASK) << NEXT_SHIFT) |
             ((prev & PREV_MASK) << PREV_SHIFT);
  }

  void set_label(uint64_t label) {
    value_ = (value_ & ~(LABEL_MASK << LABEL_SHIFT)) |
             ((label & LABEL_MASK) << LABEL_SHIFT);
  }
  void set_has_sibling() {
    value_ |= HAS_SIBLING_FLAG;
  }
  // set_is_leaf() is not provided because set_key_id() sets IS_LEAF_FLAG.

  void set_key_id(uint64_t key_id) {
    value_ = (value_ & ~(KEY_ID_MASK << KEY_ID_SHIFT)) | IS_LEAF_FLAG |
             ((key_id & KEY_ID_MASK) << KEY_ID_SHIFT);
  }

  void set_child(uint64_t child) {
    value_ = (value_ & ~(CHILD_MASK << CHILD_SHIFT)) |
             ((child & CHILD_MASK) << CHILD_SHIFT);
  }
  void set_offset(uint64_t offset) {
    if (value_ & IS_LEAF_FLAG) {
      value_ = (value_ & ~(IS_LEAF_FLAG | (OFFSET_MASK << OFFSET_SHIFT) |
                           (CHILD_MASK << CHILD_SHIFT))) |
               (offset << OFFSET_SHIFT) |
               (NODE_INVALID_LABEL << CHILD_SHIFT);
    } else {
      value_ = (value_ & ~(OFFSET_MASK << OFFSET_SHIFT)) |
               (offset << OFFSET_SHIFT);
    }
  }

 private:
  uint64_t value_;

  explicit Node(uint64_t value) : value_(value) {}
};

}  // namespace double_array
}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_DOUBLE_ARRAY_NODE_HPP
