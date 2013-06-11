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
#ifndef GRNXX_MAP_DOUBLE_ARRAY_BLOCK_HPP
#define GRNXX_MAP_DOUBLE_ARRAY_BLOCK_HPP

#include "grnxx/features.hpp"

#include "grnxx/types.hpp"

namespace grnxx {
namespace map {
namespace double_array {

constexpr uint64_t BLOCK_MAX_FAILURE_COUNT  = 4;
constexpr uint64_t BLOCK_MAX_LEVEL          = 5;
constexpr uint64_t BLOCK_INVALID_ID         = (1ULL << 40) - 1;
constexpr uint64_t BLOCK_SIZE               = 1ULL << 9;
constexpr uint64_t BLOCK_MAX_COUNT          = 16;

// The internal structure is as follows:
// - values_[0]
//    0-15 (16): first_phantom
//   16-23 ( 8): level
//   24-63 (40): next
// - values_[1]
//    0-15 (16): num_phantoms
//   16-23 ( 8): failure_count
//   24-63 (40): prev
// where 0 is the LSB and 63 is the MSB.

class Block {
  // For values_[0].
  static constexpr uint64_t FIRST_PHANTOM_MASK  = (1ULL << 16) - 1;
  static constexpr uint8_t  FIRST_PHANTOM_SHIFT = 0;
  static constexpr uint64_t LEVEL_MASK          = (1ULL << 8) - 1;
  static constexpr uint8_t  LEVEL_SHIFT         = 16;
  static constexpr uint64_t NEXT_MASK           = (1ULL << 40) - 1;
  static constexpr uint8_t  NEXT_SHIFT          = 24;

  // For values_[1].
  static constexpr uint64_t NUM_PHANTOMS_MASK   = (1ULL << 16) - 1;
  static constexpr uint8_t  NUM_PHANTOMS_SHIFT  = 0;
  static constexpr uint64_t FAILURE_COUNT_MASK  = (1ULL << 8) - 1;
  static constexpr uint8_t  FAILURE_COUNT_SHIFT = 16;
  static constexpr uint64_t PREV_MASK           = (1ULL << 40) - 1;
  static constexpr uint8_t  PREV_SHIFT          = 24;

 public:
  Block() = default;

  static Block empty_block() {
    return Block(0, BLOCK_SIZE << NUM_PHANTOMS_SHIFT);
  }

  // Return the first phantom node.
  uint64_t first_phantom() const {
    return (values_[0] >> FIRST_PHANTOM_SHIFT) & FIRST_PHANTOM_MASK;
  }
  // Return the level.
  uint64_t level() const {
    return (values_[0] >> LEVEL_SHIFT) & LEVEL_MASK;
  }
  // Return the next block ID of the same level.
  uint64_t next() const {
    return (values_[0] >> NEXT_SHIFT) & NEXT_MASK;
  }

  // Return the number of phantom nodes.
  uint64_t num_phantoms() const {
    return (values_[1] >> NUM_PHANTOMS_SHIFT) & NUM_PHANTOMS_MASK;
  }
  // Return the failure count.
  uint64_t failure_count() const {
    return (values_[1] >> FAILURE_COUNT_SHIFT) & FAILURE_COUNT_MASK;
  }
  // Return the previous block ID of the same level.
  uint64_t prev() const {
    return (values_[1] >> PREV_SHIFT) & PREV_MASK;
  }

  void set_first_phantom(uint64_t first_phantom) {
    values_[0] = (values_[0] & ~(FIRST_PHANTOM_MASK << FIRST_PHANTOM_SHIFT)) |
                 ((first_phantom & FIRST_PHANTOM_MASK) << FIRST_PHANTOM_SHIFT);
  }
  void set_level(uint64_t level) {
    values_[0] = (values_[0] & ~(LEVEL_MASK << LEVEL_SHIFT)) |
                 ((level & LEVEL_MASK) << LEVEL_SHIFT);
  }
  void set_next(uint64_t next) {
    values_[0] = (values_[0] & ~(NEXT_MASK << NEXT_SHIFT)) |
                 ((next & NEXT_MASK) << NEXT_SHIFT);
  }

  void set_num_phantoms(uint64_t num_phantoms) {
    values_[1] = (values_[1] & ~(NUM_PHANTOMS_MASK << NUM_PHANTOMS_SHIFT)) |
                 ((num_phantoms & NUM_PHANTOMS_MASK) << NUM_PHANTOMS_SHIFT);
  }
  void set_failure_count(uint64_t failure_count) {
    values_[1] = (values_[1] & ~(FAILURE_COUNT_MASK << FAILURE_COUNT_SHIFT)) |
                 ((failure_count & FAILURE_COUNT_MASK) << FAILURE_COUNT_SHIFT);
  }
  void set_prev(uint64_t prev) {
    values_[1] = (values_[1] & ~(PREV_MASK << PREV_SHIFT)) |
                 ((prev & PREV_MASK) << PREV_SHIFT);
  }

 private:
  uint64_t values_[2];

  Block(uint64_t value_0, uint64_t value_1) : values_{ value_0, value_1 } {}
};

}  // namespace double_array
}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_DOUBLE_ARRAY_BLOCK_HPP
