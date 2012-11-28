/*
  Copyright (C) 2012  Brazil, Inc.

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
#ifndef GRNXX_IO_BLOCK_HPP
#define GRNXX_IO_BLOCK_HPP

#include "../string_builder.hpp"

namespace grnxx {
namespace io {

const uint8_t  BLOCK_UNIT_SIZE_BITS = 12;
const uint64_t BLOCK_UNIT_SIZE      = uint64_t(1) << BLOCK_UNIT_SIZE_BITS;

const uint8_t  BLOCK_INFO_SIZE_BITS = 5;
const uint64_t BLOCK_INFO_SIZE      = uint64_t(1) << BLOCK_INFO_SIZE_BITS;

const uint32_t BLOCK_MAX_ID     = 0xFFFFFFFEU;
const uint32_t BLOCK_INVALID_ID = 0xFFFFFFFFU;

const uint64_t BLOCK_MAX_OFFSET = uint64_t(0xFFFFFFFFU) << BLOCK_UNIT_SIZE_BITS;
const uint64_t BLOCK_MAX_SIZE   = uint64_t(0xFFFFFFFFU) << BLOCK_UNIT_SIZE_BITS;

enum BlockStatus : uint8_t {
  BLOCK_PHANTOM = 0,
  BLOCK_ACTIVE  = 1,
  BLOCK_FROZEN  = 2,
  BLOCK_IDLE    = 3
};

class BlockInfo {
 public:
  BlockInfo()
    : id_(0), status_(BLOCK_PHANTOM), reserved_(0),
      chunk_id_(0), offset_(0), size_(0),
      next_block_id_(0), prev_block_id_(0),
      next_idle_block_id_(0), prev_idle_block_id_(0) {}

  // Return the ID of the block.
  uint32_t id() const {
    return id_;
  }
  // Return the status of the block.
  BlockStatus status() const {
    return status_;
  }
  // Return the ID of the chunk that contains the block.
  uint16_t chunk_id() const {
    return chunk_id_;
  }
  // Return the offset of the block in the chunk (bytes).
  uint64_t offset() const {
    return static_cast<uint64_t>(offset_) << BLOCK_UNIT_SIZE_BITS;
  }
  // Return the size of the block (bytes).
  uint64_t size() const {
    return static_cast<uint64_t>(size_) << BLOCK_UNIT_SIZE_BITS;
  }
  // Return the ID of the next block in the same chunk.
  // If the block is the rearmost block in the chunk, this function returns
  // io::BLOCK_INVALID_ID.
  uint32_t next_block_id() const {
    return next_block_id_;
  }
  // Return the ID of the previous block in the same chunk.
  // If the block is the first block in the chunk, this function returns
  // io::BLOCK_INVALID_ID.
  uint32_t prev_block_id() const {
    return prev_block_id_;
  }
  // Return the ID of the next (older) phantom block.
  // If the block is the oldest phantom block, this function returns
  // io::BLOCK_INVALID_ID.
  // Note: Available iff the block is a phantom block.
  uint32_t next_phantom_block_id() const {
    return next_phantom_block_id_;
  }
  // Return the ID of the next (newer) frozen block.
  // If the block is the latest frozen block, this function returns
  // the ID of the oldest frozen block.
  // Note: Available iff the block is a frozen block.
  uint32_t next_frozen_block_id() const {
    return next_frozen_block_id_;
  }
  // Return the ID of the next (newer) idle block.
  // If the block is the latest idle block, this function returns
  // the ID of the oldest idle block.
  // Note: Available iff the block is an idle block.
  uint32_t next_idle_block_id() const {
    return next_idle_block_id_;
  }
  // Return the stamp generated when the block was frozen.
  // Note; Available iff the block is a frozen block.
  uint16_t frozen_stamp() const {
    return static_cast<uint16_t>(frozen_stamp_);
  }
  // Return the ID of the previous (older) idle block.
  // If the block is the oldest idle block, this function returns
  // the ID of the latest idle block.
  // Note: Available iff the block is an idle block.
  uint32_t prev_idle_block_id() const {
    return prev_idle_block_id_;
  }

  void set_id(uint32_t value) {
    id_ = value;
  }
  void set_status(BlockStatus value) {
    status_ = value;
  }
  void set_chunk_id(uint16_t value) {
    chunk_id_ = static_cast<uint16_t>(value);
  }
  void set_offset(uint64_t value) {
    offset_ = static_cast<uint32_t>(value >> BLOCK_UNIT_SIZE_BITS);
  }
  void set_size(uint64_t value) {
    size_ = static_cast<uint32_t>(value >> BLOCK_UNIT_SIZE_BITS);
  }
  void set_next_block_id(uint32_t value) {
    next_block_id_ = value;
  }
  void set_prev_block_id(uint32_t value) {
    prev_block_id_ = value;
  }
  void set_next_phantom_block_id(uint32_t value) {
    next_phantom_block_id_ = value;
  }
  void set_next_frozen_block_id(uint32_t value) {
    next_frozen_block_id_ = value;
  }
  void set_next_idle_block_id(uint32_t value) {
    next_idle_block_id_ = value;
  }
  void set_frozen_stamp(uint16_t value) {
    frozen_stamp_ = value;
  }
  void set_prev_idle_block_id(uint32_t value) {
    prev_idle_block_id_ = value;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  uint32_t id_;
  BlockStatus status_;
  uint8_t reserved_;
  uint16_t chunk_id_;
  uint32_t offset_;
  uint32_t size_;
  uint32_t next_block_id_;
  uint32_t prev_block_id_;
  union {
    uint32_t next_phantom_block_id_;
    uint32_t next_frozen_block_id_;
    uint32_t next_idle_block_id_;
  };
  union {
    uint32_t frozen_stamp_;
    uint32_t prev_idle_block_id_;
  };
};

static_assert(sizeof(BlockInfo) == BLOCK_INFO_SIZE,
              "sizeof(BlockInfo) != BLOCK_INFO_SIZE");

StringBuilder &operator<<(StringBuilder &builder, BlockStatus status);

std::ostream &operator<<(std::ostream &stream, BlockStatus status);

inline StringBuilder &operator<<(StringBuilder &builder,
                                 const BlockInfo &block_info) {
  return block_info.write_to(builder);
}

}  // namespace io
}  // namespace grnxx

#endif  // GRNXX_IO_BLOCK_HPP
