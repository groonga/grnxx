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
#ifndef GRNXX_IO_CHUNK_HPP
#define GRNXX_IO_CHUNK_HPP

#include "grnxx/io/block.hpp"
#include "grnxx/io/view.hpp"

namespace grnxx {
namespace io {

constexpr uint8_t  CHUNK_UNIT_SIZE_BITS = 16;
constexpr uint64_t CHUNK_UNIT_SIZE      = uint64_t(1) << CHUNK_UNIT_SIZE_BITS;

constexpr uint8_t  CHUNK_INFO_SIZE_BITS = 4;
constexpr uint64_t CHUNK_INFO_SIZE      = uint64_t(1) << CHUNK_INFO_SIZE_BITS;

constexpr uint16_t CHUNK_MAX_ID     = 0xFFFE;
constexpr uint16_t CHUNK_INVALID_ID = 0xFFFF;

constexpr uint64_t CHUNK_MAX_OFFSET =
    uint64_t(0xFFFFFFFFU) << CHUNK_UNIT_SIZE_BITS;
constexpr uint64_t CHUNK_MAX_SIZE   = BLOCK_MAX_OFFSET;

class Chunk {
 public:
  Chunk() : view_(), address_(nullptr) {}

  Chunk &operator=(View *view) {
    view_.reset(view);
    address_ = view->address();
    return *this;
  }

  // Return true iff its view is available.
  explicit operator bool() const {
    return static_cast<bool>(view_);
  }

  // Return the associated view of the chunk.
  const View &view() const {
    return *view_;
  }
  // Return the address of the chunk.
  void *address() const {
    return address_;
  }

  StringBuilder &write_to(StringBuilder &builder) const {
    return builder << *view_;
  }

 private:
  std::unique_ptr<View> view_;
  void *address_;

  Chunk(const Chunk &);
  Chunk &operator=(const Chunk &);
};

inline StringBuilder &operator<<(StringBuilder &builder, const Chunk &chunk) {
  return chunk.write_to(builder);
}

class ChunkInfo {
 public:
  ChunkInfo() : id_(0), file_id_(0), offset_(0), size_(0), reserved_(0) {}

  explicit operator bool() const {
    return size_ != 0;
  }

  // Return the ID of the chunk.
  uint16_t id() const {
    return id_;
  }
  // Return the ID of the file that contains the chunk.
  uint16_t file_id() const {
    return file_id_;
  }
  // Return the offset of the chunk in the file (bytes).
  uint64_t offset() const {
    return static_cast<uint64_t>(offset_) << CHUNK_UNIT_SIZE_BITS;
  }
  // Return the size of the chunk (bytes).
  uint64_t size() const {
    return static_cast<uint64_t>(size_) << CHUNK_UNIT_SIZE_BITS;
  }

  void set_id(uint16_t value) {
    id_ = value;
  }
  void set_file_id(uint16_t value) {
    file_id_ = value;
  }
  void set_offset(uint64_t value) {
    offset_ = static_cast<uint32_t>(value >> CHUNK_UNIT_SIZE_BITS);
  }
  void set_size(uint64_t value) {
    size_ = static_cast<uint32_t>(value >> CHUNK_UNIT_SIZE_BITS);
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  uint16_t id_;
  uint16_t file_id_;
  uint32_t offset_;
  uint32_t size_;
  uint32_t reserved_;
};

static_assert(sizeof(ChunkInfo) == CHUNK_INFO_SIZE,
              "sizeof(ChunkInfo) != CHUNK_INFO_SIZE");

inline StringBuilder &operator<<(StringBuilder &builder,
                                 const ChunkInfo &chunk_info) {
  return chunk_info.write_to(builder);
}

}  // namespace io
}  // namespace grnxx

#endif  // GRNXX_IO_CHUNK_HPP
