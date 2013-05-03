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
#ifndef GRNXX_STORAGE_CHUNK_INDEX_HPP
#define GRNXX_STORAGE_CHUNK_INDEX_HPP

#include "grnxx/basic.hpp"

namespace grnxx {
namespace storage {

constexpr size_t CHUNK_INDEX_SIZE = 32;

enum ChunkIndexType : uint8_t {
  HEADER_CHUNK       = 0,
  REGULAR_BODY_CHUNK = 1,
  SMALL_BODY_CHUNK   = 2
};

struct ChunkIndex {
  // The chunk ID.
  uint16_t id;
  // The chunk type.
  ChunkIndexType type;
  uint8_t reserved_0;
  // The ID of the file to which the chunk belongs.
  uint16_t file_id;
  uint16_t reserved_1;
  // The offset in file.
  uint64_t offset;
  // The chunk size.
  uint64_t size;
  uint64_t reserved_2;

  ChunkIndex(uint16_t id, ChunkIndexType type);
};

static_assert(sizeof(ChunkIndex) == CHUNK_INDEX_SIZE,
              "sizeof(ChunkIndex) != CHUNK_INDEX_SIZE");

}  // namespace storage
}  // namespace grnxx

#endif  // GRNXX_STORAGE_CHUNK_INDEX_HPP
