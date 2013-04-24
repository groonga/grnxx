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
#include "grnxx/storage/chunk.hpp"

#include "grnxx/storage/chunk-posix.hpp"
#include "grnxx/storage/chunk-windows.hpp"
#include "grnxx/string_builder.hpp"

namespace grnxx {
namespace storage {

#define GRNXX_FLAGS_WRITE(flag) do { \
  if (flags & flag) { \
    if (!is_first) { \
      builder << " | "; \
    } \
    builder << #flag; \
    is_first = false; \
  } \
} while (false)

StringBuilder &operator<<(StringBuilder &builder, ChunkFlags flags) {
  bool is_first = true;
  GRNXX_FLAGS_WRITE(CHUNK_ANONYMOUS);
  GRNXX_FLAGS_WRITE(CHUNK_HUGE_TLB);
  GRNXX_FLAGS_WRITE(CHUNK_READ_ONLY);
  if (is_first) {
    builder << "CHUNK_DEFAULT";
  }
  return builder;
}

Chunk::Chunk() {}
Chunk::~Chunk() {}

Chunk *Chunk::create(File *file, int64_t offset, int64_t size,
                     ChunkFlags flags) {
  return ChunkImpl::create(file, offset, size, flags);
}

}  // namespace storage
}  // namespace grnxx
