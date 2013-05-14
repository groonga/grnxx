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
#ifndef GRNXX_STORAGE_CHUNK_WINDOWS_HPP
#define GRNXX_STORAGE_CHUNK_WINDOWS_HPP

#include "grnxx/features.hpp"

#ifdef GRNXX_WINDOWS

#include <windows.h>

#include "grnxx/storage/chunk.hpp"

// FILE_READ_ONLY is defined as a macro in windows.h.
#ifdef FILE_READ_ONLY
# undef FILE_READ_ONLY
#endif  // FILE_READ_ONLY

namespace grnxx {
namespace storage {

class ChunkImpl : public Chunk {
 public:
  ChunkImpl();
  ~ChunkImpl();

  static ChunkImpl *create(File *file, int64_t offset, int64_t size,
                           ChunkFlags flags);

  bool sync(int64_t offset, int64_t size);

  ChunkFlags flags() const;
  void *address() const;
  int64_t size() const;

 private:
  ChunkFlags flags_;
  HANDLE handle_;
  void *address_;
  uint64_t size_;

  bool create_file_backed_chunk(File *file, int64_t offset, int64_t size,
                                ChunkFlags flags);
  bool create_anonymous_chunk(int64_t size, ChunkFlags flags);
};

}  // namespace storage
}  // namespace grnxx

#endif  // GRNXX_WINDOWS

#endif  // GRNXX_STORAGE_CHUNK_WINDOWS_HPP
