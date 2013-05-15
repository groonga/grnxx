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
#include "grnxx/storage/chunk-windows.hpp"

#ifdef GRNXX_WINDOWS

#include <new>

#include "grnxx/error.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/storage/file.hpp"

namespace grnxx {
namespace storage {

ChunkImpl::ChunkImpl()
    : flags_(CHUNK_DEFAULT),
      handle_(nullptr),
      address_(nullptr),
      size_(0) {}

ChunkImpl::~ChunkImpl() {
  if (address_) {
    if (!::UnmapViewOfFile(address_)) {
      GRNXX_ERROR() << "failed to unmap chunk"
                    << ": '::UnmapViewOfFile' " << Error(::GetLastError());
    }
  }
  if (handle_) {
    if (!::CloseHandle(handle_)) {
      GRNXX_ERROR() << "failed to close file mapping"
                    << ": '::CloseHandle' " << Error(::GetLastError());
    }
  }
}

ChunkImpl *ChunkImpl::create(File *file, uint64_t offset, uint64_t size,
                             ChunkFlags flags) {
  std::unique_ptr<ChunkImpl> chunk(new (std::nothrow) ChunkImpl);
  if (!chunk) {
    GRNXX_ERROR() << "new grnxx::storage::FileImpl failed";
    return nullptr;
  }
  if (file) {
    if (!chunk->create_file_backed_chunk(file, offset, size, flags)) {
      return nullptr;
    }
  } else {
    if (!chunk->create_anonymous_chunk(size, flags)) {
      return nullptr;
    }
  }
  return chunk.release();
}

bool ChunkImpl::sync(uint64_t offset, uint64_t size) {
  if ((flags_ & CHUNK_ANONYMOUS) || (flags_ & CHUNK_READ_ONLY)) {
    GRNXX_WARNING() << "invalid operation: flags = " << flags_;
    return false;
  }
  if ((offset > size_) || (size > size_) || (size > (size_ - offset))) {
    GRNXX_ERROR() << "invalid argument: offset = " << offset
                  << ", size = " << size << ", chunk_size = " << size_;
    return false;
  }
  if (size > std::numeric_limits<SIZE_T>::max()) {
    GRNXX_ERROR() << "invalid argument: size = " << size;
    return false;
  }
  if (!::FlushViewOfFile(static_cast<char *>(address_) + offset, size)) {
    GRNXX_ERROR() << "failed to sync chunk: offset = " << offset
                  << ", size = " << size
                  << ": '::FlushViewOfFile' " << Error(::GetLastError());
    return false;
  }
  return true;
}

bool ChunkImpl::create_file_backed_chunk(File *file, uint64_t offset,
                                         uint64_t size, ChunkFlags flags) {
  const uint64_t file_size = file->size();
  if ((offset >= file_size) || (size > file_size) ||
      (size > (file_size - offset))) {
    GRNXX_ERROR() << "invalid argument: offset = " << offset
                  << ", size = " << size << ", file_size = " << file_size;
    return false;
  }
  if (size == 0) {
    size = file_size - offset;
  }
  if (size > std::numeric_limits<SIZE_T>::max()) {
    GRNXX_ERROR() << "invalid argument: size = " << size;
    return false;
  }
  if (file->flags() & FILE_READ_ONLY) {
    flags_ |= CHUNK_READ_ONLY;
  }
  size_ = size;
  int protection_mode = PAGE_READWRITE;
  DWORD desired_access = FILE_MAP_WRITE;
  if (flags_ & CHUNK_READ_ONLY) {
    protection_mode = PAGE_READONLY;
    desired_access = FILE_MAP_READ;
  }
  const DWORD size_high = static_cast<DWORD>((offset + size) >> 32);
  const DWORD size_low = static_cast<DWORD>(offset + size);
  handle_ = ::CreateFileMapping(*static_cast<const HANDLE *>(file->handle()),
                                nullptr, protection_mode, size_high, size_low,
                                nullptr);
  if (!handle_) {
    GRNXX_ERROR() << "failed to create file mapping: "
                  << "file_path = " << file->path()
                  << ", file_size = " << file_size << ", offset = " << offset
                  << ", size = " << size << ", flags = " << flags
                  << ": '::CreateFileMapping' " << Error(::GetLastError());
    return false;
  }
  const DWORD offset_high = static_cast<DWORD>(offset >> 32);
  const DWORD offset_low = static_cast<DWORD>(offset);
  address_ = ::MapViewOfFile(handle_, desired_access, offset_high, offset_low,
                             static_cast<SIZE_T>(size));
  if (!address_) {
    GRNXX_ERROR() << "failed to map chunk: "
                  << "file_path = " << file->path()
                  << ", file_size = " << file_size << ", offset = " << offset
                  << ", size = " << size << ", flags = " << flags
                  << ": '::MapViewOfFile' " << Error(::GetLastError());
    return false;
  }
  return true;
}

bool ChunkImpl::create_anonymous_chunk(uint64_t size, ChunkFlags flags) {
  if (size == 0) {
    GRNXX_ERROR() << "invalid argument: size = " << size;
    return false;
  }
  flags_ = CHUNK_ANONYMOUS;
  size_ = size;
  const DWORD size_high = static_cast<DWORD>(size >> 32);
  const DWORD size_low = static_cast<DWORD>(size);
  handle_ = ::CreateFileMapping(INVALID_HANDLE_VALUE, nullptr, PAGE_READWRITE,
                                size_high, size_low, nullptr);
  if (!handle_) {
    GRNXX_ERROR() << "failed to create anonymous file mapping: "
                  << "size = " << size << ", flags = " << flags
                  << ": '::CreateFileMapping' " << Error(::GetLastError());
    return false;
  }
  address_ = ::MapViewOfFile(handle_, FILE_MAP_WRITE, 0, 0, 0);
  if (!address_) {
    GRNXX_ERROR() << "failed to map anonymous chunk: "
                  << "size = " << size << ", flags = " << flags
                  << ": '::MapViewOfFile' " << Error(::GetLastError());
    return false;
  }
  return true;
}

}  // namespace storage
}  // namespace grnxx

#endif  // GRNXX_WINDOWS
