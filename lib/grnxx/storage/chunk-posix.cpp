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
#include "grnxx/storage/chunk-posix.hpp"

#ifndef GRNXX_WINDOWS

#include <sys/mman.h>

#include <cerrno>
#include <limits>
#include <memory>
#include <new>

#include "grnxx/errno.hpp"
#include "grnxx/exception.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/storage/file.hpp"

#ifndef MAP_ANONYMOUS
# ifdef MAP_ANON
#  define MAP_ANONYMOUS MAP_ANON
# endif  // MAP_ANON
#endif  // MAP_ANONYMOUS

namespace grnxx {
namespace storage {

ChunkImpl::ChunkImpl()
    : flags_(CHUNK_DEFAULT),
      address_(MAP_FAILED),
      size_(0) {}

ChunkImpl::~ChunkImpl() {
  if (address_ != MAP_FAILED) {
    if (::munmap(address_, static_cast<size_t>(size_)) != 0) {
      Errno errno_copy(errno);
      GRNXX_WARNING() << "failed to unmap chunk: "
                      << "call = ::munmap, errno = " << errno_copy;
    }
  }
}

ChunkImpl *ChunkImpl::create(File *file, uint64_t offset, uint64_t size,
                             ChunkFlags flags) {
  std::unique_ptr<ChunkImpl> chunk(new (std::nothrow) ChunkImpl);
  if (!chunk) {
    GRNXX_ERROR() << "new grnxx::storage::FileImpl failed";
    throw MemoryError();
  }
  if (file) {
    chunk->create_file_backed_chunk(file, offset, size, flags);
  } else {
    chunk->create_anonymous_chunk(size, flags);
  }
  return chunk.release();
}

void ChunkImpl::sync(uint64_t offset, uint64_t size) {
  if ((flags_ & CHUNK_ANONYMOUS) || (flags_ & CHUNK_READ_ONLY)) {
    GRNXX_ERROR() << "invalid operation: flags = " << flags_;
    throw LogicError();
  }
  if ((offset > size_) || (size > size_) || (size > (size_ - offset))) {
    GRNXX_ERROR() << "invalid argument: offset = " << offset
                  << ", size = " << size << ", chunk_size = " << size_;
    throw LogicError();
  }
  if (size == 0) {
    size = size_ - offset;
  }
  if (size > std::numeric_limits<size_t>::max()) {
    GRNXX_ERROR() << "invalid argument: size = " << size;
    throw LogicError();
  }
  if (size > 0) {
    if (::msync(static_cast<char *>(address_) + offset, size, MS_SYNC) != 0) {
      Errno errno_copy(errno);
      GRNXX_ERROR() << "failed to sync chunk: offset = " << offset
                    << ", size = " << size
                    << ", call = ::msync, errno = " << errno_copy;
      throw SystemError(errno_copy);
    }
  }
}

ChunkFlags ChunkImpl::flags() const {
  return flags_;
}

void *ChunkImpl::address() const {
  return address_;
}

uint64_t ChunkImpl::size() const {
  return size_;
}

void ChunkImpl::create_file_backed_chunk(File *file, uint64_t offset,
                                         uint64_t size, ChunkFlags flags) {
  const uint64_t file_size = file->get_size();
  if ((offset >= file_size) || (size > file_size) ||
      (size > (file_size - offset))) {
    GRNXX_ERROR() << "invalid argument: offset = " << offset
                  << ", size = " << size << ", file_size = " << file_size;
    throw LogicError();
  }
  if (size == 0) {
    size = file_size - offset;
  }
  if ((offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) ||
      (size > std::numeric_limits<size_t>::max())) {
    GRNXX_ERROR() << "invalid argument: offset = " << offset
                  << ", size = " << size;
    throw LogicError();
  }
  if (file->flags() & FILE_READ_ONLY) {
    flags_ |= CHUNK_READ_ONLY;
  }
  size_ = size;
  int protection_flags = PROT_READ | PROT_WRITE;
  if (flags_ & CHUNK_READ_ONLY) {
    flags_ |= CHUNK_READ_ONLY;
    protection_flags = PROT_READ;
  }
  const int mmap_flags = MAP_SHARED;
  address_ = ::mmap(nullptr, size, protection_flags, mmap_flags,
                    *static_cast<const int *>(file->handle()), offset);
  if (address_ == MAP_FAILED) {
    Errno errno_copy(errno);
    GRNXX_ERROR() << "failed to map file-backed chunk: "
                  << "file_path = " << file->path()
                  << ", file_size = " << file_size
                  << ", offset = " << offset << ", size = " << size
                  << ", flags = " << flags
                  << ", call = ::mmap, errno = " << errno_copy;
    throw SystemError(errno_copy);
  }
}

void ChunkImpl::create_anonymous_chunk(uint64_t size, ChunkFlags flags) {
  if ((size == 0) || (size > std::numeric_limits<size_t>::max())) {
    GRNXX_ERROR() << "invalid argument: size = " << size;
    throw LogicError();
  }
  flags_ = CHUNK_ANONYMOUS;
  size_ = size;
  const int protection_flags = PROT_READ | PROT_WRITE;
  const int mmap_flags = MAP_PRIVATE | MAP_ANONYMOUS;
#ifdef MAP_HUGETLB
  if (flags & CHUNK_HUGE_TLB) {
    address_ = ::mmap(nullptr, size, protection_flags,
                      mmap_flags | MAP_HUGETLB, -1, 0);
    if (address_ != MAP_FAILED) {
      flags_ |= CHUNK_HUGE_TLB;
    }
  }
#endif  // MAP_HUGETLB
  if (address_ == MAP_FAILED) {
    address_ = ::mmap(nullptr, size, protection_flags, mmap_flags, -1, 0);
    if (address_ == MAP_FAILED) {
      Errno errno_copy(errno);
      GRNXX_ERROR() << "failed to map anonymous chunk: size = " << size
                    << ", flags = " << flags
                    << ", call = ::mmap, errno = " << errno_copy;
      throw SystemError(errno_copy);
    }
  }
}

}  // namespace storage
}  // namespace grnxx

#endif  // GRNXX_WINDOWS
