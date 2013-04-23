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
#include "grnxx/storage/view-posix.hpp"

#ifndef GRNXX_WINDOWS

#include <sys/mman.h>
#include <cerrno>

#include "grnxx/error.hpp"
#include "grnxx/storage/file.hpp"
#include "grnxx/logger.hpp"

#ifndef MAP_ANONYMOUS
# ifdef MAP_ANON
#  define MAP_ANONYMOUS MAP_ANON
# endif  // MAP_ANON
#endif  // MAP_ANONYMOUS

namespace grnxx {
namespace storage {

ViewImpl::ViewImpl() : flags_(VIEW_DEFAULT), address_(MAP_FAILED), size_(0) {}

ViewImpl::~ViewImpl() {
  if (address_ != MAP_FAILED) {
    if (::munmap(address_, static_cast<size_t>(size_)) != 0) {
      GRNXX_ERROR() << "failed to unmap view: '::munmap' " << Error(errno);
    }
  }
}

View *ViewImpl::create(File *file, int64_t offset, int64_t size,
                       ViewFlags flags) {
  std::unique_ptr<ViewImpl> view(new (std::nothrow) ViewImpl);
  if (!view) {
    GRNXX_ERROR() << "new grnxx::storage::FileImpl failed";
    return nullptr;
  }
  if (file) {
    if (!view->create_file_backed_view(file, offset, size, flags)) {
      return nullptr;
    }
  } else {
    if (!view->create_anonymous_view(size, flags)) {
      return nullptr;
    }
  }
  return view.release();
}

bool ViewImpl::sync(int64_t offset, int64_t size) {
  if ((flags_ & VIEW_ANONYMOUS) || (flags_ & VIEW_READ_ONLY)) {
    GRNXX_WARNING() << "invalid operation: flags = " << flags_;
    return false;
  }
  if ((offset < 0) || (offset > size_) || (size > size_) ||
      ((size >= 0) && (size > (size_ - offset)))) {
    GRNXX_ERROR() << "invalid argument: offset = " << offset
                  << ", size = " << size << ", view_size = " << size_;
    return false;
  }
  if (size < 0) {
    size = size_ - offset;
  }
  if (size > 0) {
    if (::msync(static_cast<char *>(address_) + offset, size, MS_SYNC) != 0) {
      GRNXX_ERROR() << "failed to sync view: offset = " << offset
                    << ", size = " << size << ": '::msync' " << Error(errno);
      return false;
    }
  }
  return true;
}

ViewFlags ViewImpl::flags() const {
  return flags_;
}

void *ViewImpl::address() const {
  return address_;
}

int64_t ViewImpl::size() const {
  return size_;
}

bool ViewImpl::create_file_backed_view(File *file, int64_t offset, int64_t size,
                                       ViewFlags flags) {
  const int64_t file_size = file->size();
  if ((offset < 0) || (offset >= file_size) ||
      (size == 0) || (size > file_size) ||
      ((size > 0) && (size > (file_size - offset)))) {
    GRNXX_ERROR() << "invalid argument: offset = " << offset
                  << ", size = " << size << ", file_size = " << file_size;
    return false;
  }
  if (size < 0) {
    size = file_size - offset;
  }
  if (file->flags() & FILE_READ_ONLY) {
    flags_ |= VIEW_READ_ONLY;
  }
  size_ = size;
  int protection_flags = PROT_READ | PROT_WRITE;
  if (flags_ & VIEW_READ_ONLY) {
    flags_ |= VIEW_READ_ONLY;
    protection_flags = PROT_READ;
  }
  const int mmap_flags = MAP_SHARED;
  address_ = ::mmap(nullptr, size, protection_flags, mmap_flags,
                    *static_cast<const int *>(file->handle()), offset);
  if (address_ == MAP_FAILED) {
    GRNXX_ERROR() << "failed to map file-backed view: "
                  << "file_path = " << file->path()
                  << ", offset = " << offset << ", size = " << size
                  << ", flags = " << flags << ": '::mmap' " << Error(errno);
    return false;
  }
  return true;
}

bool ViewImpl::create_anonymous_view(int64_t size, ViewFlags flags) {
  if (size <= 0) {
    GRNXX_ERROR() << "invalid argument: size = " << size;
    return false;
  }
  flags_ = VIEW_ANONYMOUS;
  size_ = size;
  const int protection_flags = PROT_READ | PROT_WRITE;
  const int mmap_flags = MAP_PRIVATE | MAP_ANONYMOUS;
#ifdef MAP_HUGETLB
  if (flags & VIEW_HUGE_TLB) {
    address_ = ::mmap(nullptr, size, protection_flags,
                      mmap_flags | MAP_HUGETLB, -1, 0);
    if (address_ != MAP_FAILED) {
      flags_ |= VIEW_HUGE_TLB;
    }
  }
#endif  // MAP_HUGETLB
  if (address_ == MAP_FAILED) {
    address_ = ::mmap(nullptr, size, protection_flags, mmap_flags, -1, 0);
    if (address_ == MAP_FAILED) {
      GRNXX_ERROR() << "failed to map anonymous view: size = " << size
                    << ", flags = " << flags << ": '::mmap' " << Error(errno);
      return false;
    }
  }
  return true;
}

}  // namespace storage
}  // namespace grnxx

#endif  // GRNXX_WINDOWS
