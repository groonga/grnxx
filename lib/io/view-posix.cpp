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
#include "view-posix.hpp"

#ifndef GRNXX_WINDOWS

#include <sys/mman.h>
#include <errno.h>

#include "../error.hpp"
#include "../exception.hpp"
#include "../logger.hpp"

#ifndef MAP_ANONYMOUS
# ifdef MAP_ANON
#  define MAP_ANONYMOUS MAP_ANON
# endif  // MAP_ANON
#endif  // MAP_ANONYMOUS

namespace grnxx {
namespace io {

ViewImpl::~ViewImpl() {
  if (address_ != MAP_FAILED) {
    if (::munmap(address_, static_cast<size_t>(size_)) != 0) {
      GRNXX_ERROR() << "failed to unmap view: view = " << *this
                    << ": '::munmap' " << Error(errno);
    }
  }
}

std::unique_ptr<ViewImpl> ViewImpl::map(Flags flags, uint64_t size) {
  std::unique_ptr<ViewImpl> view(new (std::nothrow) ViewImpl);
  if (!view) {
    GRNXX_ERROR() << "new grnxx::io::ViewImpl failed";
    GRNXX_THROW();
  }
  view->map_on_memory(flags, size);
  return view;
}

std::unique_ptr<ViewImpl> ViewImpl::map(const File &file, Flags flags) {
  std::unique_ptr<ViewImpl> view(new (std::nothrow) ViewImpl);
  if (!view) {
    GRNXX_ERROR() << "new grnxx::io::ViewImpl failed";
    GRNXX_THROW();
  }
  view->map_on_file(file, flags, 0, file.size());
  return view;
}

std::unique_ptr<ViewImpl> ViewImpl::map(const File &file, Flags flags,
                                        uint64_t offset, uint64_t size) {
  std::unique_ptr<ViewImpl> view(new (std::nothrow) ViewImpl);
  if (!view) {
    GRNXX_ERROR() << "new grnxx::io::ViewImpl failed";
    GRNXX_THROW();
  }
  view->map_on_file(file, flags, offset, size);
  return view;
}

void ViewImpl::sync() {
  sync(0, size_);
}

void ViewImpl::sync(uint64_t offset, uint64_t size) {
  if ((offset > size_) || (size > size_) || ((offset + size) > size_)) {
    GRNXX_ERROR() << "invalid arguments: view = " << *this
                  << ", offset = " << offset << ", size = " << size;
    GRNXX_THROW();
  }

  if (size != 0) {
    if (::msync(static_cast<char *>(address_) + offset, size, MS_SYNC) != 0) {
      GRNXX_ERROR() << "failed to sync memory mapping: view = " << *this
                    << ", offset = " << offset << ", size = " << size
                    << ": '::msync' " << Error(errno);
      GRNXX_THROW();
    }
  }
}

ViewImpl::ViewImpl()
  : file_(), flags_(), address_(MAP_FAILED), offset_(0), size_(0) {}

void ViewImpl::map_on_memory(Flags flags, uint64_t size) {
  if ((size == 0) || (size > std::numeric_limits<size_t>::max())) {
    GRNXX_ERROR() << "invalid argument: size = " << size << ": (0, "
                  << std::numeric_limits<size_t>::max() << ']';
    GRNXX_THROW();
  }

  flags_ = GRNXX_IO_PRIVATE | GRNXX_IO_ANONYMOUS;
  size_ = size;

  int map_flags = MAP_PRIVATE | MAP_ANONYMOUS;
#ifdef MAP_HUGETLB
  if (flags & GRNXX_IO_HUGE_TLB) {
    address_ = ::mmap(nullptr, size, PROT_READ | PROT_WRITE,
                      map_flags | MAP_HUGETLB, -1, 0);
    if (address_ != MAP_FAILED) {
      flags_ |= GRNXX_IO_HUGE_TLB;
    }
  }
#endif  // MAP_HUGETLB
  if (address_ == MAP_FAILED) {
    address_ = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, map_flags, -1, 0);
  }

  if (address_ == MAP_FAILED) {
    GRNXX_ERROR() << "failed to map anonymous view: size = " << size
                  << ": '::mmap' " << Error(errno);
    GRNXX_THROW();
  }
}

void ViewImpl::map_on_file(const File &file, Flags flags, uint64_t offset,
                           uint64_t size) {
  if ((size == 0) || (size > std::numeric_limits<size_t>::max())) {
    GRNXX_ERROR() << "invalid argument: size = " << size << ": (0, "
                  << std::numeric_limits<size_t>::max() << ']';
    GRNXX_THROW();
  }
  if (offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
    GRNXX_ERROR() << "invalid argument: offset = " << offset << ": [0, "
                  << std::numeric_limits<off_t>::max() << ']';
    GRNXX_THROW();
  }

  file_ = file;
  offset_ = offset;
  size_ = size;

  int protection_flags = PROT_READ | PROT_WRITE;
  if ((file.flags() & GRNXX_IO_READ_ONLY) ||
      ((~file.flags() & GRNXX_IO_WRITE_ONLY) &&
       (flags & GRNXX_IO_READ_ONLY))) {
    flags_ |= GRNXX_IO_READ_ONLY;
    protection_flags = PROT_READ;
  } else if ((file.flags() & GRNXX_IO_WRITE_ONLY) ||
             (flags & GRNXX_IO_WRITE_ONLY)) {
    flags_ |= GRNXX_IO_WRITE_ONLY;
    protection_flags = PROT_WRITE;
  }

  int map_flags;
  if ((flags & GRNXX_IO_SHARED) || (~flags & GRNXX_IO_PRIVATE)) {
    flags_ |= GRNXX_IO_SHARED;
    map_flags = MAP_SHARED;
  } else {
    flags_ |= GRNXX_IO_PRIVATE;
    map_flags = MAP_PRIVATE;
  }

  address_ = ::mmap(nullptr, size, protection_flags, map_flags,
                    *static_cast<const int *>(file.handle()), offset);
  if (address_ == MAP_FAILED) {
    GRNXX_ERROR() << "failed to map view: file = " << file
                  << ", flags = " << flags << ", offset = " << offset
                  << ", size = " << size << ": '::mmap' " << Error(errno);
    GRNXX_THROW();
  }
}

StringBuilder &ViewImpl::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  if (file_) {
    builder << "{ file = " << file_.path();
  } else {
    builder << "{ file = n/a";
  }
  return builder << ", flags = " << flags_
                 << ", address = " << address_
                 << ", offset = " << offset_
                 << ", size = " << size_ << " }";
}

}  // namespace io
}  // namespace grnxx

#endif  // GRNXX_WINDOWS
