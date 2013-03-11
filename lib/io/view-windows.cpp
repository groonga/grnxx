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
#include "io/view-windows.hpp"

#ifdef GRNXX_WINDOWS

#include "error.hpp"
#include "exception.hpp"
#include "io/file.hpp"
#include "logger.hpp"

namespace grnxx {
namespace io {

ViewImpl::~ViewImpl() {
  if (address_) {
    if (!::UnmapViewOfFile(address_)) {
      GRNXX_ERROR() << "failed to unmap view: view = " << *this
                    << ": '::UnmapViewOfFile' " << Error(::GetLastError());
    }
  }

  if (handle_) {
    if (!::CloseHandle(handle_)) {
      GRNXX_ERROR() << "failed to close file mapping: view = " << *this
                    << ": '::CloseHandle' " << Error(::GetLastError());
    }
  }
}

ViewImpl *ViewImpl::open(ViewFlags flags, uint64_t size) {
  if (size == 0) {
    GRNXX_ERROR() << "invalid argument: size = " << size;
    GRNXX_THROW();
  }

  std::unique_ptr<ViewImpl> view(new (std::nothrow) ViewImpl);
  if (!view) {
    GRNXX_ERROR() << "new grnxx::io::ViewImpl failed";
    GRNXX_THROW();
  }
  view->open_view(flags, size);
  return view.release();
}

ViewImpl *ViewImpl::open(ViewFlags flags, File *file) {
  std::unique_ptr<ViewImpl> view(new (std::nothrow) ViewImpl);
  if (!view) {
    GRNXX_ERROR() << "new grnxx::io::ViewImpl failed";
    GRNXX_THROW();
  }
  view->open_view(flags, file, 0, 0);
  return view.release();
}

ViewImpl *ViewImpl::open(ViewFlags flags, File *file,
                         uint64_t offset, uint64_t size) {
  if (size == 0) {
    GRNXX_ERROR() << "invalid argument: size = " << size;
    GRNXX_THROW();
  }

  std::unique_ptr<ViewImpl> view(new (std::nothrow) ViewImpl);
  if (!view) {
    GRNXX_ERROR() << "new grnxx::io::ViewImpl failed";
    GRNXX_THROW();
  }
  view->open_view(flags, file, offset, size);
  return view.release();
}

void ViewImpl::sync() {
  if (!::FlushViewOfFile(address_, 0)) {
    GRNXX_ERROR() << "failed to sync memory mapping: view = " << *this
                  << ": '::FlushViewOfFile' " << Error(::GetLastError());
    GRNXX_THROW();
  }
}

void ViewImpl::sync(uint64_t offset, uint64_t size) {
  if ((offset > size_) || (size > size_) || ((offset + size) > size_)) {
    GRNXX_ERROR() << "invalid arguments: view = " << *this
                  << ", offset = " << offset << ", size = " << size;
    GRNXX_THROW();
  }

  if (size != 0) {
    if (!::FlushViewOfFile(static_cast<char *>(address_) + offset, size)) {
      GRNXX_ERROR() << "failed to sync memory mapping: view = " << *this
                    << ", offset = " << offset << ", size = " << size
                    << ": '::FlushViewOfFile' " << Error(::GetLastError());
      GRNXX_THROW();
    }
  }
}

ViewImpl::ViewImpl()
  : flags_(ViewFlags::none()), handle_(nullptr), address_(nullptr), size_(0) {}

void ViewImpl::open_view(ViewFlags, uint64_t size) {
  flags_ = VIEW_PRIVATE | VIEW_ANONYMOUS;
  size_ = size;

  const DWORD size_high = static_cast<DWORD>(size >> 32);
  const DWORD size_low = static_cast<DWORD>(size & 0xFFFFFFFFU);
  handle_ = ::CreateFileMapping(INVALID_HANDLE_VALUE, nullptr, PAGE_READWRITE,
                                size_high, size_low, nullptr);
  if (!handle_) {
    GRNXX_ERROR() << "failed to create anonymous file mapping: size = "
                  << size << ": '::CreateFileMapping' "
                  << Error(::GetLastError());
    GRNXX_THROW();
  }

  address_ = ::MapViewOfFile(handle_, FILE_MAP_WRITE, 0, 0, 0);
  if (!address_) {
    GRNXX_ERROR() << "failed to map anonymous view: size = " << size
                  << ": '::MapViewOfFile' " << Error(::GetLastError());
    GRNXX_THROW();
  }
}

void ViewImpl::open_view(ViewFlags flags, File *file,
                           uint64_t offset, uint64_t size) {
  const uint64_t file_size = file->size();
  if (file_size == 0) {
    GRNXX_ERROR() << "invalid argument: file = " << *file;
    GRNXX_THROW();
  }
  if (flags & (VIEW_ANONYMOUS | VIEW_HUGE_TLB)) {
    GRNXX_ERROR() << "invalid argument: flags = " << flags;
    GRNXX_THROW();
  }
  if (size >= std::numeric_limits<SIZE_T>::max()) {
    GRNXX_ERROR() << "invalid argument: size = " << size
                  << ", max_size = " << std::numeric_limits<SIZE_T>::max();
    GRNXX_THROW();
  }
  if ((size > file_size) || (offset > (file_size - size))) {
    GRNXX_ERROR() << "invalid argument: size = " << size
                  << ", offset = " << offset << ", file_size = " << file_size;
    GRNXX_THROW();
  }

  size_ = (size != 0) ? size : file_size;

  int protection_mode = PAGE_READWRITE;
  DWORD desired_access = FILE_MAP_WRITE;
  if ((file->flags() & FILE_READ_ONLY) ||
      ((~file->flags() & FILE_WRITE_ONLY) && (flags & VIEW_READ_ONLY))) {
    flags_ |= VIEW_READ_ONLY;
    protection_mode = PAGE_READONLY;
    desired_access = FILE_MAP_READ;
  } else if (file->flags() & FILE_WRITE_ONLY) {
    // Write-only memory mapping is not supported on Windows.
    GRNXX_ERROR() << "mapping file is write-only: file = " << *file;
    GRNXX_THROW();
  } else {
    // VIEW_WRITE_ONLY is ignored because write-only memory mapping is not
    // supported on Windows.
    protection_mode = PAGE_READWRITE;
    if (flags & VIEW_PRIVATE) {
      protection_mode = PAGE_WRITECOPY;
      desired_access = FILE_MAP_COPY;
    }
  }

  if ((flags & VIEW_SHARED) || (~flags & VIEW_PRIVATE)) {
    flags_ |= VIEW_SHARED;
  } else {
    flags_ |= VIEW_PRIVATE;
  }

  const DWORD size_high = static_cast<DWORD>((offset + size) >> 32);
  const DWORD size_low = static_cast<DWORD>((offset + size) & 0xFFFFFFFFU);
  handle_ = ::CreateFileMapping(*static_cast<const HANDLE *>(file->handle()),
                                nullptr, protection_mode, size_high, size_low,
                                nullptr);
  if (!handle_) {
    GRNXX_ERROR() << "failed to create file mapping: file = " << *file
                  << ", flags = " << flags << ", offset = " << offset
                  << ", size = " << size
                  << ": '::CreateFileMapping' " << Error(::GetLastError());
    GRNXX_THROW();
  }

  const DWORD offset_high = static_cast<DWORD>(offset >> 32);
  const DWORD offset_low = static_cast<DWORD>(offset & 0xFFFFFFFFU);
  address_ = ::MapViewOfFile(handle_, desired_access, offset_high, offset_low,
                             static_cast<SIZE_T>(size));
  if (!address_) {
    GRNXX_ERROR() << "failed to map view: file = " << *file
                  << ", flags = " << flags << ", offset = " << offset
                  << ", size = " << size
                  << ": '::MapViewOfFile' " << Error(::GetLastError());
    GRNXX_THROW();
  }
}

StringBuilder &ViewImpl::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  return builder << "{ flags = " << flags_
                 << ", address = " << address_
                 << ", size = " << size_ << " }";
}

}  // namespace io
}  // namespace grnxx

#endif  // GRNXX_WINDOWS
