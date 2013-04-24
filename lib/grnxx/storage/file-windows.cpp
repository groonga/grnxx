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
#include "grnxx/storage/file-windows.hpp"

#ifdef GRNXX_WINDOWS

#include <sys/types.h>
#include <sys/stat.h>
#include <io.h>

#include "grnxx/error.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/storage/path.hpp"

namespace grnxx {
namespace storage {
namespace {

constexpr int UNIQUE_PATH_GENERATION_TRIAL_COUNT = 10;

}  // namespace

FileImpl::FileImpl()
    : File(),
      path_(),
      flags_(FILE_DEFAULT),
      handle_(INVALID_HANDLE_VALUE),
      locked_(false) {}

FileImpl::~FileImpl() {
  if (handle_ != INVALID_HANDLE_VALUE) {
    if (locked_) {
      unlock();
    }
    if (!::CloseHandle(handle_)) {
      GRNXX_ERROR() << "failed to close file: file = " << *this
                    << ": '::CloseHandle' " << Error(::GetLastError());
    }
  }
}


FileImpl *FileImpl::create(const char *path, FileFlags flags) {
  std::unique_ptr<FileImpl> file(new (std::nothrow) FileImpl);
  if (!file) {
    GRNXX_ERROR() << "new grnxx::storage::FileImpl failed";
    return nullptr;
  }
  if (path && (~flags & FILE_TEMPORARY)) {
    if (!file->create_persistent_file(path, flags)) {
      return nullptr;
    }
  } else {
    if (!file->create_temporary_file(path, flags)) {
      return nullptr;
    }
  }
  return file.release();
}

FileImpl *FileImpl::open(const char *path, FileFlags flags) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = nullptr";
    return nullptr;
  }
  std::unique_ptr<FileImpl> file(new (std::nothrow) FileImpl);
  if (!file) {
    GRNXX_ERROR() << "new grnxx::storage::FileImpl failed";
    return nullptr;
  }
  if (!file->open_file(path, flags)) {
    return nullptr;
  }
  return file.release();
}

FileImpl *FileImpl::open_or_create(const char *path, FileFlags flags) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = nullptr";
    return nullptr;
  }
  std::unique_ptr<FileImpl> file(new (std::nothrow) FileImpl);
  if (!file) {
    GRNXX_ERROR() << "new grnxx::storage::FileImpl failed";
    return nullptr;
  }
  if (!file->open_or_create_file(path, flags)) {
    return nullptr;
  }
  return file.release();
}

bool FileImpl::exists(const char *path) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = nullptr";
    return false;
  }
  struct _stat stat;
  if (::_stat(path, &stat) != 0) {
    return false;
  }
  return stat.st_mode & _S_IFREG;
}

bool FileImpl::unlink(const char *path) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = nullptr";
    return false;
  }
  if (!::DeleteFile(path)) {
    GRNXX_WARNING() << "failed to unlink file: path = " << path
                    << ": '::DeleteFile' " << Error(::GetLastError());
    return false;
  }
  return true;
}

bool FileImpl::try_lock(FileLockMode mode) {
  if (locked_) {
    GRNXX_ERROR() << "already locked: path = " << path_.get();
    return false;
  }
  FileLockFlags lock_type =
      lock_flags & (FILE_LOCK_SHARED | FILE_LOCK_EXCLUSIVE);
  if (!lock_type || (lock_type == (FILE_LOCK_SHARED | FILE_LOCK_EXCLUSIVE))) {
    GRNXX_ERROR() << "invalid argument: lock_flags == " << lock_flags;
    return false;
  }
  DWORD flags = LOCKFILE_FAIL_IMMEDIATELY;
  if (lock_flags & FILE_LOCK_SHARED) {
    // Nothing to do.
  }
  if (lock_flags & FILE_LOCK_EXCLUSIVE) {
    flags |= LOCKFILE_EXCLUSIVE_LOCK;
  }
  if (lock_flags & FILE_LOCK_NONBLOCKING) {
    flags |= LOCKFILE_FAIL_IMMEDIATELY;
  }

  OVERLAPPED overlapped;
  overlapped.Offset = 0;
  overlapped.OffsetHigh = 0x80000000U;
  if (!::LockFileEx(handle_, flags, 0, 0, 0x80000000U, &overlapped)) {
    const DWORD last_error = ::GetLastError();
    if (last_error == ERROR_LOCK_FAILED) {
      // The file is locked by others.
      return false;
    }
    GRNXX_ERROR() << "failed to lock file: path = " << path_.get()
                  << ", mode = " << mode
                  << ": '::LockFileEx' " << Error(last_error);
    return false;
  }
  locked_ = true;
  return true;
}

bool FileImpl::unlock() {
  if (!locked_) {
    GRNXX_WARNING() << "unlocked: path = " << path_.get();
    return false;
  }

  OVERLAPPED overlapped;
  overlapped.Offset = 0;
  overlapped.OffsetHigh = 0x80000000U;
  if (!::UnlockFileEx(handle_, 0, 0, 0x80000000U, &overlapped)) {
    GRNXX_ERROR() << "failed to unlock file: path = " << path_.get()
                  << ": '::UnlockFileEx' " << Error(::GetLastError());
    return false;
  }
  locked_ = false;
  return true;
}

void FileImpl::sync() {
  if (!::FlushFileBuffers(handle_)) {
    GRNXX_ERROR() << "failed to sync file: path = " << path_.get()
                  << ": '::FlushFileBuffers' " << Error(errno);
    return false;
  }
  return true;
}

bool FileImpl::resize(int64_t size) {
  if (flags_ & FILE_READ_ONLY) {
    GRNXX_ERROR() << "read-only";
    return false;
  }
  if ((size < 0) ||
      (size > std::numeric_limits<off_t>::max())) {
    GRNXX_ERROR() << "invalid argument: size = " << size;
    return false;
  }
  LARGE_INTEGER request;
  request.QuadPart = size;
  if (!::SetFilePointerEx(handle_, request, nullptr, FILE_BEGIN)) {
    GRNXX_ERROR() << "failed to seek file: path = " << path_.get()
                  << ", offset = " << offset << ", whence = " << whence
                  << ": '::SetFilePointerEx' " << Error(::GetLastError());
    return false;
  }
  if (!::SetEndOfFile(handle_)) {
    GRNXX_ERROR() << "failed to resize file: path = " << path_.get()
                  << ", size = " << size
                  << ": '::SetEndOfFile' " << Error(::GetLastError());
    return false;
  }
  return true;
}

int64_t FileImpl::size() const {
  LARGE_INTEGER file_size;
  if (!::GetFileSizeEx(handle_, &file_size)) {
    GRNXX_ERROR() << "failed to get file size: path = " << path_.get()
                  << ": '::GetFileSizeEx' " << Error(::GetLastError());
    return -1;
  }
  return file_size.QuadPart;
}

const char *FileImpl::path() const {
  return path_.get();
}

FileFlags FileImpl::flags() const {
  return flags_;
}

const void *FileImpl::handle() const {
  return &handle_;
}

bool FileImpl::create_persistent_file(const char *path, FileFlags flags) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = nullptr";
    return false;
  }
  path_.reset(Path::clone_path(path));
  if (!path_) {
    return false;
  }
  const DWORD desired_access = GENERIC_READ | GENERIC_WRITE;
  const DWORD share_mode =
      FILE_SHARE_DELETE | FILE_SHARE_READ | FILE_SHARE_WRITE;
  const DWORD creation_disposition = CREATE_NEW;
  const DWORD flags_and_attributes = FILE_ATTRIBUTE_NORMAL;
  handle_ = ::CreateFileA(path, desired_access, share_mode, nullptr,
                          creation_disposition, flags_and_attributes, nullptr);
  if (handle_ == INVALID_HANDLE_VALUE) {
    GRNXX_ERROR() << "failed to open file: path = " << path
                  << ", flags = " << flags
                  << ": '::CreateFileA' " << Error(::GetLastError());
    return false;
  }
  return true;
}

bool FileImpl::create_temporary_file(const char *path, FileFlags flags) {
  flags_ = FILE_TEMPORARY;
  const DWORD desired_access = GENERIC_READ | GENERIC_WRITE;
  const DWORD share_mode = FILE_SHARE_DELETE;
  const DWORD creation_disposition = CREATE_NEW;
  const DWORD flags_and_attributes = FILE_ATTRIBUTE_TEMPORARY |
                                     FILE_FLAG_DELETE_ON_CLOSE;
  for (int i = 0; i < UNIQUE_PATH_GENERATION_TRIAL_COUNT; ++i) {
    path_.reset(Path::unique_path(path));
    handle_ = ::CreateFileA(path_.get(), desired_access,
                            share_mode, nullptr, creation_disposition,
                            flags_and_attributes, nullptr);
    if (handle_ != INVALID_HANDLE_VALUE) {
      return true;
    }
    GRNXX_WARNING() << "failed to create file: path = " << path_.get()
                    << ": '::CreateFileA' " << Error(::GetLastError());
  }
  GRNXX_ERROR() << "failed to create temporary file: path = " << path
                << ", flags = " << flags;
  return false;
}

bool FileImpl::open_file(const char *path, FileFlags flags) {
  path_.reset(Path::clone_path(path));
  if (!path_) {
    return false;
  }
  DWORD desired_access = GENERIC_READ | GENERIC_WRITE;
  if (flags_ & FILE_READ_ONLY) {
    flags_ |= FILE_READ_ONLY;
    desired_access = GENERIC_READ;
  }
  const DWORD share_mode =
      FILE_SHARE_DELETE | FILE_SHARE_READ | FILE_SHARE_WRITE;
  const DWORD creation_disposition = OPEN_EXISTING;
  const DWORD flags_and_attributes = FILE_ATTRIBUTE_NORMAL;
  handle_ = ::CreateFileA(path, desired_access, share_mode, nullptr,
                          creation_disposition, flags_and_attributes, nullptr);
  if (handle_ == INVALID_HANDLE_VALUE) {
    GRNXX_ERROR() << "failed to open file: path = " << path
                  << ", flags = " << flags
                  << ": '::CreateFileA' " << Error(::GetLastError());
    return false;
  }
  return true;
}

bool FileImpl::open_or_create_file(const char *path, FileFlags flags) {
  path_.reset(Path::clone_path(path));
  if (!path_) {
    return false;
  }
  const DWORD desired_access = GENERIC_READ | GENERIC_WRITE;
  const DWORD share_mode =
      FILE_SHARE_DELETE | FILE_SHARE_READ | FILE_SHARE_WRITE;
  const DWORD creation_disposition = OPEN_ALWAYS;
  const DWORD flags_and_attributes = FILE_ATTRIBUTE_NORMAL;
  handle_ = ::CreateFileA(path, desired_access, share_mode, nullptr,
                          creation_disposition, flags_and_attributes, nullptr);
  if (handle_ == INVALID_HANDLE_VALUE) {
    GRNXX_ERROR() << "failed to open file: path = " << path
                  << ", flags = " << flags
                  << ": '::CreateFileA' " << Error(::GetLastError());
    return false;
  }
  return true;
}

}  // namespace storage
}  // namespace grnxx

#endif  // GRNXX_WINDOWS
