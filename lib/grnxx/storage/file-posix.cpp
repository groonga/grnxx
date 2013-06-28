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
#include "grnxx/storage/file-posix.hpp"

#ifndef GRNXX_WINDOWS

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <limits>
#include <new>

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
      fd_(-1),
      locked_(false) {}

FileImpl::~FileImpl() {
  if (fd_ != -1) {
    if (locked_) {
      unlock();
    }
    if (::close(fd_) != 0) {
      GRNXX_ERROR() << "failed to close file: path = " << path_.get()
                    << ": '::close' " << Error(errno);
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
  struct stat stat;
  if (::stat(path, &stat) != 0) {
    return false;
  }
  return S_ISREG(stat.st_mode);
}

bool FileImpl::unlink(const char *path) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = nullptr";
    return false;
  }
  if (::unlink(path) != 0) {
    GRNXX_WARNING() << "failed to unlink file: path = " << path
                    << ": '::unlink' " << Error(errno);
    return false;
  }
  return true;
}

bool FileImpl::lock(FileLockFlags lock_flags) {
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
  int operation = 0;
  if (lock_flags & FILE_LOCK_SHARED) {
    operation |= LOCK_SH;
  }
  if (lock_flags & FILE_LOCK_EXCLUSIVE) {
    operation |= LOCK_EX;
  }
  if (lock_flags & FILE_LOCK_NONBLOCKING) {
    operation |= LOCK_NB;
  }
  if (::flock(fd_, operation) != 0) {
    if (errno == EWOULDBLOCK) {
      // The file is locked by others.
      return false;
    }
    GRNXX_ERROR() << "failed to lock file: path = " << path_.get()
                  << ", lock_flags = " << lock_flags
                  << ": '::flock' " << Error(errno);
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
  if (::flock(fd_, LOCK_UN) != 0) {
    GRNXX_ERROR() << "failed to unlock file: path = " << path_.get()
                  << ": '::flock' " << Error(errno);
    return false;
  }
  locked_ = false;
  return true;
}

bool FileImpl::sync() {
  if (::fsync(fd_) != 0) {
    GRNXX_ERROR() << "failed to sync file: path = " << path_.get()
                  << ": '::fsync' " << Error(errno);
    return false;
  }
  return true;
}

bool FileImpl::resize(uint64_t size) {
  if (flags_ & FILE_READ_ONLY) {
    GRNXX_ERROR() << "read-only";
    return false;
  }
  if (size > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
    GRNXX_ERROR() << "invalid argument: size = " << size;
    return false;
  }
  if (::ftruncate(fd_, size) != 0) {
    GRNXX_ERROR() << "failed to resize file: path = " << path_.get()
                  << ", size = " << size
                  << ": '::ftruncate' " << Error(errno);
    return false;
  }
  return true;
}

bool FileImpl::get_size(uint64_t *size) {
  struct stat stat;
  if (::fstat(fd_, &stat) != 0) {
    GRNXX_ERROR() << "failed to stat file: path = " << path_.get()
                  << ": '::fstat' " << Error(errno);
    return false;
  }
  if (size) {
    *size = stat.st_size;
  }
  return true;
}

const char *FileImpl::path() const {
  return path_.get();
}

FileFlags FileImpl::flags() const {
  return flags_;
}

const void *FileImpl::handle() const {
  return &fd_;
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
  fd_ = ::open(path, O_RDWR | O_CREAT | O_EXCL, 0644);
  if (fd_ == -1) {
    GRNXX_ERROR() << "failed to create file: path = " << path
                  << ", flags = " << flags
                  << ": '::open' " << Error(errno);
    return false;
  }
  return true;
}

bool FileImpl::create_temporary_file(const char *path, FileFlags flags) {
  flags_ = FILE_TEMPORARY;
  int posix_flags = O_RDWR | O_CREAT | O_EXCL | O_NOCTTY;
#ifdef O_NOATIME
  posix_flags |= O_NOATIME;
#endif  // O_NOATIME
#ifdef O_NOFOLLOW
  posix_flags |= O_NOFOLLOW;
#endif  // O_NOFOLLOW
  for (int i = 0; i < UNIQUE_PATH_GENERATION_TRIAL_COUNT; ++i) {
    path_.reset(Path::unique_path(path));
    fd_ = ::open(path_.get(), posix_flags, 0600);
    if (fd_ != -1) {
      unlink(path_.get());
      return true;
    }
    GRNXX_WARNING() << "failed to create file: path = " << path_.get()
                    << ": '::open' " << Error(errno);
  }
  GRNXX_ERROR() << "failed to create temporary file: "
                << "path = " << path
                << ", flags = " << flags;
  return false;
}

bool FileImpl::open_file(const char *path, FileFlags flags) {
  path_.reset(Path::clone_path(path));
  if (!path_) {
    return false;
  }
  int posix_flags = O_RDWR;
  if (flags & FILE_READ_ONLY) {
    posix_flags = O_RDONLY;
    flags_ |= FILE_READ_ONLY;
  }
  fd_ = ::open(path, posix_flags);
  if (fd_ == -1) {
    GRNXX_ERROR() << "failed to open file: path = " << path
                  << ", flags = " << flags
                  << ": '::open' " << Error(errno);
    return false;
  }
  return true;
}

bool FileImpl::open_or_create_file(const char *path, FileFlags flags) {
  path_.reset(Path::clone_path(path));
  if (!path_) {
    return false;
  }
  fd_ = ::open(path, O_RDWR | O_CREAT, 0644);
  if (fd_ == -1) {
    GRNXX_ERROR() << "failed to open file: path = " << path
                  << ", flags = " << flags
                  << ": '::open' " << Error(errno);
    return false;
  }
  return true;
}

}  // namespace storage
}  // namespace grnxx

#endif  // GRNXX_WINDOWS
