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

#include "grnxx/errno.hpp"
#include "grnxx/exception.hpp"
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
      Errno errno_copy(errno);
      GRNXX_WARNING() << "failed to close file: path = " << path_.get()
                      << ", call = ::close, errno = " << errno_copy;
    }
  }
}

FileImpl *FileImpl::create(const char *path, FileFlags flags) {
  std::unique_ptr<FileImpl> file(new (std::nothrow) FileImpl);
  if (!file) {
    GRNXX_ERROR() << "new grnxx::storage::FileImpl failed";
    throw MemoryError();
  }
  if (path && (~flags & FILE_TEMPORARY)) {
    file->create_persistent_file(path, flags);
  } else {
    file->create_temporary_file(path, flags);
  }
  return file.release();
}

FileImpl *FileImpl::open(const char *path, FileFlags flags) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = nullptr";
    throw LogicError();
  }
  std::unique_ptr<FileImpl> file(new (std::nothrow) FileImpl);
  if (!file) {
    GRNXX_ERROR() << "new grnxx::storage::FileImpl failed";
    throw MemoryError();
  }
  file->open_file(path, flags);
  return file.release();
}

FileImpl *FileImpl::open_or_create(const char *path, FileFlags flags) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = nullptr";
    throw LogicError();
  }
  std::unique_ptr<FileImpl> file(new (std::nothrow) FileImpl);
  if (!file) {
    GRNXX_ERROR() << "new grnxx::storage::FileImpl failed";
    throw MemoryError();
  }
  file->open_or_create_file(path, flags);
  return file.release();
}

bool FileImpl::exists(const char *path) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = nullptr";
    throw LogicError();
  }
  struct stat stat;
  if (::stat(path, &stat) != 0) {
    if (errno != ENOENT) {
      Errno errno_copy(errno);
      GRNXX_WARNING() << "failed to get file information: "
                      << "call = ::stat, errno = " << errno_copy;
    }
    return false;
  }
  return S_ISREG(stat.st_mode);
}

void FileImpl::unlink(const char *path) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = nullptr";
    throw LogicError();
  }
  if (::unlink(path) != 0) {
    Errno errno_copy(errno);
    GRNXX_ERROR() << "failed to unlink file: path = " << path
                  << ", call = ::unlink, errno = " << errno_copy;
    throw SystemError(errno_copy);
  }
}

bool FileImpl::lock(FileLockFlags lock_flags) {
  if (locked_) {
    GRNXX_ERROR() << "already locked: path = " << path_.get();
    throw LogicError();
  }
  FileLockFlags lock_type =
      lock_flags & (FILE_LOCK_SHARED | FILE_LOCK_EXCLUSIVE);
  if (!lock_type || (lock_type == (FILE_LOCK_SHARED | FILE_LOCK_EXCLUSIVE))) {
    GRNXX_ERROR() << "invalid argument: lock_flags == " << lock_flags;
    throw LogicError();
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
      return false;
    }
    Errno errno_copy(errno);
    GRNXX_ERROR() << "failed to lock file: path = " << path_.get()
                  << ", lock_flags = " << lock_flags
                  << ", call = ::flock, errno = " << Errno(errno_copy);
    throw SystemError(errno_copy);
  }
  locked_ = true;
  return true;
}

void FileImpl::unlock() {
  if (!locked_) {
    GRNXX_WARNING() << "unlocked: path = " << path_.get();
    throw LogicError();
  }
  if (::flock(fd_, LOCK_UN) != 0) {
    Errno errno_copy(errno);
    GRNXX_ERROR() << "failed to unlock file: path = " << path_.get()
                  << ", call = ::flock, errno = " << errno_copy;
    throw SystemError(errno_copy);
  }
  locked_ = false;
}

void FileImpl::sync() {
  if (::fsync(fd_) != 0) {
    Errno errno_copy(errno);
    GRNXX_ERROR() << "failed to sync file: path = " << path_.get()
                  << ", call = ::fsync, errno = " << errno_copy;
    throw SystemError(errno_copy);
  }
}

void FileImpl::resize(uint64_t size) {
  if (flags_ & FILE_READ_ONLY) {
    GRNXX_ERROR() << "invalid operation: flags = " << flags_;
    throw LogicError();
  }
  if (size > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
    GRNXX_ERROR() << "invalid argument: size = " << size;
    throw LogicError();
  }
  if (::ftruncate(fd_, size) != 0) {
    Errno errno_copy(errno);
    GRNXX_ERROR() << "failed to resize file: path = " << path_.get()
                  << ", size = " << size
                  << ", call = ::ftruncate, errno = " << errno_copy;
    throw SystemError(errno_copy);
  }
}

uint64_t FileImpl::get_size() {
  struct stat stat;
  if (::fstat(fd_, &stat) != 0) {
    Errno errno_copy(errno);
    GRNXX_ERROR() << "failed to stat file: path = " << path_.get()
                  << ", call = ::fstat, errno = " << errno_copy;
    throw SystemError(errno_copy);
  }
  return stat.st_size;
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

void FileImpl::create_persistent_file(const char *path, FileFlags flags) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = nullptr";
    throw LogicError();
  }
  path_.reset(Path::clone_path(path));
  fd_ = ::open(path, O_RDWR | O_CREAT | O_EXCL, 0644);
  if (fd_ == -1) {
    Errno errno_copy(errno);
    GRNXX_ERROR() << "failed to create file: path = " << path
                  << ", flags = " << flags
                  << ", call = ::open, errno = " << errno_copy;
    throw SystemError(errno_copy);
  }
}

void FileImpl::create_temporary_file(const char *path, FileFlags flags) {
  flags_ = FILE_TEMPORARY;
  int posix_flags = O_RDWR | O_CREAT | O_EXCL | O_NOCTTY;
#ifdef O_NOATIME
  posix_flags |= O_NOATIME;
#endif  // O_NOATIME
#ifdef O_NOFOLLOW
  posix_flags |= O_NOFOLLOW;
#endif  // O_NOFOLLOW
  Errno errno_copy;
  for (int i = 0; i < UNIQUE_PATH_GENERATION_TRIAL_COUNT; ++i) {
    path_.reset(Path::unique_path(path));
    fd_ = ::open(path_.get(), posix_flags, 0600);
    if (fd_ != -1) {
      unlink(path_.get());
      return;
    }
    errno_copy = Errno(errno);
    GRNXX_WARNING() << "failed to create file: path = " << path_.get()
                    << ", call = ::open, errno = " << errno_copy;
  }
  GRNXX_ERROR() << "failed to create temporary file: "
                << "path = " << path << ", flags = " << flags;
  throw SystemError(errno_copy);
}

void FileImpl::open_file(const char *path, FileFlags flags) {
  path_.reset(Path::clone_path(path));
  int posix_flags = O_RDWR;
  if (flags & FILE_READ_ONLY) {
    posix_flags = O_RDONLY;
    flags_ |= FILE_READ_ONLY;
  }
  fd_ = ::open(path, posix_flags);
  if (fd_ == -1) {
    Errno errno_copy(errno);
    GRNXX_ERROR() << "failed to open file: path = " << path
                  << ", flags = " << flags
                  << ", call = ::open, errno = " << errno_copy;
    throw SystemError(errno_copy);
  }
}

void FileImpl::open_or_create_file(const char *path, FileFlags flags) {
  path_.reset(Path::clone_path(path));
  fd_ = ::open(path, O_RDWR | O_CREAT, 0644);
  if (fd_ == -1) {
    Errno errno_copy(errno);
    GRNXX_ERROR() << "failed to open file: path = " << path
                  << ", flags = " << flags
                  << ", call = ::open, errno = " << errno_copy;
    throw SystemError(errno_copy);
  }
}

}  // namespace storage
}  // namespace grnxx

#endif  // GRNXX_WINDOWS
