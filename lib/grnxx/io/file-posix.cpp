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
#include "grnxx/io/file-posix.hpp"

#ifndef GRNXX_WINDOWS

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <errno.h>
#include <fcntl.h>

#include <algorithm>

#include "grnxx/error.hpp"
#include "grnxx/exception.hpp"
#include "grnxx/io/path.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/thread.hpp"
#include "grnxx/time/stopwatch.hpp"

namespace grnxx {
namespace io {

FileImpl::~FileImpl() {
  if (fd_ != -1) {
    unlock();
    if (::close(fd_) != 0) {
      GRNXX_ERROR() << "failed to close file: file = " << *this
                    << ": '::close' " << Error(errno);
    }
  }
  if (unlink_at_close_ && (~flags_ & FILE_TEMPORARY)) {
    unlink_if_exists(path_.c_str());
  }
}

FileImpl *FileImpl::open(FileFlags flags, const char *path, int permission) {
  std::unique_ptr<FileImpl> file(new (std::nothrow) FileImpl);
  if (!file) {
    GRNXX_ERROR() << "new grnxx::io::FileImpl failed";
    GRNXX_THROW();
  }

  if (flags & FILE_TEMPORARY) {
    file->open_temporary_file(flags, path, permission);
  } else {
    file->open_regular_file(flags, path, permission);
  }
  return file.release();
}

bool FileImpl::exists(const char *path) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = " << path;
    GRNXX_THROW();
  }

  struct stat stat;
  if (::stat(path, &stat) != 0) {
    return false;
  }
  return S_ISREG(stat.st_mode);
}

void FileImpl::unlink(const char *path) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = " << path;
    GRNXX_THROW();
  }

  if (::unlink(path) != 0) {
    GRNXX_ERROR() << "failed to unlink file: path = " << path
                  << ": '::unlink' " << Error(errno);
    GRNXX_THROW();
  }
}

bool FileImpl::unlink_if_exists(const char *path) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = " << path;
    GRNXX_THROW();
  }

  return ::unlink(path) == 0;
}

void FileImpl::lock(FileLockMode mode) {
  if (locked_) {
    GRNXX_ERROR() << "deadlock: file = " << *this;
    GRNXX_THROW();
  }
  while (!try_lock(mode)) {
    Thread::sleep_for(FILE_LOCK_SLEEP_DURATION);
  }
}

bool FileImpl::lock(FileLockMode mode, Duration timeout) {
  if (locked_) {
    GRNXX_ERROR() << "deadlock: file = " << *this;
    GRNXX_THROW();
  }
  if (try_lock(mode)) {
    return true;
  }
  Stopwatch stopwatch(true);
  while (stopwatch.elapsed() < timeout) {
    if (try_lock(mode)) {
      return true;
    }
    Thread::sleep_for(FILE_LOCK_SLEEP_DURATION);
  }
  return false;
}

bool FileImpl::try_lock(FileLockMode mode) {
  if (locked_) {
    return false;
  }

  int operation = LOCK_NB;
  switch (mode) {
    case FILE_LOCK_SHARED: {
      operation |= LOCK_SH;
      break;
    }
    case FILE_LOCK_EXCLUSIVE: {
      operation |= LOCK_EX;
      break;
    }
    default: {
      GRNXX_ERROR() << "invalid argument: mode = " << mode;
      GRNXX_THROW();
    }
  }

  if (::flock(fd_, operation) != 0) {
    if (errno == EWOULDBLOCK) {
      return false;
    }
    GRNXX_ERROR() << "failed to lock file: file = " << *this
                  << ", mode = " << mode << ": '::flock' " << Error(errno);
    GRNXX_THROW();
  }
  locked_ = true;
  return true;
}

bool FileImpl::unlock() {
  if (!locked_) {
    return false;
  }

  if (::flock(fd_, LOCK_UN) != 0) {
    GRNXX_ERROR() << "failed to unlock file: file = " << *this
                  << ": '::flock' " << Error(errno);
    GRNXX_THROW();
  }
  locked_ = false;
  return true;
}

constexpr uint64_t FILE_IMPL_MAX_OFFSET = std::numeric_limits<off_t>::max();
constexpr uint64_t FILE_IMPL_MAX_SIZE = std::numeric_limits<ssize_t>::max();

uint64_t FileImpl::read(void *buf, uint64_t size) {
  if (flags_ & FILE_WRITE_ONLY) {
    GRNXX_ERROR() << "file is write-only";
    GRNXX_THROW();
  }

  const size_t chunk_size =
      static_cast<size_t>(std::min(size, FILE_IMPL_MAX_SIZE));
  const ssize_t result = ::read(fd_, buf, chunk_size);
  if (result < 0) {
    GRNXX_ERROR() << "failed to read from file: file = " << *this
                  << ", size = " << size << ": '::read' " << Error(errno);
    GRNXX_THROW();
  }
  return result;
}

uint64_t FileImpl::read(void *buf, uint64_t size, uint64_t offset) {
  if (flags_ & FILE_WRITE_ONLY) {
    GRNXX_ERROR() << "file is write-only";
    GRNXX_THROW();
  }

  if (offset > FILE_IMPL_MAX_OFFSET) {
    GRNXX_ERROR() << "invalid argument: offset = " << offset
                  << ", max_offset = " << FILE_IMPL_MAX_OFFSET;
    GRNXX_THROW();
  }

  const size_t chunk_size =
      static_cast<size_t>(std::min(size, FILE_IMPL_MAX_SIZE));
#ifdef GRNXX_HAS_PREAD
  const ssize_t result =
      ::pread(fd_, buf, chunk_size, static_cast<off_t>(offset));
  if (result < 0) {
    GRNXX_ERROR() << "failed to read from file: file = " << *this
                  << ", size = " << size << ", offset = " << offset
                  << ": '::pwrite' " << Error(errno);
  }
#else  // GRNXX_HAS_PREAD
  // The resulting file offset is not defined on failure.
  const uint64_t current_position = tell();
  seek(offset, SEEK_SET);
  const uint64_t result = read(buf, chunk_size);
  seek(current_position, SEEK_SET);
#endif  // GRNXX_HAS_PREAD
  return result;
}

uint64_t FileImpl::write(const void *buf, uint64_t size) {
  if (flags_ & FILE_READ_ONLY) {
    GRNXX_ERROR() << "file is read-only";
    GRNXX_THROW();
  }

  const size_t chunk_size =
      static_cast<size_t>(std::min(size, FILE_IMPL_MAX_SIZE));
  const ssize_t result = ::write(fd_, buf, chunk_size);
  if (result < 0) {
    GRNXX_ERROR() << "failed to write to file: file = " << *this
                  << ", size = " << size << ": '::write' " << Error(errno);
    GRNXX_THROW();
  }
  return result;
}

uint64_t FileImpl::write(const void *buf, uint64_t size, uint64_t offset) {
  if (flags_ & FILE_READ_ONLY) {
    GRNXX_ERROR() << "file is read-only";
    GRNXX_THROW();
  }

  if (offset > FILE_IMPL_MAX_OFFSET) {
    GRNXX_ERROR() << "invalid argument: offset = " << offset
                  << ", max_offset = " << FILE_IMPL_MAX_OFFSET;
    GRNXX_THROW();
  }
  const size_t chunk_size =
      static_cast<size_t>(std::min(size, FILE_IMPL_MAX_SIZE));
#ifdef GRNXX_HAS_PWRITE
  const ssize_t result =
      ::pwrite(fd_, buf, chunk_size, static_cast<off_t>(offset));
  if (result < 0) {
    GRNXX_ERROR() << "failed to write file: file = " << *this
                  << ", size = " << size << ", offset = " << offset
                  << ": '::pwrite' " << Error(errno);
  }
#else  // GRNXX_HAS_PWRITE
  // The resulting file offset is not defined on failure.
  const uint64_t current_position = tell();
  seek(offset, SEEK_SET);
  const uint64_t result = write(buf, chunk_size);
  seek(current_position, SEEK_SET);
#endif  // GRNXX_HAS_PWRITE
  return result;
}

void FileImpl::sync() {
  if (::fsync(fd_) != 0) {
    GRNXX_ERROR() << "failed to sync file: file = " << *this
                  << ": '::fsync' " << Error(errno);
    GRNXX_THROW();
  }
}

uint64_t FileImpl::seek(int64_t offset, int whence) {
  if ((offset < std::numeric_limits<off_t>::min()) ||
      (offset > std::numeric_limits<off_t>::max())) {
    GRNXX_ERROR() << "invalid argument: offset = " << offset
                  << ": [" << std::numeric_limits<off_t>::min() << ", "
                  << std::numeric_limits<off_t>::max() << ']';
    GRNXX_THROW();
  }

  switch (whence) {
    case SEEK_SET:
    case SEEK_CUR:
    case SEEK_END: {
      break;
    }
    default: {
      GRNXX_ERROR() << "invalid argument: whence = " << whence;
      GRNXX_THROW();
    }
  }

  const off_t result = ::lseek(fd_, offset, whence);
  if (result == -1) {
    GRNXX_ERROR() << "failed to seek file: file = " << *this
                  << ", offset = " << offset << ", whence = " << whence
                  << ": '::lseek' " << Error(errno);
    GRNXX_THROW();
  }
  return result;
}

uint64_t FileImpl::tell() const {
  const off_t result = ::lseek(fd_, 0, SEEK_CUR);
  if (result == -1) {
    GRNXX_ERROR() << "failed to get current position: file = " << *this
                  << ": '::lseek' " << Error(errno);
    GRNXX_THROW();
  }
  return result;
}

void FileImpl::resize(uint64_t size) {
  if (flags_ & FILE_READ_ONLY) {
    GRNXX_ERROR() << "file is read-only";
    GRNXX_THROW();
  }

  const uint64_t MAX_SIZE = std::numeric_limits<int64_t>::max();
  if (size > MAX_SIZE) {
    GRNXX_ERROR() << "invalid argument: size = " << size
                  << ": [0, " << MAX_SIZE << ']';
    GRNXX_THROW();
  }

  if (::ftruncate(fd_, size) != 0) {
    GRNXX_ERROR() << "failed to resize file: file = " << *this
                  << ", size = " << size
                  << ": '::ftruncate' " << Error(errno);
    GRNXX_THROW();
  }
}

uint64_t FileImpl::size() const {
  struct stat stat;
  if (::fstat(fd_, &stat) != 0) {
    GRNXX_ERROR() << "failed to stat file: file = " << *this
                  << ": '::fstat' " << Error(errno);
    GRNXX_THROW();
  }
  return stat.st_size;
}

FileImpl::FileImpl()
  : path_(), flags_(FileFlags::none()), fd_(-1), locked_(false),
    unlink_at_close_(false) {}

void FileImpl::open_regular_file(FileFlags flags, const char *path,
                                 int permission) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = " << path;
    GRNXX_THROW();
  }
  path_ = path;

  int posix_flags = O_RDWR;

  if ((~flags & FILE_CREATE) && (flags & FILE_READ_ONLY)) {
    flags_ |= FILE_READ_ONLY;
    posix_flags = O_RDONLY;
  } else if (flags & FILE_WRITE_ONLY) {
    flags_ |= FILE_WRITE_ONLY;
    posix_flags = O_WRONLY;
  }

  if ((~flags_ & FILE_READ_ONLY) && (flags & FILE_APPEND)) {
    flags_ |= FILE_APPEND;
    posix_flags |= O_APPEND;
  }

  if (flags & FILE_CREATE) {
    flags_ |= FILE_CREATE;
    posix_flags |= O_CREAT;
    if (flags & FILE_OPEN) {
      flags_ |= FILE_OPEN;
    } else {
      posix_flags |= O_EXCL;
    }
  } else {
    flags_ |= FILE_OPEN;
  }

  if ((flags_ & FILE_OPEN) && (flags & FILE_TRUNCATE)) {
    flags_ |= FILE_TRUNCATE;
    posix_flags |= O_TRUNC;
  }

  fd_ = ::open(path, posix_flags, permission);
  if (fd_ == -1) {
    GRNXX_ERROR() << "failed to open file: path = " << path
                  << ", flags = " << flags
                  << ", permission = " << permission
                  << ": '::open' " << Error(errno);
    GRNXX_THROW();
  }
}

void FileImpl::open_temporary_file(FileFlags flags, const char *path, int) {
  flags_ = FILE_TEMPORARY;

  int posix_flags = O_RDWR | O_CREAT | O_EXCL | O_NOCTTY;
#ifdef O_NOATIME
  posix_flags |= O_NOATIME;
#endif  // O_NOATIME
#ifdef O_NOFOLLOW
  posix_flags |= O_NOFOLLOW;
#endif  // O_NOFOLLOW

  for (int i = 0; i < FILE_UNIQUE_PATH_GENERATION_MAX_NUM_TRIALS; ++i) {
    path_ = Path::unique_path(path);
    fd_ = ::open(path_.c_str(), posix_flags, 0600);
    if (fd_ != -1) {
      unlink(path_.c_str());
      return;
    }
    GRNXX_WARNING() << "failed to create temporary file: path = " << path
                    << ", unique_path = " << path_
                    << ": '::open' " << Error(errno);
  }
  GRNXX_ERROR() << "failed to create temporary file: path = " << path
                << ", flags = " << flags;
  GRNXX_THROW();
}

StringBuilder &FileImpl::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  builder << "{ path = " << path_
          << ", flags = " << flags_
          << ", fd = " << fd_;

  builder << ", size = ";
  struct stat stat;
  if (::fstat(fd_, &stat) != 0) {
    builder << "n/a";
  } else {
    builder << stat.st_size;
  }

  builder << ", offset = ";
  const off_t result = ::lseek(fd_, 0, SEEK_CUR);
  if (result == -1) {
    builder << "n/a";
  } else {
    builder << result;
  }

  return builder << ", locked = " << locked_
                 << ", unlink_at_close = " << unlink_at_close_ << " }";
}

}  // namespace io
}  // namespace grnxx

#endif  // GRNXX_WINDOWS