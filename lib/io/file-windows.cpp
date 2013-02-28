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
#include "file-windows.hpp"

#ifdef GRNXX_WINDOWS

#include <sys/types.h>
#include <sys/stat.h>
#include <io.h>

#include "../error.hpp"
#include "../exception.hpp"
#include "../logger.hpp"
#include "../stopwatch.hpp"
#include "../thread.hpp"
#include "path.hpp"

namespace grnxx {
namespace io {

FileImpl::~FileImpl() {
  if (handle_ != INVALID_HANDLE_VALUE) {
    unlock();
    if (!::CloseHandle(handle_)) {
      GRNXX_ERROR() << "failed to close file: file = " << *this
                    << ": '::CloseHandle' " << Error(::GetLastError());
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

  struct _stat stat;
  if (::_stat(path, &stat) != 0) {
    return false;
  }
  return stat.st_mode & _S_IFREG;
}

void FileImpl::unlink(const char *path) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = " << path;
    GRNXX_THROW();
  }

  if (!::DeleteFile(path)) {
    GRNXX_ERROR() << "failed to unlink file: path = " << path
                  << ": '::DeleteFile' " << Error(::GetLastError());
    GRNXX_THROW();
  }
}

bool FileImpl::unlink_if_exists(const char *path) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = " << path;
    GRNXX_THROW();
  }

  return ::DeleteFile(path);
}

void FileImpl::lock(FileLockMode mode) {
  if (locked_) {
    GRNXX_ERROR() << "deadlock: file = " << *this;
    GRNXX_THROW();
  }
  while (!try_lock(mode)) {
    Thread::sleep(FILE_LOCK_SLEEP_DURATION);
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
    Thread::sleep(FILE_LOCK_SLEEP_DURATION);
  }
  return false;
}

bool FileImpl::try_lock(FileLockMode mode) {
  if (locked_) {
    return false;
  }

  DWORD flags = LOCKFILE_FAIL_IMMEDIATELY;
  switch (mode) {
    case FILE_LOCK_SHARED: {
      break;
    }
    case FILE_LOCK_EXCLUSIVE: {
      flags |= LOCKFILE_EXCLUSIVE_LOCK;
      break;
    }
    default: {
      GRNXX_ERROR() << "invalid argument: mode = " << mode;
      GRNXX_THROW();
    }
  }

  OVERLAPPED overlapped;
  overlapped.Offset = 0;
  overlapped.OffsetHigh = 0x80000000U;
  if (!::LockFileEx(handle_, flags, 0, 0, 0x80000000U, &overlapped)) {
    const DWORD last_error = ::GetLastError();
    if (last_error == ERROR_LOCK_FAILED) {
      return false;
    }
    GRNXX_ERROR() << "failed to lock file: file = " << *this
                  << ", mode = " << mode
                  << ": '::LockFileEx' " << Error(last_error);
    GRNXX_THROW();
  }
  locked_ = true;
  return true;
}

bool FileImpl::unlock() {
  if (!locked_) {
    return false;
  }

  OVERLAPPED overlapped;
  overlapped.Offset = 0;
  overlapped.OffsetHigh = 0x80000000U;
  if (!::UnlockFileEx(handle_, 0, 0, 0x80000000U, &overlapped)) {
    GRNXX_ERROR() << "failed to unlock file: file = " << *this
                  << ": '::UnlockFileEx' " << Error(::GetLastError());
    GRNXX_THROW();
  }
  locked_ = false;
  return true;
}

uint64_t FileImpl::read(void *buf, uint64_t size) {
  if (flags_ & FILE_WRITE_ONLY) {
    GRNXX_ERROR() << "file is write-only";
    GRNXX_THROW();
  }

  uint64_t offset = 0;
  while (offset < size) {
    const DWORD chunk_size = static_cast<DWORD>(std::min(size - offset,
        static_cast<uint64_t>(std::numeric_limits<DWORD>::max())));
    DWORD input_size;
    if (!::ReadFile(handle_, buf, chunk_size, &input_size, nullptr)) {
      GRNXX_ERROR() << "failed to read from file: file = " << *this
                    << ", size = " << size
                    << ": '::ReadFile' " << Error(::GetLastError());
      GRNXX_THROW();
    } else if (input_size == 0) {
      break;
    }
    offset += input_size;
  }
  return offset;
}

uint64_t FileImpl::read(void *buf, uint64_t size, uint64_t offset) {
  if (flags_ & FILE_WRITE_ONLY) {
    GRNXX_ERROR() << "file is write-only";
    GRNXX_THROW();
  }

  // The resulting file offset is not defined on failure.
  const uint64_t current_position = seek(0, SEEK_CUR);
  seek(offset, SEEK_SET);
  const uint64_t result = read(buf, size);
  seek(current_position, SEEK_SET);
  return result;
}

uint64_t FileImpl::write(const void *buf, uint64_t size) {
  if (flags_ & FILE_READ_ONLY) {
    GRNXX_ERROR() << "file is read-only";
    GRNXX_THROW();
  }

  if (append_mode_) { 
    seek(0, SEEK_END);
  }
  uint64_t offset = 0;
  while (offset < size) {
    const DWORD chunk_size = static_cast<DWORD>(std::min(size - offset,
        static_cast<uint64_t>(std::numeric_limits<DWORD>::max())));
    DWORD output_size;
    if (!::WriteFile(handle_, buf, chunk_size, &output_size, nullptr)) {
      GRNXX_ERROR() << "failed to write to file: file = " << *this
                    << ", size = " << size
                    << ": '::WriteFile' " << Error(::GetLastError());
      GRNXX_THROW();
    } else if (output_size == 0) {
      break;
    }
    offset += output_size;
  }
  return offset;
}

uint64_t FileImpl::write(const void *buf, uint64_t size, uint64_t offset) {
  if (flags_ & FILE_READ_ONLY) {
    GRNXX_ERROR() << "file is read-only";
    GRNXX_THROW();
  }

  // The resulting file offset is not defined on failure.
  const uint64_t current_position = seek(0, SEEK_CUR);
  seek(offset, SEEK_SET);
  const uint64_t result = write(buf, size);
  seek(current_position, SEEK_SET);
  return result;
}

void FileImpl::sync() {
  if (!::FlushFileBuffers(handle_)) {
    GRNXX_ERROR() << "failed to sync file: file = " << *this
                  << ": '::FlushFileBuffers' " << Error(errno);
    GRNXX_THROW();
  }
}

uint64_t FileImpl::seek(int64_t offset, int whence) {
  DWORD move_method;
  switch (whence) {
    case SEEK_SET: {
      move_method = FILE_BEGIN;
      break;
    }
    case SEEK_CUR: {
      move_method = FILE_CURRENT;
      break;
    }
    case SEEK_END: {
      move_method = FILE_END;
      break;
    }
    default: {
      GRNXX_ERROR() << "invalid argument: whence = " << whence;
      GRNXX_THROW();
    }
  }

  LARGE_INTEGER request;
  LARGE_INTEGER new_position;
  request.QuadPart = offset;
  if (!::SetFilePointerEx(handle_, request, &new_position, move_method)) {
    GRNXX_ERROR() << "failed to seek file: file = " << *this
                  << ", offset = " << offset << ", whence = " << whence
                  << ": '::SetFilePointerEx' " << Error(::GetLastError());
    GRNXX_THROW();
  }
  return new_position.QuadPart;
}

uint64_t FileImpl::tell() const {
  LARGE_INTEGER zero;
  zero.QuadPart = 0;
  LARGE_INTEGER current_position;
  if (!::SetFilePointerEx(handle_, zero, &current_position, FILE_CURRENT)) {
    GRNXX_ERROR() << "failed to get current position: file = " << *this
                  << ": '::SetFilePointerEx' " << Error(::GetLastError());
    GRNXX_THROW();
  }
  return current_position.QuadPart;
}

void FileImpl::resize(uint64_t size) {
  if (flags_ & FILE_READ_ONLY) {
    GRNXX_ERROR() << "file is read-only";
    GRNXX_THROW();
  }

  const uint64_t offset = tell();
  seek(size, SEEK_SET);
  if (!::SetEndOfFile(handle_)) {
    GRNXX_ERROR() << "failed to resize file: file = " << *this
                  << ", size = " << size
                  << ": '::SetEndOfFile' " << Error(::GetLastError());
    GRNXX_THROW();
  }
  seek(offset, SEEK_SET);
}

uint64_t FileImpl::size() const {
  LARGE_INTEGER file_size;
  if (!::GetFileSizeEx(handle_, &file_size)) {
    GRNXX_ERROR() << "failed to get file size: file = " << *this
                  << ": '::GetFileSizeEx' " << Error(::GetLastError());
    GRNXX_THROW();
  }
  return file_size.QuadPart;
}

FileImpl::FileImpl()
  : path_(), flags_(FileFlags::none()), handle_(INVALID_HANDLE_VALUE),
    append_mode_(false), locked_(false), unlink_at_close_(false) {}

void FileImpl::open_regular_file(FileFlags flags, const char *path,
                                 int permission) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = " << path;
    GRNXX_THROW();
  }
  path_ = path;

  DWORD desired_access = GENERIC_READ | GENERIC_WRITE;
  if ((~flags & FILE_CREATE) && (flags & FILE_READ_ONLY)) {
    flags_ |= FILE_READ_ONLY;
    desired_access = GENERIC_READ;
  } else if (flags & FILE_WRITE_ONLY) {
    flags_ |= FILE_WRITE_ONLY;
    desired_access = GENERIC_WRITE;
  }

  if ((~flags_ & FILE_READ_ONLY) && (flags & FILE_APPEND)) {
    flags_ |= FILE_APPEND;
    append_mode_ = true;
  }

  const DWORD share_mode =
      FILE_SHARE_DELETE | FILE_SHARE_READ | FILE_SHARE_WRITE;

  DWORD creation_disposition;
  if (flags & FILE_CREATE) {
    flags_ |= FILE_CREATE;
    creation_disposition = CREATE_NEW;
    if (flags & FILE_OPEN) {
      flags_ |= FILE_OPEN;
      creation_disposition = OPEN_ALWAYS;
      if (flags & FILE_TRUNCATE) {
        flags_ |= FILE_TRUNCATE;
        creation_disposition = CREATE_ALWAYS;
      }
    }
  } else {
    flags_ |= FILE_OPEN;
    creation_disposition = OPEN_EXISTING;
    if (flags & FILE_TRUNCATE) {
      flags_ |= FILE_TRUNCATE;
      creation_disposition = TRUNCATE_EXISTING;
    }
  }

  const DWORD flags_and_attributes = FILE_ATTRIBUTE_NORMAL;

  handle_ = ::CreateFileA(path, desired_access, share_mode, nullptr,
                          creation_disposition, flags_and_attributes, nullptr);
  if (handle_ == INVALID_HANDLE_VALUE) {
    GRNXX_ERROR() << "failed to open file: path = " << path
                  << ", flags = " << flags
                  << ", permission = " << permission
                  << ": '::CreateFileA' " << Error(::GetLastError());
    GRNXX_THROW();
  }
}

void FileImpl::open_temporary_file(FileFlags flags, const char *path,
                                   int permission) try {
  flags_ = FILE_TEMPORARY;

  const DWORD desired_access = GENERIC_READ | GENERIC_WRITE;
  const DWORD share_mode = FILE_SHARE_DELETE;
  const DWORD creation_disposition = CREATE_NEW;
  const DWORD flags_and_attributes = FILE_ATTRIBUTE_TEMPORARY |
                                     FILE_FLAG_DELETE_ON_CLOSE;

  for (int i = 0; i < FILE_UNIQUE_PATH_GENERATION_MAX_NUM_TRIALS; ++i) {
    path_ = Path::unique_path(path);
    handle_ = ::CreateFileA(path_.c_str(), desired_access,
                            share_mode, nullptr, creation_disposition,
                            flags_and_attributes, nullptr);
    if (handle_ != INVALID_HANDLE_VALUE) {
      return;
    }
    GRNXX_WARNING() << "failed to create temporary file: path = " << path
                    << ", unique_path = " << path_
                    << ": '::open' " << Error(::GetLastError());
  }
  GRNXX_ERROR() << "failed to create temporary file: path = " << path
                << ", flags = " << flags
                << ", permission = " << permission;
  GRNXX_THROW();
} catch (const std::exception &exception) {
  GRNXX_ERROR() << exception;
  GRNXX_THROW();
}

StringBuilder &FileImpl::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  builder << "{ path = " << path_
          << ", flags = " << flags_;

  builder << ", size = ";
  LARGE_INTEGER file_size;
  if (!::GetFileSizeEx(handle_, &file_size)) {
    builder << "n/a";
  } else {
    builder << file_size.QuadPart;
  }

  builder << ", offset = ";
  LARGE_INTEGER zero;
  zero.QuadPart = 0;
  LARGE_INTEGER current_position;
  if (!::SetFilePointerEx(handle_, zero, &current_position, FILE_CURRENT)) {
    builder << "n/a";
  } else {
    builder << current_position.QuadPart;
  }

  return builder << ", append_mode = " << append_mode_
                 << ", locked = " << locked_
                 << ", unlink_at_close = " << unlink_at_close_ << " }";
}

}  // namespace io
}  // namespace grnxx

#endif  // GRNXX_WINDOWS
