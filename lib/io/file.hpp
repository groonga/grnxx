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
#ifndef GRNXX_IO_FILE_HPP
#define GRNXX_IO_FILE_HPP

#include "../duration.hpp"

namespace grnxx {
namespace io {

constexpr int FILE_UNIQUE_PATH_GENERATION_MAX_NUM_TRIALS = 10;

constexpr Duration FILE_LOCK_SLEEP_DURATION = Duration::milliseconds(10);

class FileFlagsIdentifier {};
typedef FlagsImpl<FileFlagsIdentifier> FileFlags;

// FILE_WRITE_ONLY is ignored if FILE_READ_ONLY is enabled.
// FILE_READ_ONLY is disabled if FILE_CREATE is specified.
// If both flags are not set, a file is created/opened in read-write mode.

// Read-only mode.
constexpr FileFlags FILE_READ_ONLY      = FileFlags::define(0x0001);
// Write-only mode.
constexpr FileFlags FILE_WRITE_ONLY     = FileFlags::define(0x0002);

// FILE_APPEND is ignored if FILE_READ_ONLY is enabled.
// FILE_CREATE disables FILE_READ_ONLY.
// FILE_OPEN is enabled if FILE_CREATE is not specified.
// FILE_TEMPORARY disables other flags.

// Append mode.
constexpr FileFlags FILE_APPEND         = FileFlags::define(0x0020);
// Create a file if it does not exist.
constexpr FileFlags FILE_CREATE         = FileFlags::define(0x0040);
// Open an existing file.
constexpr FileFlags FILE_OPEN           = FileFlags::define(0x0100);
// Create a file, if it does not exist, or open an existing file.
constexpr FileFlags FILE_CREATE_OR_OPEN = FILE_CREATE | FILE_OPEN;
// Create a temporary file.
constexpr FileFlags FILE_TEMPORARY      = FileFlags::define(0x0200);
// Truncate an existing file.
constexpr FileFlags FILE_TRUNCATE       = FileFlags::define(0x0400);

StringBuilder &operator<<(StringBuilder &builder, FileFlags flags);
std::ostream &operator<<(std::ostream &builder, FileFlags flags);

enum FileLockMode {
  FILE_LOCK_EXCLUSIVE = 0x1000,  // Create an exclusive lock.
  FILE_LOCK_SHARED    = 0x2000   // Create a shared lock.
};

// Note: Windows ignores permission.
class File {
 public:
  File();
  virtual ~File();

  static File *open(FileFlags flags, const char *path = nullptr,
                    int permission = 0644);

  static bool exists(const char *path);
  static void unlink(const char *path);
  static bool unlink_if_exists(const char *path);

  // The following functions operate advisory locks for files, not for
  // FileImpl instances. The word "advisory" indicates that the file is
  // accessible even if it is locked.

  virtual void lock(FileLockMode mode) = 0;
  // lock() returns true on success, false on failure.
  virtual bool lock(FileLockMode mode, Duration timeout) = 0;
  // try_lock() returns false if the file is already locked.
  virtual bool try_lock(FileLockMode mode) = 0;
  // unlock() returns false if the file is not locked.
  virtual bool unlock() = 0;

  // The following functions are not thread-safe.

  // read() reads data from file at most size bytes and returns the number of
  // actually read bytes.
  virtual uint64_t read(void *buf, uint64_t size) = 0;
  virtual uint64_t read(void *buf, uint64_t size, uint64_t offset) = 0;
  // write() writes data into file at most size bytes and returns the number
  // of actually written bytes.
  virtual uint64_t write(const void *buf, uint64_t size) = 0;
  virtual uint64_t write(const void *buf, uint64_t size, uint64_t offset) = 0;

  virtual void sync() = 0;

  // seek() moves the file pointer and returns the new position.
  virtual uint64_t seek(int64_t offset, int whence = SEEK_SET) = 0;
  // tell() returns the current position.
  virtual uint64_t tell() const = 0;

  // resize() resizes the file and moves the file pointer to the new
  // end-of-file.
  virtual void resize(uint64_t size) = 0;
  // size() returns the file size in bytes.
  virtual uint64_t size() const = 0;

  // If true, the associated path will be unlinked after closing the file
  // handle.
  virtual bool unlink_at_close() const = 0;
  virtual void set_unlink_at_close(bool value) = 0;

  virtual String path() const = 0;
  virtual FileFlags flags() const = 0;

  virtual const void *handle() const = 0;

  virtual StringBuilder &write_to(StringBuilder &builder) const = 0;
};

inline StringBuilder &operator<<(StringBuilder &builder, const File &file) {
  return file.write_to(builder);
}

}  // namespace io
}  // namespace grnxx

#endif  // GRNXX_IO_FILE_HPP
