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
#ifndef GRNXX_IO_FILE_HPP
#define GRNXX_IO_FILE_HPP

#include "../duration.hpp"

namespace grnxx {
namespace io {

constexpr int FILE_UNIQUE_PATH_GENERATION_MAX_NUM_TRIALS = 10;

constexpr Duration FILE_LOCK_SLEEP_DURATION = Duration::milliseconds(10);

class File;
typedef FlagsImpl<File> FileFlags;

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

  // Create or open a file.
  static File *open(FileFlags flags, const char *path = nullptr,
                    int permission = 0644);

  // Return true iff "path" refers to a regular file.
  static bool exists(const char *path);
  // Remove a file.
  static void unlink(const char *path);
  // Remove a file and return true on success.
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

  // Note: The following operations are not thread-safe. In practice, it is
  // possible to make these operations thread-safe. However, a file might be
  // accessed via multiple objects or processes. It is difficult to cover all
  // the possible cases.

  // Read up to "size" bytes from the file into the buffer starting at "buf"
  // and return the number of bytes read.
  virtual uint64_t read(void *buf, uint64_t size) = 0;
  // Read up to "size" bytes from the file at offset "offset" into the buffer
  // starting at "buf" and return the number of bytes read.
  // The file offset is not changed on success but not defined on failure.
  virtual uint64_t read(void *buf, uint64_t size, uint64_t offset) = 0;

  // Write "size" bytes from the buffer starting at "buf" to the file and
  // return the number of bytes written.
  virtual uint64_t write(const void *buf, uint64_t size) = 0;
  // Write "size" bytes from the buffer starting at "buf" to the file at
  // offset "offset" and return the number of bytes written.
  // The file offset is not changed on success but not defined on failure.
  virtual uint64_t write(const void *buf, uint64_t size, uint64_t offset) = 0;

  // Flush modified pages.
  virtual void sync() = 0;

  // Move the file offset and return the new offset.
  // "whence" must be SEEK_SET, SEEK_CUR, or SEEK_END as follows:
  // - SEEK_SET: "offset" is relative to the start of the file.
  // - SEEK_CUR: "offset" is relative to the current offset.
  // - SEEK_END: "offset" is relative to the end of the file.
  virtual uint64_t seek(int64_t offset, int whence = SEEK_SET) = 0;
  // Return the current offset.
  virtual uint64_t tell() const = 0;

  // Extend or truncate a file to "size" bytes.
  // The file offset is not changed on success but not defined on failure.
  // Note that the contents of the extended part are not defined.
  virtual void resize(uint64_t size) = 0;
  // Return the file size.
  virtual uint64_t size() const = 0;

  // Return the unlink-at-close flag.
  // If the flag is true, the file will be unlinked immediately after close.
  virtual bool unlink_at_close() const = 0;
  // Modify the unlink-at-close flag to "value".
  virtual void set_unlink_at_close(bool value) = 0;

  // Return the file path.
  virtual String path() const = 0;
  // Return the enabled file flags.
  virtual FileFlags flags() const = 0;

  // Return a pointer to the file handle 
  virtual const void *handle() const = 0;

  virtual StringBuilder &write_to(StringBuilder &builder) const = 0;
};

inline StringBuilder &operator<<(StringBuilder &builder, const File &file) {
  return file.write_to(builder);
}

}  // namespace io
}  // namespace grnxx

#endif  // GRNXX_IO_FILE_HPP
