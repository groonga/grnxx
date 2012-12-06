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
#ifndef FILE_FILE_HPP
#define FILE_FILE_HPP

#include "../duration.hpp"

namespace grnxx {
namespace io {

constexpr int FILE_UNIQUE_PATH_GENERATION_MAX_NUM_TRIALS = 10;

constexpr Duration FILE_LOCK_SLEEP_DURATION = Duration::milliseconds(10);

class FileFlagsIdentifier {};
typedef FlagsImpl<FileFlagsIdentifier> FileFlags;

// FILE_WRITE_ONLY is ignored if FILE_READ_ONLY is enabled.
// FILE_READ_ONLY is disabled if FILE_CREATE is specified.
// If both flags are not set, and object is created/opened/mapped in
// read-write mode.

// Read-only mode.
constexpr FileFlags FILE_READ_ONLY      = FileFlags::define(0x0001);
// Write-only mode.
constexpr FileFlags FILE_WRITE_ONLY     = FileFlags::define(0x0002);

// FILE_APPEND is ignored if FILE_READ_ONLY is enabled.
// FILE_CREATE disables FILE_READ_ONLY.
// FILE_OPEN is enabled if FILE_CREATE is not specified.
// If both FILE_CREATE and FILE_OPEN are set, it first tries to create
// an object and, if already exists, then tries to open the existing object.
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

enum FileLockMode {
  FILE_LOCK_EXCLUSIVE = 0x1000,  // Create an exclusive lock.
  FILE_LOCK_SHARED    = 0x2000   // Create a shared lock.
};

class FileImpl;

// Note: Windows ignores permission.
class File {
 public:
  File();
  explicit File(FileFlags flags, const char *path = nullptr,
                int permission = 0644);
  ~File();

  File(const File &file);
  File &operator=(const File &file);

  File(File &&file);
  File &operator=(File &&file);

  explicit operator bool() const {
    return static_cast<bool>(impl_);
  }

  void open(FileFlags flags, const char *path) {
    *this = File(flags | FILE_OPEN, path);
  }
  void close() {
    *this = File();
  }

  // The following functions operate advisory locks for files, not for
  // FileImpl instances. The word "advisory" indicates that the file is
  // accessible even if it is locked.

  void lock(FileLockMode mode);
  // lock() returns true on success, false on failure.
  bool lock(FileLockMode mode, Duration timeout);
  // try_lock() returns false if the file is already locked.
  bool try_lock(FileLockMode mode);
  // unlock() returns false if the file is not locked.
  bool unlock();

  // The following functions are not thread-safe.

  // read() reads data from file at most size bytes and returns the number of
  // actually read bytes.
  uint64_t read(void *buf, uint64_t size);
  uint64_t read(void *buf, uint64_t size, uint64_t offset);
  // write() writes data into file at most size bytes and returns the number
  // of actually written bytes.
  uint64_t write(const void *buf, uint64_t size);
  uint64_t write(const void *buf, uint64_t size, uint64_t offset);

  void sync();

  // seek() moves the file pointer and returns the new position.
  uint64_t seek(int64_t offset, int whence = SEEK_SET);
  // tell() returns the current position.
  uint64_t tell() const;

  // resize() resizes the file and moves the file pointer to the new
  // end-of-file.
  void resize(uint64_t size);
  // size() returns the file size in bytes.
  uint64_t size() const;

  // If true, the associated path will be unlinked after closing the file
  // handle.
  bool unlink_at_close() const;
  void set_unlink_at_close(bool value);

  String path() const;
  FileFlags flags() const;

  const void *handle() const;

  void swap(File &file);

  StringBuilder &write_to(StringBuilder &builder) const;

  static bool exists(const char *path);
  static void unlink(const char *path);
  static bool unlink_if_exists(const char *path);

 private:
  std::shared_ptr<FileImpl> impl_;

  void throw_if_impl_is_invalid() const;
};

inline void swap(File &lhs, File &rhs) {
  lhs.swap(rhs);
}

inline StringBuilder &operator<<(StringBuilder &builder, const File &file) {
  return file.write_to(builder);
}

}  // namespace io
}  // namespace grnxx

#endif  // FILE_FILE_HPP
