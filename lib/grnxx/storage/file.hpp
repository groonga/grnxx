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
#ifndef GRNXX_STORAGE_FILE_HPP
#define GRNXX_STORAGE_FILE_HPP

#include "grnxx/basic.hpp"
#include "grnxx/flags_impl.hpp"

namespace grnxx {

class StringBuilder;

namespace storage {

class File;
using FileFlags = FlagsImpl<File>;

// Use the default settings.
constexpr FileFlags FILE_DEFAULT   = FileFlags::define(0x00);
// Open a file in read-only mode.
constexpr FileFlags FILE_READ_ONLY = FileFlags::define(0x01);
// Create a temporary file.
// This flag is implicitly enabled if "path" == nullptr.
constexpr FileFlags FILE_TEMPORARY = FileFlags::define(0x02);

StringBuilder &operator<<(StringBuilder &builder, FileFlags flags);

class FileLock;
using FileLockFlags = FlagsImpl<FileLock>;

// Apply an exclusive advisory lock.
constexpr FileLockFlags FILE_LOCK_SHARED      = FileLockFlags::define(0x01);
// Apply a shared advisory lock.
constexpr FileLockFlags FILE_LOCK_EXCLUSIVE   = FileLockFlags::define(0x02);
// Immediately return the result when the file is locked.
constexpr FileLockFlags FILE_LOCK_NONBLOCKING = FileLockFlags::define(0x04);

StringBuilder &operator<<(StringBuilder &builder, FileLockFlags flags);

class File {
 public:
  File();
  virtual ~File();

  // Create a file.
  // FILE_TEMPORARY is implicitly enabled if "path" == nullptr.
  // The available flag is FILE_TEMPORARY.
  static File *create(const char *path,
                      FileFlags flags = FILE_DEFAULT);
  // Open a file.
  // The available flag is FILE_READ_ONLY.
  static File *open(const char *path,
                    FileFlags flags = FILE_DEFAULT);
  // Open or create a file.
  // There are no available flags.
  static File *open_or_create(const char *path,
                              FileFlags flags = FILE_DEFAULT);

  // Return true iff "path" refers to a regular file.
  static bool exists(const char *path);
  // Unlink a file and return true on success.
  static bool unlink(const char *path);

  // Try to lock a file and return true on success.
  // Note that the file is accessible even if it is locked (advisory).
  virtual bool lock(FileLockFlags lock_flags) = 0;
  // Unlock a file and return true on success.
  virtual bool unlock() = 0;

  // Flush modified pages and return true on success.
  virtual bool sync() = 0;

  // Extend or truncate a file to "size" bytes.
  // Note that the contents of the extended part are undefined.
  virtual bool resize(int64_t size) = 0;
  // Return the file size on success, or a negative value on failure.
  virtual int64_t size() const = 0;

  // Return the file path.
  virtual const char *path() const = 0;
  // Return the activated flags.
  virtual FileFlags flags() const = 0;
  // Return a pointer to the file handle 
  virtual const void *handle() const = 0;
};

}  // namespace storage
}  // namespace grnxx

#endif  // GRNXX_STORAGE_FILE_HPP
