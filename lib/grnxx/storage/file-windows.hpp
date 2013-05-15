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
#ifndef GRNXX_STORAGE_FILE_WINDOWS_HPP
#define GRNXX_STORAGE_FILE_WINDOWS_HPP

#include "grnxx/features.hpp"

#ifdef GRNXX_WINDOWS

#include <windows.h>

#include <memory>

#include "grnxx/storage/file.hpp"

// FILE_READ_ONLY is defined as a macro in windows.h.
#ifdef FILE_READ_ONLY
# undef FILE_READ_ONLY
#endif  // FILE_READ_ONLY

namespace grnxx {
namespace storage {

class FileImpl : public File {
 public:
  FileImpl();
  ~FileImpl();

  static FileImpl *create(const char *path, FileFlags flags);
  static FileImpl *open(const char *path, FileFlags flags);
  static FileImpl *open_or_create(const char *path, FileFlags flags);

  static bool exists(const char *path);
  static bool unlink(const char *path);

  bool lock(FileLockFlags lock_flags);
  bool unlock();

  bool sync();

  bool resize(uint64_t size);
  bool get_size(uint64_t *size);

  const char *path() const;
  FileFlags flags() const;
  const void *handle() const;

 private:
  std::unique_ptr<char[]> path_;
  FileFlags flags_;
  HANDLE handle_;
  int fd_;
  bool locked_;

  bool create_persistent_file(const char *path, FileFlags flags);
  bool create_temporary_file(const char *path_prefix, FileFlags flags);
  bool open_file(const char *path, FileFlags flags);
  bool open_or_create_file(const char *path, FileFlags flags);
};

}  // namespace storage
}  // namespace grnxx

#endif  // GRNXX_WINDOWS

#endif  // GRNXX_STORAGE_FILE_WINDOWS_HPP
