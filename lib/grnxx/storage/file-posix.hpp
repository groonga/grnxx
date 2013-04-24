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
#ifndef GRNXX_STORAGE_FILE_POSIX_HPP
#define GRNXX_STORAGE_FILE_POSIX_HPP

#include "grnxx/storage/file.hpp"

#ifndef GRNXX_WINDOWS

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

  bool resize(int64_t size);
  int64_t size() const;

  const char *path() const;
  FileFlags flags() const;
  const void *handle() const;

 private:
  std::unique_ptr<char[]> path_;
  FileFlags flags_;
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

#endif  // GRNXX_STORAGE_FILE_POSIX_HPP
