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
#ifndef GRNXX_IO_FILE_WINDOWS_HPP
#define GRNXX_IO_FILE_WINDOWS_HPP

#include "file.hpp"

#ifdef GRNXX_WINDOWS

#ifndef NOMINMAX
# define NOMINMAX
#endif  // NOMINMAX
#include <windows.h>

namespace grnxx {
namespace io {

class FileImpl {
 public:
  ~FileImpl();

  static std::unique_ptr<FileImpl> open(const char *path, Flags flags,
                                        int permission);

  bool lock(LockMode mode, int sleep_count, Duration sleep_duration);
  bool try_lock(LockMode mode);
  bool unlock();

  bool locked() const {
    return locked_;
  }
  bool unlocked() const {
    return !locked_;
  }

  uint64_t read(void *buf, uint64_t size);
  uint64_t read(void *buf, uint64_t size, uint64_t offset);
  uint64_t write(const void *buf, uint64_t size);
  uint64_t write(const void *buf, uint64_t size,
                      uint64_t offset);

  void sync();

  uint64_t seek(int64_t offset, int whence);
  uint64_t tell() const;

  void resize(uint64_t size);
  uint64_t size() const;

  bool unlink_at_close() const {
    return unlink_at_close_;
  }
  void set_unlink_at_close(bool value) {
    unlink_at_close_ = value;
  }

  String path() const {
    return path_;
  }
  Flags flags() const {
    return flags_;
  }

  const void *handle() const {
   return &handle_;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

  static bool exists(const char *path);
  static void unlink(const char *path);
  static bool unlink_if_exists(const char *path);

 private:
  String path_;
  Flags flags_;
  HANDLE handle_;
  bool append_mode_;
  bool locked_;
  bool unlink_at_close_;

  FileImpl();

  void open_regular_file(const char *path, Flags flags, int permission);
  void open_temporary_file(const char *path, Flags flags, int permission);

  FileImpl(const FileImpl &);
  FileImpl &operator=(const FileImpl &);
};

inline StringBuilder &operator<<(StringBuilder &builder, const FileImpl &file) {
  return file.write_to(builder);
}

}  // namespace io
}  // namespace grnxx

#endif  // GRNXX_WINDOWS

#endif  // GRNXX_IO_FILE_WINDOWS_HPP
