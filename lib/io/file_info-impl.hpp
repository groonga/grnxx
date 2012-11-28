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
#ifndef GRNXX_IO_FILE_INFO_IMPL_HPP
#define GRNXX_IO_FILE_INFO_IMPL_HPP

#include <sys/types.h>
#include <sys/stat.h>

#include "../time.hpp"
#include "file.hpp"

namespace grnxx {
namespace io {

#ifdef GRNXX_WINDOWS
typedef struct _stat Stat;
#else  // GRNXX_WINDOWS
typedef struct stat Stat;
#endif  // GRNXX_WINDOWS

class FileInfoImpl {
 public:
  static std::unique_ptr<FileInfoImpl> stat(const char *path);
  static std::unique_ptr<FileInfoImpl> stat(const File &file);

  bool is_file() const {
#ifdef GRNXX_WINDOWS
    return (stat_.st_mode & _S_IFREG) != 0;
#else  // GRNXX_WINDOWS
    return S_ISREG(stat_.st_mode) != 0;
#endif  // GRNXX_WINDOWS
  }

  bool is_directory() const {
#ifdef GRNXX_WINDOWS
    return (stat_.st_mode & _S_IFDIR) != 0;
#else  // GRNXX_WINDOWS
    return S_ISDIR(stat_.st_mode) != 0;
#endif  // GRNXX_WINDOWS
  }

  int64_t device_id() const {
    return stat_.st_dev;
  }
  int64_t inode_id() const {
    return stat_.st_ino;
  }
  int64_t mode_flags() const {
    return stat_.st_mode;
  }
  int64_t num_links() const {
    return stat_.st_nlink;
  }
  int64_t user_id() const {
    return stat_.st_uid;
  }
  int64_t group_id() const {
    return stat_.st_gid;
  }
  uint64_t size() const {
    return stat_.st_size;
  }
  Time last_access_time() const {
    return Time(static_cast<int64_t>(stat_.st_atime) * 1000000000);
  }
  Time last_modification_time() const {
    return Time(static_cast<int64_t>(stat_.st_mtime) * 1000000000);
  }
  Time last_status_change_time() const {
    return Time(static_cast<int64_t>(stat_.st_ctime) * 1000000000);
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  Stat stat_;

  FileInfoImpl(const Stat &stat) : stat_(stat) {}

  FileInfoImpl(const FileInfoImpl &);
  FileInfoImpl &operator=(const FileInfoImpl &);
};

}  // namespace io
}  // namespace grnxx

#endif  // GRNXX_IO_FILE_INFO_IMPL_HPP
