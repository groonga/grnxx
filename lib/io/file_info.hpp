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
#ifndef GRNXX_IO_FILE_INFO_HPP
#define GRNXX_IO_FILE_INFO_HPP

#include "../time.hpp"
#include "file.hpp"

namespace grnxx {
namespace io {

class FileInfoImpl;

class FileInfo {
 public:
  FileInfo();
  explicit FileInfo(const char *path);
  explicit FileInfo(const File &file);

  FileInfo(const FileInfo &file_info);
  FileInfo &operator=(const FileInfo &file_info);

  FileInfo(FileInfo &&file_info);
  FileInfo &operator=(FileInfo &&file_info);

  explicit operator bool() const {
    return static_cast<bool>(impl_);
  }

  bool is_file() const;
  bool is_directory() const;
  int64_t device_id() const;
  int64_t inode_id() const;
  int64_t mode_flags() const;
  int64_t num_links() const;
  int64_t user_id() const;
  int64_t group_id() const;
  uint64_t size() const;
  Time last_access_time() const;
  Time last_modification_time() const;
  Time last_status_change_time() const;

  void swap(FileInfo &rhs);

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  std::shared_ptr<FileInfoImpl> impl_;

  // Copyable.
};

inline void swap(FileInfo &lhs, FileInfo &rhs) {
  lhs.swap(rhs);
}

inline StringBuilder &operator<<(StringBuilder &builder,
                                 const FileInfo &file_info) {
  return file_info.write_to(builder);
}

}  // namespace io
}  // namespace grnxx

#endif  // GRNXX_IO_FILE_INFO_HPP
