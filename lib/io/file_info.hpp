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

namespace grnxx {
namespace io {

class File;

class FileInfo {
 public:
  FileInfo();
  virtual ~FileInfo();

  // Return nullptr iff "path" is not a valid path.
  static FileInfo *stat(const char *path);
  static FileInfo *stat(const File &file);

  virtual bool is_file() const = 0;
  virtual bool is_directory() const = 0;
  virtual int64_t device_id() const = 0;
  virtual int64_t inode_id() const = 0;
  virtual int64_t mode_flags() const = 0;
  virtual int64_t num_links() const = 0;
  virtual int64_t user_id() const = 0;
  virtual int64_t group_id() const = 0;
  virtual uint64_t size() const = 0;
  virtual Time last_access_time() const = 0;
  virtual Time last_modification_time() const = 0;
  virtual Time last_status_change_time() const = 0;

  virtual StringBuilder &write_to(StringBuilder &builder) const = 0;
};

inline StringBuilder &operator<<(StringBuilder &builder,
                                 const FileInfo &file_info) {
  return file_info.write_to(builder);
}

}  // namespace io
}  // namespace grnxx

#endif  // GRNXX_IO_FILE_INFO2_HPP
