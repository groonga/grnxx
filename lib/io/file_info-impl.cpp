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
#include "file_info-impl.hpp"

#include <cerrno>

#include "../error.hpp"
#include "../exception.hpp"
#include "../logger.hpp"

namespace grnxx {
namespace io {

std::unique_ptr<FileInfoImpl> FileInfoImpl::stat(const char *path) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = " << path;
    GRNXX_THROW();
  }

  Stat stat;
#ifdef GRNXX_WINDOWS
  if (::_stat(path, &stat) != 0) {
    if (errno != ENOENT) {
      GRNXX_WARNING() << "failed to get file information: path = <" << path
                      << ">: '::_stat' " << Error(errno);
#else  // GRNXX_WINDOWS
  if (::stat(path, &stat) != 0) {
    if (errno != ENOENT) {
      GRNXX_WARNING() << "failed to get file information: path = <" << path
                      << ">: '::stat' " << Error(errno);
#endif  // GRNXX_WINDOWS
    }
    return nullptr;
  }

  std::unique_ptr<FileInfoImpl> file_info(
      new (std::nothrow) FileInfoImpl(stat));
  if (!file_info) {
    GRNXX_ERROR() << "new grnxx::io::FileInfoImpl failed";
    GRNXX_THROW();
  }
  return file_info;
}

std::unique_ptr<FileInfoImpl> FileInfoImpl::stat(const File &file) {
  Stat stat;
#ifdef GRNXX_WINDOWS
  if (::_stat(file.path().c_str(), &stat) != 0) {
    if (errno != ENOENT) {
      GRNXX_WARNING() << "failed to get file information: file = " << file
                      << ": '::_stat' " << Error(errno);
    }
#else  // GRNXX_WINDOWS
  if (::fstat(*static_cast<const int *>(file.handle()), &stat) != 0) {
    GRNXX_WARNING() << "failed to get file information: file = " << file
                    << ": '::fstat' " << Error(errno);
#endif  // GRNXX_WINDOWS
    return nullptr;
  }

  std::unique_ptr<FileInfoImpl> file_info(
      new (std::nothrow) FileInfoImpl(stat));
  if (!file_info) {
    GRNXX_ERROR() << "new grnxx::io::FileInfoImpl failed";
    GRNXX_THROW();
  }
  return file_info;
}

StringBuilder &FileInfoImpl::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  return builder << "{ is_file = " << is_file()
                 << ", is_directory = " << is_directory()
                 << ", device_id = " << device_id()
                 << ", inode_id = " << inode_id()
                 << ", mode_flags = " << mode_flags()
                 << ", num_links = " << num_links()
                 << ", user_id = " << user_id()
                 << ", group_id = " << group_id()
                 << ", size = " << size()
                 << ", last_access_time = " << last_access_time()
                 << ", last_modification_time = " << last_modification_time()
                 << ", last_status_change_time = " << last_status_change_time()
                 << " }";
}

}  // namespace io
}  // namespace grnxx
