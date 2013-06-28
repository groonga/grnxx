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
#include "grnxx/io/file_info.hpp"

#include <sys/types.h>
#include <sys/stat.h>

#include <cerrno>

#include "grnxx/error.hpp"
#include "grnxx/exception.hpp"
#include "grnxx/io/file.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/string_builder.hpp"

namespace grnxx {
namespace io {
namespace {

#ifdef GRNXX_WINDOWS
typedef struct _stat Stat;
#else  // GRNXX_WINDOWS
typedef struct stat Stat;
#endif  // GRNXX_WINDOWS

class Impl : public FileInfo {
 public:
  static Impl *stat(const char *path);
  static Impl *stat(File *file);

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
    return Time(static_cast<int64_t>(stat_.st_atime) * 1000000);
  }
  Time last_modification_time() const {
    return Time(static_cast<int64_t>(stat_.st_mtime) * 1000000);
  }
  Time last_status_change_time() const {
    return Time(static_cast<int64_t>(stat_.st_ctime) * 1000000);
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  Stat stat_;

  Impl(const Stat &stat) : stat_(stat) {}
};

Impl *Impl::stat(const char *path) {
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

  std::unique_ptr<Impl> file_info(new (std::nothrow) Impl(stat));
  if (!file_info) {
    GRNXX_ERROR() << "new grnxx::io::{anonymous_namespace}::Impl failed";
    GRNXX_THROW();
  }
  return file_info.release();
}

Impl *Impl::stat(File *file) {
  Stat stat;
#ifdef GRNXX_WINDOWS
  if (::_stat(file->path().c_str(), &stat) != 0) {
    if (errno != ENOENT) {
      GRNXX_WARNING() << "failed to get file information: file = " << *file
                      << ": '::_stat' " << Error(errno);
    }
#else  // GRNXX_WINDOWS
  if (::fstat(*static_cast<const int *>(file->handle()), &stat) != 0) {
    GRNXX_WARNING() << "failed to get file information: file = " << *file
                    << ": '::fstat' " << Error(errno);
#endif  // GRNXX_WINDOWS
    return nullptr;
  }

  std::unique_ptr<Impl> file_info(new (std::nothrow) Impl(stat));
  if (!file_info) {
    GRNXX_ERROR() << "new grnxx::io::{anonymous_namespace}::Impl failed";
    GRNXX_THROW();
  }
  return file_info.release();
}

StringBuilder &Impl::write_to(StringBuilder &builder) const {
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
                 << ", last_access_time = "
                 << last_access_time().local_time()
                 << ", last_modification_time = "
                 << last_modification_time().local_time()
                 << ", last_status_change_time = "
                 << last_status_change_time().local_time()
                 << " }";
}

}  // namespace

FileInfo *FileInfo::stat(const char *path) {
  return Impl::stat(path);
}

FileInfo *FileInfo::stat(File *file) {
  return Impl::stat(file);
}

FileInfo::FileInfo() {}
FileInfo::~FileInfo() {}

}  // namespace io
}  // namespace grnxx
