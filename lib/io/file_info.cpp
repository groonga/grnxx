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
#include "file_info.hpp"
#include "file_info-impl.hpp"

namespace grnxx {
namespace io {

FileInfo::FileInfo() : impl_() {}

FileInfo::FileInfo(const char *path) : impl_(FileInfoImpl::stat(path)) {}

FileInfo::FileInfo(const File &file) : impl_(FileInfoImpl::stat(file)) {}

FileInfo::FileInfo(const FileInfo &file_info) : impl_(file_info.impl_) {}

FileInfo &FileInfo::operator=(const FileInfo &file_info) {
  impl_ = file_info.impl_;
  return *this;
}

FileInfo::FileInfo(FileInfo &&file_info) : impl_(std::move(file_info.impl_)) {}

FileInfo &FileInfo::operator=(FileInfo &&file_info) {
  impl_ = std::move(file_info.impl_);
  return *this;
}

bool FileInfo::is_file() const {
  return impl_ ? impl_->is_file() : false;
}
bool FileInfo::is_directory() const {
  return impl_ ? impl_->is_directory() : false;
}
int64_t FileInfo::device_id() const {
  return impl_ ? impl_->device_id() : 0;
}
int64_t FileInfo::inode_id() const {
  return impl_ ? impl_->inode_id() : 0;
}
int64_t FileInfo::mode_flags() const {
  return impl_ ? impl_->mode_flags() : 0;
}
int64_t FileInfo::num_links() const {
  return impl_ ? impl_->num_links() : 0;
}
int64_t FileInfo::user_id() const {
  return impl_ ? impl_->user_id() : 0;
}
int64_t FileInfo::group_id() const {
  return impl_ ? impl_->group_id() : 0;
}
uint64_t FileInfo::size() const {
  return impl_ ? impl_->size() : 0;
}
Time FileInfo::last_access_time() const {
  return impl_ ? impl_->last_access_time() : Time();
}
Time FileInfo::last_modification_time() const {
  return impl_ ? impl_->last_modification_time() : Time();
}
Time FileInfo::last_status_change_time() const {
  return impl_ ? impl_->last_status_change_time() : Time();
}

void FileInfo::swap(FileInfo &rhs) {
  impl_.swap(rhs.impl_);
}

StringBuilder &FileInfo::write_to(StringBuilder &builder) const {
  return impl_ ? impl_->write_to(builder) : (builder << "n/a");
}

}  // namespace io
}  // namespace grnxx
