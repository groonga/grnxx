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
#include "file.hpp"
#include "file-posix.hpp"
#include "file-windows.hpp"

#include "../exception.hpp"
#include "../logger.hpp"

namespace grnxx {
namespace io {

File::File() : impl_() {}

File::File(const char *path, Flags flags, int permission)
  : impl_(FileImpl::open(path, flags, permission)) {}

File::~File() {}

File::File(const File &file) : impl_(file.impl_) {}

File &File::operator=(const File &file) {
  impl_ = file.impl_;
  return *this;
}

File::File(File &&file) : impl_(std::move(file.impl_)) {}

File &File::operator=(File &&file) {
  impl_ = std::move(file.impl_);
  return *this;
}

bool File::lock(LockMode mode, int sleep_count, Duration sleep_duration) {
  throw_if_impl_is_invalid();
  return impl_->lock(mode, sleep_count, sleep_duration);
}

bool File::try_lock(LockMode mode) {
  throw_if_impl_is_invalid();
  return impl_->try_lock(mode);
}

bool File::unlock() {
  throw_if_impl_is_invalid();
  return impl_->unlock();
}

bool File::locked() const {
  throw_if_impl_is_invalid();
  return impl_->locked();
}

bool File::unlocked() const {
  throw_if_impl_is_invalid();
  return impl_->unlocked();
}

uint64_t File::read(void *buf, uint64_t size) {
  throw_if_impl_is_invalid();
  return impl_->read(buf, size);
}

uint64_t File::read(void *buf, uint64_t size, uint64_t offset) {
  throw_if_impl_is_invalid();
  return impl_->read(buf, size, offset);
}

uint64_t File::write(const void *buf, uint64_t size) {
  throw_if_impl_is_invalid();
  return impl_->write(buf, size);
}

uint64_t File::write(const void *buf, uint64_t size,
                          uint64_t offset) {
  throw_if_impl_is_invalid();
  return impl_->write(buf, size, offset);
}

void File::sync() {
  throw_if_impl_is_invalid();
  impl_->sync();
}

uint64_t File::seek(int64_t offset, int whence) {
  throw_if_impl_is_invalid();
  return impl_->seek(offset, whence);
}

uint64_t File::tell() const {
  throw_if_impl_is_invalid();
  return impl_->tell();
}

void File::resize(uint64_t size) {
  throw_if_impl_is_invalid();
  impl_->resize(size);
}

uint64_t File::size() const {
  throw_if_impl_is_invalid();
  return impl_->size();
}

bool File::unlink_at_close() const {
  return impl_ ? impl_->unlink_at_close() : false;
}

void File::set_unlink_at_close(bool value) {
  throw_if_impl_is_invalid();
  impl_->set_unlink_at_close(value);
}

String File::path() const {
  return impl_ ? impl_->path() : String();
}

Flags File::flags() const {
  return impl_ ? impl_->flags() : Flags();
}

const void *File::handle() const {
  return impl_ ? impl_->handle() : nullptr;
}

void File::swap(File &file) {
  impl_.swap(file.impl_);
}

bool File::exists(const char *path) {
  return FileImpl::exists(path);
}

void File::unlink(const char *path) {
  FileImpl::unlink(path);
}

bool File::unlink_if_exists(const char *path) {
  return FileImpl::unlink_if_exists(path);
}

void File::throw_if_impl_is_invalid() const {
  if (!impl_) {
    GRNXX_ERROR() << "invalid instance: file = " << *this;
    GRNXX_THROW();
  }
}

StringBuilder &File::write_to(StringBuilder &builder) const {
  return impl_ ? impl_->write_to(builder) : (builder << "n/a");
}

}  // namespace io
}  // namespace grnxx
