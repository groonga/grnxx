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
#include "view.hpp"
#include "view-posix.hpp"
#include "view-windows.hpp"

#include "../exception.hpp"
#include "../logger.hpp"

namespace grnxx {
namespace io {

View::View() : impl_() {}

View::View(Flags flags, uint64_t size)
  : impl_(ViewImpl::map(flags, size)) {}

View::View(const File &file, Flags flags)
  : impl_(ViewImpl::map(file, flags)) {}

View::View(const File &file, Flags flags, uint64_t offset, uint64_t size)
  : impl_(ViewImpl::map(file, flags, offset, size)) {}

View::~View() {}

View::View(const View &view) : impl_(view.impl_) {}

View &View::operator=(const View &view) {
  impl_ = view.impl_;
  return *this;
}

View::View(View &&view)
  : impl_(std::move(view.impl_)) {}

View &View::operator=(View &&view) {
  impl_ = std::move(view.impl_);
  return *this;
}

void View::sync() {
  if (impl_) {
    impl_->sync();
  }
}

void View::sync(uint64_t offset, uint64_t size) {
  if (impl_) {
    impl_->sync(offset, size);
  }
}

File View::file() const {
  return impl_ ? impl_->file() : File();
}

Flags View::flags() const {
  return impl_ ? impl_->flags() : Flags::none();
}

void *View::address() const {
  return impl_ ? impl_->address() : nullptr;
}

uint64_t View::offset() const {
  return impl_ ? impl_->offset() : 0;
}

uint64_t View::size() const {
  return impl_ ? impl_->size() : 0;
}

void View::swap(View &view) {
  impl_.swap(view.impl_);
}

StringBuilder &View::write_to(StringBuilder &builder) const {
  return impl_ ? impl_->write_to(builder) : (builder << "n/a");
}

}  // namespace io
}  // namespace grnxx
