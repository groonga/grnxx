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

#define GRNXX_FLAGS_WRITE(flag) do { \
  if (flags & flag) { \
    if (!is_first) { \
      builder << " | "; \
    } \
    builder << #flag; \
    is_first = false; \
  } \
} while (false)

StringBuilder &operator<<(StringBuilder &builder, ViewFlags flags) {
  if (flags) {
    bool is_first = true;
    GRNXX_FLAGS_WRITE(VIEW_READ_ONLY);
    GRNXX_FLAGS_WRITE(VIEW_WRITE_ONLY);
    GRNXX_FLAGS_WRITE(VIEW_ANONYMOUS);
    GRNXX_FLAGS_WRITE(VIEW_HUGE_TLB);
    GRNXX_FLAGS_WRITE(VIEW_PRIVATE);
    GRNXX_FLAGS_WRITE(VIEW_SHARED);
    return builder;
  } else {
    return builder << "0";
  }
}

View::View() : impl_() {}

View::View(ViewFlags flags, uint64_t size)
  : impl_(ViewImpl::map(flags, size)) {}

View::View(ViewFlags flags, const File &file)
  : impl_(ViewImpl::map(flags, file)) {}

View::View(ViewFlags flags, const File &file, uint64_t offset, uint64_t size)
  : impl_(ViewImpl::map(flags, file, offset, size)) {}

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

ViewFlags View::flags() const {
  return impl_ ? impl_->flags() : ViewFlags::none();
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
