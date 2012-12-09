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
#ifndef GRNXX_IO_VIEW_HPP
#define GRNXX_IO_VIEW_HPP

#include "file.hpp"

namespace grnxx {
namespace io {

class ViewFlagsIdentifier {};
typedef FlagsImpl<ViewFlagsIdentifier> ViewFlags;

// VIEW_WRITE_ONLY is ignored if VIEW_READ_ONLY is enabled.
// VIEW_READ_ONLY is disabled if VIEW_CREATE is specified.
// If both flags are not set, a memory mapping is created in read-write mode.

// Read-only mode.
constexpr ViewFlags VIEW_READ_ONLY  = ViewFlags::define(0x0001);
// Write-only mode.
constexpr ViewFlags VIEW_WRITE_ONLY = ViewFlags::define(0x0002);

// VIEW_ANONYMOUS disables all the flags other than GRNXX_HUGE_TLB and
// enables VIEW_PRIVATE.
// VIEW_CREATE disables VIEW_READ_ONLY.

// Anonymous mode.
constexpr ViewFlags VIEW_ANONYMOUS  = ViewFlags::define(0x0010);
// Try to use huge pages.
constexpr ViewFlags VIEW_HUGE_TLB   = ViewFlags::define(0x0080);

// VIEW_PRIVATE is ignored if VIEW_SHARED is enabled.

// Private mode.
constexpr ViewFlags VIEW_PRIVATE    = ViewFlags::define(0x1000);
// Shared mode.
constexpr ViewFlags VIEW_SHARED     = ViewFlags::define(0x2000);

StringBuilder &operator<<(StringBuilder &builder, ViewFlags flags);
std::ostream &operator<<(std::ostream &builder, ViewFlags flags);

class ViewImpl;

class View {
 public:
  View();
  // Create an anonymous memory mapping.
  // Available flags are VIEW_HUGE_TLB only.
  explicit View(ViewFlags flags, uint64_t size);
  // Create a file-backed memory mapping.
  // Available flags are as follows:
  //  VIEW_READ_ONLY, VIEW_WRITE_ONLY, VIEW_SHARED, VIEW_PRIVATE.
  View(ViewFlags flags, const File &file);
  View(ViewFlags flags, const File &file, uint64_t offset, uint64_t size);
  ~View();

  View(const View &view);
  View &operator=(const View &view);

  View(View &&view);
  View &operator=(View &&view);

  explicit operator bool() const {
    return static_cast<bool>(impl_);
  }

  void open(ViewFlags flags, uint64_t size) {
    *this = View(flags, size);
  }
  void open(ViewFlags flags, const File &file) {
    *this = View(flags, file);
  }
  void open(ViewFlags flags, const File &file,
            uint64_t offset, uint64_t size) {
    *this = View(flags, file, offset, size);
  }
  void close() {
    *this = View();
  }

  void sync();
  void sync(uint64_t offset, uint64_t size);

  File file() const;
  ViewFlags flags() const;
  void *address() const;
  uint64_t offset() const;
  uint64_t size() const;

  void swap(View &view);

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  std::shared_ptr<ViewImpl> impl_;
};

inline void swap(View &lhs, View &rhs) {
  lhs.swap(rhs);
}

inline StringBuilder &operator<<(StringBuilder &builder, const View &view) {
  return view.write_to(builder);
}

}  // namespace io
}  // namespace grnxx

#endif  // GRNXX_IO_VIEW_HPP
