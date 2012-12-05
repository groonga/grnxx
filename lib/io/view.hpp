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

class ViewImpl;

class View {
 public:
  View();
  // Create an anonymou memory mapping.
  // Available flags are GRNXX_IO_HUGE_TLB only.
  explicit View(Flags flags, uint64_t size);
  // Create a file-backed memory mapping.
  // Available flags are as follows:
  //  GRNXX_IO_READ_ONLY, GRNXX_IO_WRITE_ONLY, GRNXX_IO_SHARED,
  //  GRNXX_IO_PRIVATE.
  View(const File &file, Flags flags);
  View(const File &file, Flags flags, uint64_t offset, uint64_t size);
  ~View();

  View(const View &view);
  View &operator=(const View &view);

  View(View &&view);
  View &operator=(View &&view);

  explicit operator bool() const {
    return static_cast<bool>(impl_);
  }

  void sync();
  void sync(uint64_t offset, uint64_t size);

  File file() const;
  Flags flags() const;
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
