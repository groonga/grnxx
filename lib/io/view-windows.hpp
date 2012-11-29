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
#ifndef GRNXX_IO_VIEW_WINDOWS_HPP
#define GRNXX_IO_VIEW_WINDOWS_HPP

#include "view.hpp"

#ifdef GRNXX_WINDOWS

#ifndef NOMINMAX
# define NOMINMAX
#endif  // NOMINMAX
#include <windows.h>

namespace grnxx {
namespace io {

class ViewImpl {
 public:
  ~ViewImpl();

  static std::unique_ptr<ViewImpl> map(Flags flags, uint64_t size);
  static std::unique_ptr<ViewImpl> map(const File &file, Flags flags);
  static std::unique_ptr<ViewImpl> map(const File &file, Flags flags,
                                       uint64_t offset, uint64_t size);

  void sync();
  void sync(uint64_t offset, uint64_t size);

  File file() const {
    return file_;
  }
  Flags flags() const {
    return flags_;
  }
  void *address() const {
    return address_;
  }
  uint64_t offset() const {
    return offset_;
  }
  uint64_t size() const {
    return size_;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  File file_;
  Flags flags_;
  HANDLE handle_;
  void *address_;
  uint64_t offset_;
  uint64_t size_;

  ViewImpl();

  void map_on_memory(Flags flags, uint64_t size);
  void map_on_file(const File &file, Flags flags, uint64_t offset,
                   uint64_t size);

  ViewImpl(const ViewImpl &);
  ViewImpl &operator=(const ViewImpl &);
};

inline StringBuilder &operator<<(StringBuilder &builder,
                                 const ViewImpl &view) {
  return view.write_to(builder);
}

}  // namespace io
}  // namespace grnxx

#endif  // GRNXX_WINDOWS

#endif  // GRNXX_IO_VIEW_WINDOWS_HPP
