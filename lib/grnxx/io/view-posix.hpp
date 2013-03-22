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
#ifndef GRNXX_IO_VIEW_POSIX_HPP
#define GRNXX_IO_VIEW_POSIX_HPP

#include "grnxx/io/view.hpp"

#ifndef GRNXX_WINDOWS

namespace grnxx {
namespace io {

class ViewImpl : public View {
 public:
  ~ViewImpl();

  static ViewImpl *open(ViewFlags flags, uint64_t size);
  static ViewImpl *open(ViewFlags flags, File *file);
  static ViewImpl *open(ViewFlags flags, File *file,
                        uint64_t offset, uint64_t size);

  void sync();
  void sync(uint64_t offset, uint64_t size);

  ViewFlags flags() const {
    return flags_;
  }
  void *address() const {
    return address_;
  }
  uint64_t size() const {
    return size_;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  ViewFlags flags_;
  void *address_;
  uint64_t size_;

  ViewImpl();

  void open_view(ViewFlags flags, uint64_t size);
  void open_view(ViewFlags flags, File *file,
                 uint64_t offset, uint64_t size);
};

}  // namespace io
}  // namespace grnxx

#endif  // GRNXX_WINDOWS

#endif  // GRNXX_IO_VIEW_POSIX_HPP
