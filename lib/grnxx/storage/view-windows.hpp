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
#ifndef GRNXX_STORAGE_VIEW_WINDOWS_HPP
#define GRNXX_STORAGE_VIEW_WINDOWS_HPP

#include "grnxx/storage/view.hpp"

#ifdef GRNXX_WINDOWS

#include <windows.h>

// FILE_READ_ONLY is defined as a macro in windows.h.
#ifdef FILE_READ_ONLY
# undef FILE_READ_ONLY
#endif  // FILE_READ_ONLY

namespace grnxx {
namespace storage {

class ViewImpl : public View {
 public:
  ViewImpl();
  ~ViewImpl();

  static View *create(File *file, int64_t offset, int64_t size,
                      ViewFlags flags);

  bool sync(int64_t offset, int64_t size);

  ViewFlags flags() const;
  void *address() const;
  int64_t size() const;

 private:
  ViewFlags flags_;
  HANDLE handle_;
  void *address_;
  uint64_t size_;

  bool create_file_backed_view(File *file, int64_t offset, int64_t size,
                               ViewFlags flags);
  bool create_anonymous_view(int64_t size, ViewFlags flags);
};

}  // namespace storage
}  // namespace grnxx

#endif  // GRNXX_WINDOWS

#endif  // GRNXX_STORAGE_VIEW_WINDOWS_HPP
