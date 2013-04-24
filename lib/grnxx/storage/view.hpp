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
#ifndef GRNXX_STORAGE_VIEW_HPP
#define GRNXX_STORAGE_VIEW_HPP

#include "grnxx/basic.hpp"
#include "grnxx/flags_impl.hpp"

namespace grnxx {

class StringBuilder;

namespace storage {

class View;
using ViewFlags = FlagsImpl<View>;

// Use the default settings.
constexpr ViewFlags VIEW_DEFAULT   = ViewFlags::define(0x00);
// Create an anonymous memory mapping.
// This flag is implicitly enabled if "file" == nullptr.
constexpr ViewFlags VIEW_ANONYMOUS = ViewFlags::define(0x01);
// Use huge pages if available, or use regular pages.
constexpr ViewFlags VIEW_HUGE_TLB  = ViewFlags::define(0x02);
// Create a read-only memory mapping.
// This flag is implicitly enabled if "file" is read-only.
constexpr ViewFlags VIEW_READ_ONLY = ViewFlags::define(0x04);

StringBuilder &operator<<(StringBuilder &builder, ViewFlags flags);

class File;

class View {
 public:
  View();
  virtual ~View();

  // Create a file-backed memory mapping on "file" if "file" != nullptr, or
  // create an anonymous memory mapping.
  // The available flag is VIEW_HUGE_TLB.
  static View *create(File *file,
                      int64_t offset = 0,
                      int64_t size = -1,
                      ViewFlags flags = VIEW_DEFAULT);

  // Flush modified pages.
  virtual bool sync(int64_t offset = 0, int64_t size = -1) = 0;

  // Return the enabled flags.
  virtual ViewFlags flags() const = 0;
  // Return the starting address.
  virtual void *address() const = 0;
  // Return the size.
  virtual int64_t size() const = 0;
};

}  // namespace storage
}  // namespace grnxx

#endif  // GRNXX_STORAGE_VIEW_HPP
