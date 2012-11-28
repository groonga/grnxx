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
#ifndef GRNXX_IO_FLAGS_HPP
#define GRNXX_IO_FLAGS_HPP

#include "../string_builder.hpp"

namespace grnxx {
namespace io {

class FlagsIdentifier {};
typedef FlagsImpl<FlagsIdentifier> Flags;

// GRNXX_IO_WRITE_ONLY is ignored if GRNXX_IO_READ_ONLY is enabled.
// GRNXX_IO_READ_ONLY is disabled if GRNXX_IO_CREATE is specified.
// If both flags are not set, and object is created/opened/mapped in
// read-write mode.

// Read-only mode.
const Flags GRNXX_IO_READ_ONLY  = Flags::define(0x0001);
// Write-only mode.
const Flags GRNXX_IO_WRITE_ONLY = Flags::define(0x0002);

// GRNXX_IO_ANONYMOUS disables all the flags other than GRNXX_HUGE_TLB and
// enables GRNXX_IO_PRIVATE.
// GRNXX_IO_APPEND is ignored if GRNXX_IO_READ_ONLY is enabled.
// GRNXX_IO_CREATE disables GRNXX_IO_READ_ONLY.
// GRNXX_IO_OPEN is enabled if GRNXX_IO_CREATE is not specified.
// If both GRNXX_IO_CREATE and GRNXX_IO_OPEN are set, it first tries to create
// an object and, if already exists, then tries to open the existing object.
// GRNXX_IO_TEMPORARY disables other flags.

// Anonymous (non-file-backed) mode.
const Flags GRNXX_IO_ANONYMOUS  = Flags::define(0x0010);
// Append mode.
const Flags GRNXX_IO_APPEND     = Flags::define(0x0020);
// Create an object if it does not exist.
const Flags GRNXX_IO_CREATE     = Flags::define(0x0040);
// Try to use huge pages.
const Flags GRNXX_IO_HUGE_TLB   = Flags::define(0x0080);
// Open an existing object.
const Flags GRNXX_IO_OPEN       = Flags::define(0x0100);
// Create a temporary object.
const Flags GRNXX_IO_TEMPORARY  = Flags::define(0x0200);
// Truncate an existing object.
const Flags GRNXX_IO_TRUNCATE   = Flags::define(0x0400);

// GRNXX_IO_PRIVATE is ignored if GRNXX_IO_SHARED is enabled.

// Private mode.
const Flags GRNXX_IO_PRIVATE    = Flags::define(0x1000);
// Shared mode.
const Flags GRNXX_IO_SHARED     = Flags::define(0x2000);

StringBuilder &operator<<(StringBuilder &builder, Flags flags);

std::ostream &operator<<(std::ostream &stream, Flags flags);

// One of these modes must be specified.
enum LockMode {
  GRNXX_IO_EXCLUSIVE_LOCK = 0x10000,  // Create an exclusive lock.
  GRNXX_IO_SHARED_LOCK    = 0x20000   // Create a shared lock.
};

}  // namespace io
}  // namespace grnxx

#endif  // GRNXX_IO_FLAGS_HPP
