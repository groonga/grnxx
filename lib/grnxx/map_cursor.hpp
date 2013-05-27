/*
  Copyright (C) 2013  Brazil, Inc.

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
#ifndef GRNXX_MAP_CURSOR_HPP
#define GRNXX_MAP_CURSOR_HPP

#include "grnxx/features.hpp"

#include "grnxx/flags_impl.hpp"
#include "grnxx/traits.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class StringBuilder;

template <typename T> class Map;

// TODO: How to implement NEAR cursor.
struct MapCursorFlagsIdentifier;
using MapCursorFlags = FlagsImpl<MapCursorFlagsIdentifier>;

// Use the default settings.
constexpr MapCursorFlags MAP_CURSOR_DEFAULT       =
    MapCursorFlags::define(0x00);
// Sort keys by ID.
constexpr MapCursorFlags MAP_CURSOR_ORDER_BY_ID   =
    MapCursorFlags::define(0x01);
// Sort keys by key.
constexpr MapCursorFlags MAP_CURSOR_ORDER_BY_KEY  =
    MapCursorFlags::define(0x02);
// Access keys in reverse order.
constexpr MapCursorFlags MAP_CURSOR_REVERSE_ORDER =
    MapCursorFlags::define(0x10);

StringBuilder &operator<<(StringBuilder &builder, MapCursorFlags flags);

struct MapCursorOptions {
  MapCursorFlags flags;
  uint64_t offset;
  uint64_t limit;

  // Initialize the members.
  MapCursorOptions();
};

template <typename T>
class MapCursor {
 public:
  using Key = typename Traits<T>::Type;
  using KeyArg = typename Traits<T>::ArgumentType;

  MapCursor();
  virtual ~MapCursor();

  // Move the cursor to the next key and return true on success.
  virtual bool next() = 0;
  // Remove the current key and return true on success.
  virtual bool remove();

  // Return the ID of the current key.
  int64_t key_id() const {
    return key_id_;
  }
  // Return the current key.
  const Key &key() const {
    return key_;
  }

 protected:
  int64_t key_id_;
  Key key_;
};

}  // namespace grnxx

#endif  // GRNXX_MAP_CURSOR_HPP
