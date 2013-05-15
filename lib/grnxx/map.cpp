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
#include "grnxx/map.hpp"

#include "grnxx/logger.hpp"

namespace grnxx {

template <typename T>
MapCursor<T>::MapCursor() : key_id_(-1), key_() {}

template <typename T>
MapCursor<T>::~MapCursor() {}

template <typename T>
bool MapCursor<T>::remove() {
  GRNXX_ERROR() << "invalid operation";
  return false;
}

MapCursorOptions::MapCursorOptions()
    : flags(MAP_CURSOR_DEFAULT),
      offset(0),
      limit(-1) {}

// TODO

}  // namespace grnxx
