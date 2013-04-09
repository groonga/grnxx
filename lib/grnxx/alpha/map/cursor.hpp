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
#ifndef GRNXX_ALPHA_MAP_CURSOR_HPP
#define GRNXX_ALPHA_MAP_CURSOR_HPP

#include "grnxx/alpha/map.hpp"

namespace grnxx {
namespace alpha {
namespace map {

template <typename T>
class IDCursor : public MapCursor<T> {
 public:
  explicit IDCursor(Map<T> *map, int64_t min, int64_t max,
                    const MapCursorOptions &options);
  ~IDCursor();

  bool next();
  bool remove();

 private:
  Map<T> *map_;
  int64_t end_;
  int64_t step_;
  uint64_t left_;
};

template <typename T>
class KeyCursor : public MapCursor<T> {
 public:
  explicit KeyCursor(Map<T> *map, T min, T max,
                     const MapCursorOptions &options);
  ~KeyCursor();

  bool next();
  bool remove();

 private:
  Map<T> *map_;
  T min_;
  T max_;
  int64_t end_;
  int64_t step_;
  uint64_t left_;
  MapCursorFlags flags_;

  bool in_range(T key) const;
};

}  // namespace map
}  // namespace alpha
}  // namespace grnxx

#endif  // GRNXX_ALPHA_MAP_CURSOR_HPP
