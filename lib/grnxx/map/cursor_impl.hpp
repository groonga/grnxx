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
#ifndef GRNXX_MAP_CURSOR_IMPL_HPP
#define GRNXX_MAP_CURSOR_IMPL_HPP

#include "grnxx/features.hpp"

#include "grnxx/map_cursor.hpp"
#include "grnxx/map_cursor_query.hpp"
#include "grnxx/traits.hpp"
#include "grnxx/types.hpp"

namespace grnxx {
namespace map {

template <typename T>
class KeyIDRangeCursor : public MapCursor<T> {
 public:
  using Key = typename Traits<T>::Type;
  using KeyArg = typename Traits<T>::ArgumentType;

  KeyIDRangeCursor();
  ~KeyIDRangeCursor();

  static KeyIDRangeCursor *create(Map<T> *map,
                                  const MapCursorKeyIDRange<T> &query,
                                  const MapCursorOptions &options);

  bool next();
  bool remove();

 private:
  Map<T> *map_;
  int64_t cur_;
  int64_t end_;
  int64_t step_;
  uint64_t count_;
  MapCursorKeyIDRange<T> query_;
  MapCursorOptions options_;

  bool init(Map<T> *map,
            const MapCursorKeyIDRange<T> &query,
            const MapCursorOptions &options);
};

template <typename T>
class KeyFilterCursor : public MapCursor<T> {
 public:
  using Key = typename Traits<T>::Type;
  using KeyArg = typename Traits<T>::ArgumentType;

  KeyFilterCursor();
  virtual ~KeyFilterCursor();

  bool next();
  bool remove();

 protected:
  Map<T> *map_;
  int64_t cur_;
  int64_t end_;
  int64_t step_;
  uint64_t count_;
  MapCursorOptions options_;

  bool init(Map<T> *map, const MapCursorOptions &options);

  // Return true if "key" satisfies the query.
  virtual bool filter(KeyArg key) const = 0;
};

template <typename T>
class KeyRangeCursor : public KeyFilterCursor<T> {
 public:
  using Key = typename Traits<T>::Type;
  using KeyArg = typename Traits<T>::ArgumentType;

  KeyRangeCursor();
  ~KeyRangeCursor();

  KeyRangeCursor *create(Map<T> *map, const MapCursorKeyRange<T> &query,
                         const MapCursorOptions &options);

 private:
  MapCursorKeyRange<T> query_;

  bool filter(KeyArg key) const;
};

}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_CURSOR_IMPL_HPP
