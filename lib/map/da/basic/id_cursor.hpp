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
#ifndef GRNXX_MAP_DA_BASIC_ID_CURSOR_HPP
#define GRNXX_MAP_DA_BASIC_ID_CURSOR_HPP

#include "map/da/basic/trie.hpp"

namespace grnxx {
namespace map {
namespace da {
namespace basic {

class IDCursor : public MapCursor {
 public:
  ~IDCursor();

  static IDCursor *open(Trie *trie, MapCursorFlags flags,
                        int64_t min, int64_t max,
                        int64_t offset, int64_t limit);

  bool next();

 private:
  Trie *trie_;
  int64_t current_;
  int64_t end_;
  int64_t step_;
  int64_t limit_;

  IDCursor();

  void open_cursor(Trie *trie, MapCursorFlags flags,
                   int64_t min, int64_t max,
                   int64_t offset, int64_t limit);
};

}  // namespace basic
}  // namespace da
}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_DA_BASIC_ID_CURSOR_HPP
