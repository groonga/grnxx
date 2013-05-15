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
#ifndef GRNXX_MAP_DA_LARGE_PREFIX_CURSOR_HPP
#define GRNXX_MAP_DA_LARGE_PREFIX_CURSOR_HPP

#include "grnxx/map/da/large/trie.hpp"

#include <vector>

namespace grnxx {
namespace map {
namespace da {
namespace large {

class PrefixCursor : public MapCursor {
 public:
  ~PrefixCursor();

  static PrefixCursor *open(Trie *trie, MapCursorFlags flags,
                            size_t min, const Slice &max,
                            int64_t offset, int64_t limit);

  bool next();

 private:
  Trie *trie_;
  std::vector<Node> nodes_;
  int64_t current_;
  int64_t end_;
  int64_t step_;

  PrefixCursor();

  void open_cursor(Trie *trie, MapCursorFlags flags,
                   size_t min, const Slice &max,
                   int64_t offset, int64_t limit);
};

}  // namespace large
}  // namespace da
}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_DA_LARGE_PREFIX_CURSOR_HPP
