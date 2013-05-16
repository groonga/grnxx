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
#ifndef GRNXX_MAP_SCANNER_HPP
#define GRNXX_MAP_SCANNER_HPP

#include "grnxx/features.hpp"

#include "grnxx/map.hpp"

namespace grnxx {

class Charset;

namespace map {

template <typename T>
class Scanner : public MapScanner<T> {
 public:
  using Key = typename MapScanner<T>::Key;
  using KeyArg = typename MapScanner<T>::KeyArg;

  Scanner();
  ~Scanner();

  static Scanner *create(Map<T> *map, KeyArg query, const Charset *charset);

  bool next();

 protected:
  Map<T> *map_;
  Key query_;
  const Charset *charset_;
};

}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_SCANNER_HPP
