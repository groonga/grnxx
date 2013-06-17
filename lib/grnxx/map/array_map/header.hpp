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
#ifndef GRNXX_MAP_ARRAY_MAP_HEADER_HPP
#define GRNXX_MAP_ARRAY_MAP_HEADER_HPP

#include "grnxx/features.hpp"

#include "grnxx/map.hpp"
#include "grnxx/types.hpp"

namespace grnxx {
namespace map {
namespace array_map {

struct Header {
  MapType map_type;
  uint32_t keys_storage_node_id;

  Header();
};

}  // namespace array_map
}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_ARRAY_MAP_HEADER_HPP
