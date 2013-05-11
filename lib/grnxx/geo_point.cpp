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
#include "grnxx/geo_point.hpp"

#include "grnxx/string_builder.hpp"

namespace grnxx {

uint64_t GeoPoint::interleave() const {
  uint64_t latitude = static_cast<uint32_t>(point_.latitude);
  uint64_t longitude = static_cast<uint32_t>(point_.longitude);
  latitude = (latitude | (latitude << 16)) & 0x0000FFFF0000FFFFULL;
  latitude = (latitude | (latitude <<  8)) & 0x00FF00FF00FF00FFULL;
  latitude = (latitude | (latitude <<  4)) & 0x0F0F0F0F0F0F0F0FULL;
  latitude = (latitude | (latitude <<  2)) & 0x3333333333333333ULL;
  latitude = (latitude | (latitude <<  1)) & 0x5555555555555555ULL;
  longitude = (longitude | (longitude << 16)) & 0x0000FFFF0000FFFFULL;
  longitude = (longitude | (longitude <<  8)) & 0x00FF00FF00FF00FFULL;
  longitude = (longitude | (longitude <<  4)) & 0x0F0F0F0F0F0F0F0FULL;
  longitude = (longitude | (longitude <<  2)) & 0x3333333333333333ULL;
  longitude = (longitude | (longitude <<  1)) & 0x5555555555555555ULL;
  return (latitude << 1) | longitude;
}

StringBuilder &operator<<(StringBuilder &builder, const GeoPoint &point) {
  return builder << "{ latitude = " << point.latitude()
                 << ", longitude = " << point.longitude() << " }";
}

}  // namespace
