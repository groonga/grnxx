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
#include "map.hpp"

#include "charset.hpp"
#include "exception.hpp"
#include "logger.hpp"
#include "map/double_array.hpp"

namespace grnxx {

MapOptions::MapOptions() : type(MAP_UNKNOWN) {}

MapHeader::MapHeader() : type(MAP_UNKNOWN) {}

MapCursor::MapCursor() : key_id_(-1), key_() {}
MapCursor::~MapCursor() {}

MapScan::~MapScan() {}

MapScan *MapScan::open(Map *map, const Slice &query, const Charset *charset) {
  std::unique_ptr<MapScan> scan(new (std::nothrow) MapScan);
  if (!scan) {
    GRNXX_ERROR() << "new grnxx::MapScan failed";
    GRNXX_THROW();
  }
  scan->map_ = map;
  scan->query_ = query;
  scan->charset_ = charset;
  return scan.release();
}

bool MapScan::next() {
  offset_ += size_;
  while (offset_ < query_.size()) {
    const Slice query_left = query_.subslice(offset_, query_.size() - offset_);
    if (map_->lcp_search(query_left, &key_id_, &key_)) {
      size_ = key_.size();
      return true;
    }
    // Move to the next character.
    if (charset_) {
      offset_ += charset_->get_char(query_left).size();
    } else {
      ++offset_;
    }
  }
  size_ = 0;
  return false;
}

MapScan::MapScan()
  : map_(nullptr),
    query_(),
    offset_(0),
    size_(0),
    key_id_(-1),
    key_() {}

Map::Map() {}
Map::~Map() {}

Map *Map::create(const MapOptions &options, io::Pool pool) {
  switch (options.type) {
    case MAP_UNKNOWN: {
      break;
    }
    case MAP_DOUBLE_ARRAY: {
      return map::DoubleArray::create(options, pool);
    }
    // TODO: Other map types will be supported in future.
//    case ???: {
//      return map::???::create(options, pool);
//    }
  }

  // TODO: Unknown type error!
  return nullptr;
}

Map *Map::open(io::Pool pool, uint32_t block_id) {
  // Get the address to the header.
  auto block_info = pool.get_block_info(block_id);
  auto block_address = pool.get_block_address(*block_info);
  auto header = static_cast<MapHeader *>(block_address);

  // Check the type.
  MapType type = header->type;

  // Call the appropriate function.
  switch (type) {
    case MAP_UNKNOWN: {
      break;
    }
    case MAP_DOUBLE_ARRAY: {
      return map::DoubleArray::open(pool, block_id);
    }
    // TODO: Other map types will be supported in future.
//    case ???: {
//      return map::???::open(pool, block_id);
//    }
  }

  // TODO: Unknown type error!
  return nullptr;
}

void Map::unlink(io::Pool pool, uint32_t block_id) {
  // Get the address to the header.
  auto block_info = pool.get_block_info(block_id);
  auto block_address = pool.get_block_address(*block_info);
  auto header = static_cast<MapHeader *>(block_address);

  // Check the type.
  MapType type = header->type;

  // Call the appropriate function.
  switch (type) {
    case MAP_UNKNOWN: {
      break;
    }
    case MAP_DOUBLE_ARRAY: {
      map::DoubleArray::unlink(pool, block_id);
      return;
    }
    // TODO: Other map types will be supported in future.
//    case ???: {
//      return map::???::unlink(pool, block_id);
//    }
  }

  // TODO: Unknown type error!
}

MapScan *Map::open_scan(const Slice &query, const Charset *charset) {
  return MapScan::open(this, query, charset);
}

}  // namespace grnxx
