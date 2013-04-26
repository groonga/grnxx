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
#include "grnxx/alpha/map.hpp"

#include "grnxx/alpha/map/array.hpp"
#include "grnxx/alpha/map/cursor.hpp"
#include "grnxx/alpha/map/double_array.hpp"
#include "grnxx/alpha/map/header.hpp"
#include "grnxx/charset.hpp"
#include "grnxx/exception.hpp"
#include "grnxx/slice.hpp"
#include "grnxx/logger.hpp"

namespace grnxx {
namespace alpha {

template <typename T>
MapCursor<T>::MapCursor() : key_id_(-1), key_() {}

template <typename T>
MapCursor<T>::~MapCursor() {}

template <typename T>
bool MapCursor<T>::next() {
  // TODO: Not supported!?
  return false;
}

template <typename T>
bool MapCursor<T>::remove() {
  // TODO: Not supported.
  return false;
}

template class MapCursor<int8_t>;
template class MapCursor<int16_t>;
template class MapCursor<int32_t>;
template class MapCursor<int64_t>;
template class MapCursor<uint8_t>;
template class MapCursor<uint16_t>;
template class MapCursor<uint32_t>;
template class MapCursor<uint64_t>;
template class MapCursor<double>;
template class MapCursor<GeoPoint>;
template class MapCursor<Slice>;

template <typename T>
Map<T>::Map() {}

template <typename T>
Map<T>::~Map() {}

template <typename T>
Map<T> *Map<T>::create(MapType type, io::Pool pool,
                       const MapOptions &options) {
  switch (type) {
    case MAP_ARRAY: {
      return map::Array<T>::create(pool, options);
    }
    case MAP_DOUBLE_ARRAY: {
      return map::DoubleArray<T>::create(pool, options);
    }
    default: {
      // Not supported yet.
      return nullptr;
    }
  }
}

template <typename T>
Map<T> *Map<T>::open(io::Pool pool, uint32_t block_id) {
  const map::Header *header = static_cast<const map::Header *>(
      pool.get_block_address(block_id));
  switch (header->type) {
    case MAP_ARRAY: {
      return map::Array<T>::open(pool, block_id);
    }
    case MAP_DOUBLE_ARRAY: {
      return map::DoubleArray<T>::open(pool, block_id);
    }
    default: {
      // Not supported yet.
      return nullptr;
    }
  }
}

template <typename T>
bool Map<T>::unlink(io::Pool pool, uint32_t block_id) {
  const map::Header *header = static_cast<const map::Header *>(
      pool.get_block_address(block_id));
  switch (header->type) {
    case MAP_ARRAY: {
      return map::Array<T>::unlink(pool, block_id);
    }
    default: {
      // Not supported yet.
      return false;
    }
  }
}

template <typename T>
uint32_t Map<T>::block_id() const {
  // Not supported.
  return 0;
}

template <typename T>
MapType Map<T>::type() const {
  // Not supported.
  return MAP_UNKNOWN;
}

template <typename T>
int64_t Map<T>::max_key_id() const {
  // Not supported.
  return -1;
}

template <typename T>
int64_t Map<T>::next_key_id() const {
  // Not supported.
  return -1;
}

template <typename T>
uint64_t Map<T>::num_keys() const {
  // Not supported.
  return 0;
}

template <typename T>
bool Map<T>::get(int64_t, T *) {
  // Not supported.
  return false;
}

template <typename T>
bool Map<T>::get_next(int64_t, int64_t *, T *) {
  return false;
}

template <typename T>
bool Map<T>::unset(int64_t) {
  // Not supported.
  return false;
}

template <typename T>
bool Map<T>::reset(int64_t, T) {
  // Not supported.
  return false;
}

template <typename T>
bool Map<T>::find(T, int64_t *) {
  // Not supported.
  return false;
}

template <typename T>
bool Map<T>::insert(T, int64_t *) {
  // Not supported.
  return false;
}

template <typename T>
bool Map<T>::remove(T) {
  // Not supported.
  return false;
}

template <typename T>
bool Map<T>::update(T, T, int64_t *) {
  // Not supported.
  return false;
}

template <typename T>
bool Map<T>::find_longest_prefix_match(T, int64_t *, T *) {
  // Not supported.
  return false;
}

template <>
bool Map<Slice>::find_longest_prefix_match(Slice query, int64_t *key_id,
                                           Slice *key) {
  // Naive implementation.
  for (size_t size = query.size(); size > 0; --size) {
    const Slice prefix = query.prefix(size);
    if (find(prefix, key_id)) {
      if (key) {
        *key = prefix;
      }
      return true;
    }
  }
  return false;
}

template <typename T>
void Map<T>::truncate() {
  // Not supported.
}

template <typename T>
MapCursor<T> *Map<T>::open_basic_cursor(const MapCursorOptions &options) {
  return new (std::nothrow) map::IDCursor<T>(this, -1, -1, options);
}

template <typename T>
MapCursor<T> *Map<T>::open_id_cursor(int64_t min, int64_t max,
                                     const MapCursorOptions &options) {
  return new (std::nothrow) map::IDCursor<T>(this, min, max, options);
}

template <typename T>
MapCursor<T> *Map<T>::open_key_cursor(T min, T max,
                                      const MapCursorOptions &options) {
  return new (std::nothrow) map::KeyCursor<T>(this, min, max, options);
}

template <>
MapCursor<GeoPoint> *Map<GeoPoint>::open_key_cursor(GeoPoint, GeoPoint,
                                                    const MapCursorOptions &) {
  // Not supported.
  return nullptr;
}

template <typename T>
MapCursor<T> *Map<T>::open_bitwise_completion_cursor(
    T, size_t, const MapCursorOptions &) {
  // Not supported.
  return nullptr;
}

template <>
MapCursor<GeoPoint> *Map<GeoPoint>::open_bitwise_completion_cursor(
    GeoPoint query, size_t bit_size, const MapCursorOptions &options) {
  return new (std::nothrow) map::BitwiseCompletionCursor(
      this, query, bit_size, options);
}

template <typename T>
MapCursor<T> *Map<T>::open_prefix_cursor(
    T, size_t, const MapCursorOptions &) {
  // Not supported.
  return nullptr;
}

template <>
MapCursor<Slice> *Map<Slice>::open_prefix_cursor(
    Slice query, size_t min_size, const MapCursorOptions &options) {
  return new (std::nothrow) map::PrefixCursor(this, query, min_size, options);
}

template <typename T>
MapCursor<T> *Map<T>::open_completion_cursor(
    T, const MapCursorOptions &) {
  // Not supported.
  return nullptr;
}

template <>
MapCursor<Slice> *Map<Slice>::open_completion_cursor(
    Slice query, const MapCursorOptions &options) {
  return new (std::nothrow) map::CompletionCursor(this, query, options);
}

template <typename T>
MapCursor<T> *Map<T>::open_reverse_completion_cursor(
    T, const MapCursorOptions &) {
  // Not supported.
  return nullptr;
}

template <>
MapCursor<Slice> *Map<Slice>::open_reverse_completion_cursor(
    Slice query, const MapCursorOptions &options) {
  return new (std::nothrow) map::ReverseCompletionCursor(this, query, options);
}

template <typename T>
MapScan *Map<T>::open_scan(T, const Charset *) {
  // Not supported
  return nullptr;
}

template <>
MapScan *Map<Slice>::open_scan(Slice query, const Charset *charset) {
  std::unique_ptr<MapScan> scan(
      new (std::nothrow) MapScan(this, query, charset));
  if (!scan) {
    GRNXX_ERROR() << "new grnxx::MapScan failed";
    GRNXX_THROW();
  }
  return scan.release();
}

template class Map<int8_t>;
template class Map<int16_t>;
template class Map<int32_t>;
template class Map<int64_t>;
template class Map<uint8_t>;
template class Map<uint16_t>;
template class Map<uint32_t>;
template class Map<uint64_t>;
template class Map<double>;
template class Map<GeoPoint>;
template class Map<Slice>;

MapScan::~MapScan() {}

bool MapScan::next() {
  offset_ += size_;
  while (offset_ < query_.size()) {
    const Slice query_left = query_.subslice(offset_, query_.size() - offset_);
    if (map_->find_longest_prefix_match(query_left, &key_id_, &key_)) {
      size_ = key_.size();
      return true;
    }
    // Move to the next character.
    if (charset_) {
      offset_ += charset_->get_char_size(query_left);
    } else {
      ++offset_;
    }
  }
  size_ = 0;
  return false;
}

MapScan::MapScan(Map<Slice> *map, const Slice &query, const Charset *charset)
  : map_(map),
    query_(query),
    offset_(0),
    size_(0),
    key_id_(-1),
    key_(),
    charset_(charset) {}

}  // namespace alpha
}  // namespace grnxx
