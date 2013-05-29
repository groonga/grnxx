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
#include "grnxx/map/cursor_impl.hpp"

#include <memory>
#include <new>

#include "grnxx/bytes.hpp"
#include "grnxx/geo_point.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/map.hpp"

namespace grnxx {
namespace map {

template <typename T>
AllKeysCursor<T>::AllKeysCursor()
    : MapCursor<T>(), map_(), cur_(), end_(), step_(), count_(0), options_() {}

template <typename T>
AllKeysCursor<T>::~AllKeysCursor() {}

template <typename T>
AllKeysCursor<T> *AllKeysCursor<T>::create(
    Map<T> *map, const MapCursorOptions &options) {
  std::unique_ptr<AllKeysCursor<T>> cursor(
      new (std::nothrow) AllKeysCursor<T>);
  if (!cursor) {
    GRNXX_ERROR() << "new grnxx::map::AllKeysCursor<T> failed";
    return nullptr;
  }
  if (!cursor->init(map, options)) {
    return nullptr;
  }
  return cursor.release();
}

template <typename T>
bool AllKeysCursor<T>::next() {
  if (count_ >= options_.limit) {
    return false;
  }
  while (cur_ != end_) {
    cur_ += step_;
    if (map_->get(cur_, &this->key_)) {
      this->key_id_ = cur_;
      ++count_;
      return true;
    }
  }
  return false;
}

template <typename T>
bool AllKeysCursor<T>::remove() {
  return map_->unset(this->key_id_);
}

template <typename T>
bool AllKeysCursor<T>::init(Map<T> *map, const MapCursorOptions &options) {
  map_ = map;
  options_ = options;
  options_.flags = MAP_CURSOR_ORDER_BY_ID;
  if (options.flags & MAP_CURSOR_REVERSE_ORDER) {
    options_.flags |= MAP_CURSOR_REVERSE_ORDER;
  }

  const int64_t min = map->min_key_id();
  const int64_t max = map->max_key_id();
  if (min > max) {
    // There are no keys in the range [min, max].
    cur_ = end_ = 0;
    return true;
  }

  if (options_.flags & MAP_CURSOR_REVERSE_ORDER) {
    cur_ = max + 1;
    end_ = min;
    step_ = -1;
  } else {
    cur_ = min - 1;
    end_ = max;
    step_ = 1;
  }

  // Skip the first "options_.offset" keys in range.
  for (uint64_t count = 0; (count < options_.offset) && (cur_ != end_); ) {
    cur_ += step_;
    if (map_->get(cur_)) {
      ++count;
    }
  }
  return true;
}

template <typename T>
KeyIDRangeCursor<T>::KeyIDRangeCursor()
    : MapCursor<T>(), map_(), cur_(), end_(), step_(), count_(0),
      query_(), options_() {}

template <typename T>
KeyIDRangeCursor<T>::~KeyIDRangeCursor() {}

template <typename T>
KeyIDRangeCursor<T> *KeyIDRangeCursor<T>::create(
    Map<T> *map,
    const MapCursorKeyIDRange<T> &query,
    const MapCursorOptions &options) {
  std::unique_ptr<KeyIDRangeCursor<T>> cursor(
      new (std::nothrow) KeyIDRangeCursor<T>);
  if (!cursor) {
    GRNXX_ERROR() << "new grnxx::map::KeyIDRangeCursor<T> failed";
    return nullptr;
  }
  if (!cursor->init(map, query, options)) {
    return nullptr;
  }
  return cursor.release();
}

template <typename T>
bool KeyIDRangeCursor<T>::next() {
  if (count_ >= options_.limit) {
    return false;
  }
  while (cur_ != end_) {
    cur_ += step_;
    if (map_->get(cur_, &this->key_)) {
      this->key_id_ = cur_;
      ++count_;
      return true;
    }
  }
  return false;
}

template <typename T>
bool KeyIDRangeCursor<T>::remove() {
  return map_->unset(this->key_id_);
}

template <typename T>
bool KeyIDRangeCursor<T>::init(Map<T> *map,
                               const MapCursorKeyIDRange<T> &query,
                               const MapCursorOptions &options) {
  map_ = map;
  query_ = query;
  options_ = options;
  options_.flags = MAP_CURSOR_ORDER_BY_ID;
  if (options.flags & MAP_CURSOR_REVERSE_ORDER) {
    options_.flags |= MAP_CURSOR_REVERSE_ORDER;
  }

  int64_t min;
  if (query.flags & MAP_CURSOR_KEY_ID_GREATER) {
    min = query_.min + 1;
  } else if (query.flags & MAP_CURSOR_KEY_ID_GREATER_EQUAL) {
    min = query_.min;
  } else {
    min = map->min_key_id();
  }

  int64_t max;
  if (query.flags & MAP_CURSOR_KEY_ID_LESS) {
    max = query_.max + 1;
  } else if (query.flags & MAP_CURSOR_KEY_ID_LESS_EQUAL) {
    max = query_.max;
  } else {
    max = map->max_key_id();
  }

  if (min > max) {
    // There are no keys in the range [min, max].
    cur_ = end_ = 0;
    return true;
  }

  if (options_.flags & MAP_CURSOR_REVERSE_ORDER) {
    cur_ = max + 1;
    end_ = min;
    step_ = -1;
  } else {
    cur_ = min - 1;
    end_ = max;
    step_ = 1;
  }

  // Skip the first "options_.offset" keys in range.
  for (uint64_t count = 0; (count < options_.offset) && (cur_ != end_); ) {
    cur_ += step_;
    if (map_->get(cur_)) {
      ++count;
    }
  }
  return true;
}

template <typename T>
KeyFilterCursor<T>::KeyFilterCursor()
    : MapCursor<T>(), map_(), cur_(), end_(), step_(), count_(0), options_() {}

template <typename T>
KeyFilterCursor<T>::~KeyFilterCursor() {}

template <typename T>
bool KeyFilterCursor<T>::next() {
  if (count_ >= options_.limit) {
    return false;
  }
  while (cur_ != end_) {
    cur_ += step_;
    if (map_->get(cur_, &this->key_)) {
      if (filter(this->key_)) {
        this->key_id_ = cur_;
        ++count_;
        return true;
      }
    }
  }
  return false;
}

template <typename T>
bool KeyFilterCursor<T>::remove() {
  return map_->unset(this->key_id_);
}

template <typename T>
bool KeyFilterCursor<T>::init(Map<T> *map, const MapCursorOptions &options) {
  map_ = map;
  options_ = options;
  options_.flags = MAP_CURSOR_ORDER_BY_ID;
  if (options.flags & MAP_CURSOR_REVERSE_ORDER) {
    options_.flags |= MAP_CURSOR_REVERSE_ORDER;
  }

  if (options_.flags & MAP_CURSOR_REVERSE_ORDER) {
    cur_ = map_->max_key_id() + 1;
    end_ = 0;
    step_ = -1;
  } else {
    cur_ = -1;
    end_ = map_->max_key_id();
    step_ = 1;
  }

  // Skip the first "options_.offset" keys in range.
  for (uint64_t count = 0; (count < options_.offset) && (cur_ != end_); ) {
    cur_ += step_;
    if (map_->get(cur_, &this->key_)) {
      if (filter(this->key_)) {
        ++count;
      }
    }
  }
  return true;
}

template <typename T>
KeyRangeCursor<T>::KeyRangeCursor() : KeyFilterCursor<T>(), query_() {}

template <typename T>
KeyRangeCursor<T>::~KeyRangeCursor() {}

template <typename T>
KeyRangeCursor<T> *KeyRangeCursor<T>::create(
    Map<T> *map,
    const MapCursorKeyRange<T> &query,
    const MapCursorOptions &options) {
  std::unique_ptr<KeyRangeCursor<T>> cursor(
      new (std::nothrow) KeyRangeCursor<T>);
  if (!cursor) {
    GRNXX_ERROR() << "new grnxx::map::KeyRangeCursor<T> failed";
    return nullptr;
  }
  cursor->query_ = query;
  if (!cursor->init(map, options)) {
    return nullptr;
  }
  return cursor.release();
}

template <typename T>
bool KeyRangeCursor<T>::filter(KeyArg key) const {
  if (query_.flags & MAP_CURSOR_KEY_LESS) {
    if (key >= query_.min) {
      return false;
    }
  } else if (query_.flags & MAP_CURSOR_KEY_LESS_EQUAL) {
    if (key > query_.min) {
      return false;
    }
  }
  if (query_.flags & MAP_CURSOR_KEY_GREATER) {
    if (key <= query_.min) {
      return false;
    }
  } else if (query_.flags & MAP_CURSOR_KEY_GREATER_EQUAL) {
    if (key < query_.min) {
      return false;
    }
  }
  return true;
}

template class AllKeysCursor<int8_t>;
template class AllKeysCursor<int16_t>;
template class AllKeysCursor<int32_t>;
template class AllKeysCursor<int64_t>;
template class AllKeysCursor<uint8_t>;
template class AllKeysCursor<uint16_t>;
template class AllKeysCursor<uint32_t>;
template class AllKeysCursor<uint64_t>;
template class AllKeysCursor<double>;
template class AllKeysCursor<GeoPoint>;
template class AllKeysCursor<Bytes>;

template class KeyIDRangeCursor<int8_t>;
template class KeyIDRangeCursor<int16_t>;
template class KeyIDRangeCursor<int32_t>;
template class KeyIDRangeCursor<int64_t>;
template class KeyIDRangeCursor<uint8_t>;
template class KeyIDRangeCursor<uint16_t>;
template class KeyIDRangeCursor<uint32_t>;
template class KeyIDRangeCursor<uint64_t>;
template class KeyIDRangeCursor<double>;
template class KeyIDRangeCursor<GeoPoint>;
template class KeyIDRangeCursor<Bytes>;

template class KeyRangeCursor<int8_t>;
template class KeyRangeCursor<int16_t>;
template class KeyRangeCursor<int32_t>;
template class KeyRangeCursor<int64_t>;
template class KeyRangeCursor<uint8_t>;
template class KeyRangeCursor<uint16_t>;
template class KeyRangeCursor<uint32_t>;
template class KeyRangeCursor<uint64_t>;
template class KeyRangeCursor<double>;
// GeoPoint does not support comparison operators (<, <=, >, >=).
//template class KeyRangeCursor<GeoPoint>;
template class KeyRangeCursor<Bytes>;

}  // namespace map
}  // namespace grnxx
