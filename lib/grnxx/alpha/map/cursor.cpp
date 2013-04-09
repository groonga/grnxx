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
#include "grnxx/alpha/map/cursor.hpp"

namespace grnxx {
namespace alpha {
namespace map {

template <typename T>
IDCursor<T>::IDCursor(Map<T> *map, int64_t min, int64_t max,
                      const MapCursorOptions &options)
  : MapCursor<T>(), map_(map), end_(), step_(), left_(options.limit) {
  // TODO?
//  if (options.flags & MAP_CURSOR_ORDER_BY_ID) {
//  } else if (options.flags & MAP_CURSOR_ORDER_BY_KEY) {
//  }

  if (min < 0) {
    min = 0;
  } else if (options.flags & MAP_CURSOR_EXCEPT_MIN) {
    ++min;
  }

  if (max > map_->max_key_id()) {
    max = map_->max_key_id();
  } else if (options.flags & MAP_CURSOR_EXCEPT_MAX) {
    --max;
  }

  if (~options.flags & MAP_CURSOR_REVERSE_ORDER) {
    this->key_id_ = min - 1;
    end_ = max;
    step_ = 1;
  } else {
    this->key_id_ = max + 1;
    end_ = min;
    step_ = -1;
  }

  uint64_t count = 0;
  while ((count < options.offset) && (this->key_id_ != end_)) {
    this->key_id_ += step_;
    if (map_->get(this->key_id_)) {
      ++count;
    }
  }
}

template <typename T>
IDCursor<T>::~IDCursor() {}

template <typename T>
bool IDCursor<T>::next() {
  if (left_ == 0) {
    return false;
  }
  while (this->key_id_ != end_) {
    this->key_id_ += step_;
    if (map_->get(this->key_id_, &this->key_)) {
      --left_;
      return true;
    }
  }
  return false;
}

template <typename T>
bool IDCursor<T>::remove() {
  return map_->unset(this->key_id_);
}

template class IDCursor<int8_t>;
template class IDCursor<int16_t>;
template class IDCursor<int32_t>;
template class IDCursor<int64_t>;
template class IDCursor<uint8_t>;
template class IDCursor<uint16_t>;
template class IDCursor<uint32_t>;
template class IDCursor<uint64_t>;
template class IDCursor<double>;
template class IDCursor<GeoPoint>;
template class IDCursor<Slice>;

template <typename T>
KeyCursor<T>::KeyCursor(Map<T> *map, T min, T max,
                        const MapCursorOptions &options)
  : MapCursor<T>(), map_(map), min_(min), max_(max), end_(), step_(),
    left_(options.limit), flags_(options.flags) {
  // TODO?
//  if (options.flags & MAP_CURSOR_ORDER_BY_ID) {
//  } else if (options.flags & MAP_CURSOR_ORDER_BY_KEY) {
//  }

  uint64_t count = 0;
  if (~flags_ & MAP_CURSOR_REVERSE_ORDER) {
    this->key_id_ = -1;
    end_ = map_->max_key_id();
    step_ = 1;
  } else {
    this->key_id_ = map_->max_key_id() + 1;
    end_ = 0;
    step_ = -1;
  }

  while ((count < options.offset) && (this->key_id_ != end_)) {
    this->key_id_ += step_;
    if (map_->get(this->key_id_, &this->key_)) {
      if (in_range(this->key_)) {
        ++count;
      }
    }
  }
}

template <typename T>
KeyCursor<T>::~KeyCursor() {}

template <typename T>
bool KeyCursor<T>::next() {
  if (left_ == 0) {
    return false;
  }
  while (this->key_id_ != end_) {
    this->key_id_ += step_;
    if (map_->get(this->key_id_, &this->key_)) {
      if (in_range(this->key_)) {
        --left_;
        return true;
      }
    }
  }
  return false;
}

template <typename T>
bool KeyCursor<T>::remove() {
  return map_->unset(this->key_id_);
}

template <typename T>
bool KeyCursor<T>::in_range(T key) const {
  if (flags_ & MAP_CURSOR_EXCEPT_MIN) {
    if (key <= min_) {
      return false;
    }
  } else if (key < min_) {
    return false;
  }

  if (flags_ & MAP_CURSOR_EXCEPT_MAX) {
    if (key >= max_) {
      return false;
    }
  } else if (key > max_) {
    return false;
  }

  return true;
}

template class KeyCursor<int8_t>;
template class KeyCursor<int16_t>;
template class KeyCursor<int32_t>;
template class KeyCursor<int64_t>;
template class KeyCursor<uint8_t>;
template class KeyCursor<uint16_t>;
template class KeyCursor<uint32_t>;
template class KeyCursor<uint64_t>;
template class KeyCursor<double>;
template class KeyCursor<Slice>;

}  // namespace map
}  // namespace alpha
}  // namespace grnxx
