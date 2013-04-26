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

#include <algorithm>

namespace grnxx {
namespace alpha {
namespace map {

template <typename T>
IDCursor<T>::IDCursor(Map<T> *map, int64_t min, int64_t max,
                      const MapCursorOptions &options)
    : MapCursor<T>(), map_(map), cur_(), end_(), step_(), count_(0),
      options_(options), keys_() {
  if (min < 0) {
    min = 0;
  } else if (options_.flags & MAP_CURSOR_EXCEPT_MIN) {
    ++min;
  }
  if ((max < 0) || (max > map_->max_key_id())) {
    max = map_->max_key_id();
  } else if (options_.flags & MAP_CURSOR_EXCEPT_MAX) {
    --max;
  }
  if (min > max) {
    cur_ = end_ = 0;
    return;
  }
  if ((options_.flags & MAP_CURSOR_ORDER_BY_ID) ||
      (~options_.flags & MAP_CURSOR_ORDER_BY_KEY)) {
    init_order_by_id(min, max);
  } else {
    init_order_by_key(min, max);
  }
}

template <typename T>
IDCursor<T>::~IDCursor() {}

template <typename T>
bool IDCursor<T>::next() {
  if (count_ >= options_.limit) {
    return false;
  }
  if (options_.flags & MAP_CURSOR_ORDER_BY_ID) {
    while (cur_ != end_) {
      cur_ += step_;
      if (map_->get(cur_, &this->key_)) {
        this->key_id_ = cur_;
        ++count_;
        return true;
      }
    }
  } else if (cur_ != end_) {
    cur_ += step_;
    this->key_ = keys_[cur_].first;
    this->key_id_ = keys_[cur_].second;
    ++count_;
    return true;
  }
  return false;
}

template <typename T>
bool IDCursor<T>::remove() {
  return map_->unset(this->key_id_);
}

template <typename T>
void IDCursor<T>::init_order_by_id(int64_t min, int64_t max) {
  options_.flags |= MAP_CURSOR_ORDER_BY_ID;
  options_.flags &= ~MAP_CURSOR_ORDER_BY_KEY;
  if (~options_.flags & MAP_CURSOR_REVERSE_ORDER) {
    cur_ = min - 1;
    end_ = max;
    step_ = 1;
  } else {
    cur_ = max + 1;
    end_ = min;
    step_ = -1;
  }
  uint64_t count = 0;
  while ((count < options_.offset) && (cur_ != end_)) {
    cur_ += step_;
    if (map_->get(cur_)) {
      ++count;
    }
  }
}

template <typename T>
void IDCursor<T>::init_order_by_key(int64_t min, int64_t max) {
  cur_ = min - 1;
  end_ = max;
  while (cur_ != end_) {
    ++cur_;
    T key;
    if (map_->get(cur_, &key)) {
      keys_.push_back(std::make_pair(key, cur_));
    }
  }
  std::sort(keys_.begin(), keys_.end());

  if (~options_.flags & MAP_CURSOR_REVERSE_ORDER) {
    cur_ = -1;
    end_ = keys_.size() - 1;
    step_ = 1;
  } else {
    cur_ = keys_.size();
    end_ = 0;
    step_ = -1;
  }
}

template <>
void IDCursor<GeoPoint>::init_order_by_key(int64_t min, int64_t max) {
  // Ignore MAP_CURSOR_ORDER_BY_KEY.
  init_order_by_id(min, max);
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
ConditionalCursor<T>::ConditionalCursor(Map<T> *map,
                                        const MapCursorOptions &options)
    : MapCursor<T>(), map_(map), cur_(), end_(), step_(), count_(0),
      options_(options), keys_() {}

template <typename T>
ConditionalCursor<T>::~ConditionalCursor() {}

template <typename T>
bool ConditionalCursor<T>::next() {
  if (count_ >= options_.limit) {
    return false;
  }
  if (options_.flags & MAP_CURSOR_ORDER_BY_ID) {
    while (cur_ != end_) {
      cur_ += step_;
      if (map_->get(cur_, &this->key_)) {
        if (is_valid(this->key_)) {
          this->key_id_ = cur_;
          ++count_;
          return true;
        }
      }
    }
  } else if (cur_ != end_) {
    cur_ += step_;
    this->key_ = keys_[cur_].first;
    this->key_id_ = keys_[cur_].second;
    ++count_;
    return true;
  }
  return false;
}

template <typename T>
bool ConditionalCursor<T>::remove() {
  return map_->unset(this->key_id_);
}

template <typename T>
void ConditionalCursor<T>::init() {
  if ((options_.flags & MAP_CURSOR_ORDER_BY_ID) ||
      (~options_.flags & MAP_CURSOR_ORDER_BY_KEY)) {
    init_order_by_id();
  } else {
    init_order_by_key();
  }
}

template <>
void ConditionalCursor<GeoPoint>::init() {
  init_order_by_id();
}

template <typename T>
void ConditionalCursor<T>::init_order_by_id() {
  options_.flags |= MAP_CURSOR_ORDER_BY_ID;
  options_.flags &= ~MAP_CURSOR_ORDER_BY_KEY;
  if (~options_.flags & MAP_CURSOR_REVERSE_ORDER) {
    cur_ = -1;
    end_ = map_->max_key_id();
    step_ = 1;
  } else {
    cur_ = map_->max_key_id() + 1;
    end_ = 0;
    step_ = -1;
  }
  uint64_t count = 0;
  while ((count < options_.offset) && (cur_ != end_)) {
    cur_ += step_;
    if (map_->get(cur_, &this->key_)) {
      if (is_valid(this->key_)) {
        ++count;
      }
    }
  }
}

template <typename T>
void ConditionalCursor<T>::init_order_by_key() {
  const std::int64_t max_key_id = map_->max_key_id();
  for (std::int64_t i = 0; i <= max_key_id; ++i) {
    T key;
    if (map_->get(i, &key)) {
      if (is_valid(key)) {
        keys_.push_back(std::make_pair(key, i));
      }
    }
  }
  std::sort(keys_.begin(), keys_.end());
  if (~options_.flags & MAP_CURSOR_REVERSE_ORDER) {
    cur_ = -1;
    end_ = keys_.size() - 1;
    step_ = 1;
  } else {
    cur_ = keys_.size();
    end_ = 0;
    step_ = -1;
  }
}

template <>
void ConditionalCursor<GeoPoint>::init_order_by_key() {
  // Ignore MAP_CURSOR_ORDER_BY_KEY.
  init_order_by_id();
}

template class ConditionalCursor<int8_t>;
template class ConditionalCursor<int16_t>;
template class ConditionalCursor<int32_t>;
template class ConditionalCursor<int64_t>;
template class ConditionalCursor<uint8_t>;
template class ConditionalCursor<uint16_t>;
template class ConditionalCursor<uint32_t>;
template class ConditionalCursor<uint64_t>;
template class ConditionalCursor<double>;
template class ConditionalCursor<Slice>;

template <typename T>
KeyCursor<T>::KeyCursor(Map<T> *map, T min, T max,
                        const MapCursorOptions &options)
    : ConditionalCursor<T>(map, options), min_(min), max_(max) {
  this->init();
}

template <typename T>
KeyCursor<T>::~KeyCursor() {}

template <typename T>
bool KeyCursor<T>::is_valid(T key) const {
  if (this->options_.flags & MAP_CURSOR_EXCEPT_MIN) {
    if (key <= min_) {
      return false;
    }
  } else if (key < min_) {
    return false;
  }
  if (this->options_.flags & MAP_CURSOR_EXCEPT_MAX) {
    if (key >= max_) {
      return false;
    }
  } else if (key > max_) {
    return false;
  }
  return true;
}

template <>
bool KeyCursor<Slice>::is_valid(Slice key) const {
  if (this->options_.flags & MAP_CURSOR_EXCEPT_MIN) {
    if (key <= min_) {
      return false;
    }
  } else if (key < min_) {
    return false;
  }
  if (max_) {
    if (this->options_.flags & MAP_CURSOR_EXCEPT_MAX) {
      if (key >= max_) {
        return false;
      }
    } else if (key > max_) {
      return false;
    }
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

BitwiseCompletionCursor::BitwiseCompletionCursor(
    Map<GeoPoint> *map, GeoPoint query, size_t bit_size,
    const MapCursorOptions &options)
    : ConditionalCursor<GeoPoint>(map, options),
      query_(query),
      mask_() {
  if (bit_size >= 64) {
    bit_size = 64;
  }
  switch (bit_size) {
    case 0: {
      mask_ = 0;
      break;
    }
    case 1: {
      mask_ = GeoPoint(1 << 31, 0).value();
      break;
    }
    default: {
      mask_ = GeoPoint(0xFFFFFFFFU << (32 - (bit_size / 2) - (bit_size % 2)),
                       0xFFFFFFFFU << (32 - (bit_size / 2))).value();
      break;
    }
  }
  this->init();
}

BitwiseCompletionCursor::~BitwiseCompletionCursor() {}

bool BitwiseCompletionCursor::is_valid(GeoPoint key) const {
  return ((key.value() ^ query_.value()) & mask_) == 0;
}

PrefixCursor::PrefixCursor(Map<Slice> *map, Slice query, size_t min_size,
                           const MapCursorOptions &options)
    : ConditionalCursor<Slice>(map, options),
      query_(query), min_size_(min_size) {
  if (this->options_.flags & MAP_CURSOR_EXCEPT_QUERY) {
    query_.remove_suffix(1);
  }
  this->init();
}

PrefixCursor::~PrefixCursor() {}

bool PrefixCursor::is_valid(Slice key) const {
  if (key.size() < min_size_) {
    return false;
  }
  return query_.starts_with(key);
}

CompletionCursor::CompletionCursor(Map<Slice> *map, Slice query,
                                   const MapCursorOptions &options)
    : ConditionalCursor<Slice>(map, options), query_(query) {
  this->init();
}

CompletionCursor::~CompletionCursor() {}

bool CompletionCursor::is_valid(Slice key) const {
  if (this->options_.flags & MAP_CURSOR_EXCEPT_QUERY) {
    if (key.size() <= query_.size()) {
      return false;
    }
  }
  return key.starts_with(query_);
}

ReverseCompletionCursor::ReverseCompletionCursor(
    Map<Slice> *map, Slice query, const MapCursorOptions &options)
    : ConditionalCursor<Slice>(map, options), query_(query) {
  this->init();
}

ReverseCompletionCursor::~ReverseCompletionCursor() {}

bool ReverseCompletionCursor::is_valid(Slice key) const {
  if (this->options_.flags & MAP_CURSOR_EXCEPT_QUERY) {
    if (key.size() <= query_.size()) {
      return false;
    }
  }
  return key.ends_with(query_);
}

}  // namespace map
}  // namespace alpha
}  // namespace grnxx
