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
#ifndef GRNXX_ALPHA_MAP_CURSOR_HPP
#define GRNXX_ALPHA_MAP_CURSOR_HPP

#include "grnxx/alpha/map.hpp"

#include <vector>

#include "grnxx/geo_point.hpp"

namespace grnxx {
namespace alpha {
namespace map {

template <typename T>
class IDCursor : public MapCursor<T> {
 public:
  IDCursor(Map<T> *map, int64_t min, int64_t max,
           const MapCursorOptions &options);
  ~IDCursor();

  bool next();
  bool remove();

 private:
  Map<T> *map_;
  int64_t cur_;
  int64_t end_;
  int64_t step_;
  uint64_t count_;
  MapCursorOptions options_;
  std::vector<std::pair<T, int64_t>> keys_;

  void init_order_by_id(int64_t min, int64_t max);
  void init_order_by_key(int64_t min, int64_t max);
};

template <typename T>
class ConditionalCursor : public MapCursor<T> {
 public:
  ConditionalCursor(Map<T> *map, const MapCursorOptions &options);
  virtual ~ConditionalCursor();

  bool next();
  bool remove();

 protected:
  Map<T> *map_;
  int64_t cur_;
  int64_t end_;
  int64_t step_;
  uint64_t count_;
  MapCursorOptions options_;
  std::vector<std::pair<T, int64_t>> keys_;

  void init();

  virtual bool is_valid(T key) const = 0;

 private:
  void init_order_by_id();
  void init_order_by_key();
};

template <typename T>
class KeyCursor : public ConditionalCursor<T> {
 public:
  KeyCursor(Map<T> *map, T min, T max, const MapCursorOptions &options);
  ~KeyCursor();

 private:
  T min_;
  T max_;

  bool is_valid(T key) const;
};

class BitwiseCompletionCursor : public ConditionalCursor<GeoPoint> {
 public:
  BitwiseCompletionCursor(Map<GeoPoint> *map, GeoPoint query, size_t bit_size,
                          const MapCursorOptions &options);
  ~BitwiseCompletionCursor();

 private:
  GeoPoint query_;
  uint64_t mask_;

  bool is_valid(GeoPoint key) const;
};

class PrefixCursor : public ConditionalCursor<Slice> {
 public:
  PrefixCursor(Map<Slice> *map, Slice query, size_t min_size,
               const MapCursorOptions &options);
  ~PrefixCursor();

 private:
  Slice query_;
  size_t min_size_;

  bool is_valid(Slice key) const;
};

class CompletionCursor : public ConditionalCursor<Slice> {
 public:
  CompletionCursor(Map<Slice> *map, Slice query,
                   const MapCursorOptions &options);
  ~CompletionCursor();

 private:
  Slice query_;

  bool is_valid(Slice key) const;
};

class ReverseCompletionCursor : public ConditionalCursor<Slice> {
 public:
  ReverseCompletionCursor(Map<Slice> *map, Slice query,
                          const MapCursorOptions &options);
  ~ReverseCompletionCursor();

 private:
  Slice query_;

  bool is_valid(Slice key) const;
};

}  // namespace map
}  // namespace alpha
}  // namespace grnxx

#endif  // GRNXX_ALPHA_MAP_CURSOR_HPP
