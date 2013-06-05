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
#include "grnxx/map/double_array.hpp"

#include <new>

#include "grnxx/bytes.hpp"
#include "grnxx/geo_point.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/map/double_array/header.hpp"
#include "grnxx/map/helper.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {
namespace map {

template <typename T>
Map<T> *DoubleArray<T>::create(Storage *, uint32_t, const MapOptions &) {
  GRNXX_ERROR() << "invalid combination";
  return nullptr;
}

template <typename T>
Map<T> *DoubleArray<T>::open(Storage *, uint32_t) {
  GRNXX_ERROR() << "invalid combination";
  return nullptr;
}

template class DoubleArray<int8_t>;
template class DoubleArray<uint8_t>;
template class DoubleArray<int16_t>;
template class DoubleArray<uint16_t>;
template class DoubleArray<int32_t>;
template class DoubleArray<uint32_t>;
template class DoubleArray<int64_t>;
template class DoubleArray<uint64_t>;
template class DoubleArray<double>;
template class DoubleArray<GeoPoint>;

DoubleArray<Bytes>::DoubleArray()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr) {}

DoubleArray<Bytes>::~DoubleArray() {}

DoubleArray<Bytes> *DoubleArray<Bytes>::create(Storage *storage,
                                               uint32_t storage_node_id,
                                               const MapOptions &options) {
  std::unique_ptr<DoubleArray> map(new (std::nothrow) DoubleArray);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::DoubleArray failed";
    return nullptr;
  }
  if (!map->create_map(storage, storage_node_id, options)) {
    return nullptr;
  }
  return map.release();
}

DoubleArray<Bytes> *DoubleArray<Bytes>::open(Storage *storage,
                                             uint32_t storage_node_id) {
  std::unique_ptr<DoubleArray> map(new (std::nothrow) DoubleArray);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::DoubleArray failed";
    return nullptr;
  }
  if (!map->open_map(storage, storage_node_id)) {
    return nullptr;
  }
  return map.release();
}

uint32_t DoubleArray<Bytes>::storage_node_id() const {
  return storage_node_id_;
}

MapType DoubleArray<Bytes>::type() const {
  return MAP_DOUBLE_ARRAY;
}

int64_t DoubleArray<Bytes>::max_key_id() const {
  return header_->max_key_id;
}

uint64_t DoubleArray<Bytes>::num_keys() const {
  return header_->num_keys;
}

bool DoubleArray<Bytes>::create_map(Storage *storage, uint32_t storage_node_id,
                                    const MapOptions &) {
  storage_ = storage;
  StorageNode storage_node =
      storage->create_node(storage_node_id, sizeof(Header));
  if (!storage_node) {
    return false;
  }
  storage_node_id_ = storage_node.id();
  header_ = static_cast<Header *>(storage_node.body());
  *header_ = Header();
  // TODO
  return false;
}

bool DoubleArray<Bytes>::open_map(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode storage_node = storage->open_node(storage_node_id);
  if (!storage_node) {
    return false;
  }
  if (storage_node.size() < sizeof(Header)) {
    GRNXX_ERROR() << "invalid format: size = " << storage_node.size()
                  << ", header_size = " << sizeof(Header);
    return false;
  }
  storage_node_id_ = storage_node_id;
  header_ = static_cast<Header *>(storage_node.body());
  // TODO
  return false;
}

}  // namespace map
}  // namespace grnxx
