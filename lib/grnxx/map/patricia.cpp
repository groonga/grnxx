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
#include "grnxx/map/patricia.hpp"

#include <new>

#include "grnxx/bytes.hpp"
#include "grnxx/geo_point.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/map/helper.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {
namespace map {

struct PatriciaHeader {
  // TODO
  int64_t max_key_id;
  uint64_t num_keys;

  PatriciaHeader();
};

PatriciaHeader::PatriciaHeader()
    : max_key_id(MAP_MIN_KEY_ID - 1),
      num_keys(0) {}

template <typename T>
Patricia<T>::Patricia()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr) {}

template <typename T>
Patricia<T>::~Patricia() {}

template <typename T>
Patricia<T> *Patricia<T>::create(Storage *storage,
                                 uint32_t storage_node_id,
                                 const MapOptions &options) {
  std::unique_ptr<Patricia> map(new (std::nothrow) Patricia);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::Patricia failed";
    return nullptr;
  }
  if (!map->create_map(storage, storage_node_id, options)) {
    return nullptr;
  }
  return map.release();
}

template <typename T>
Patricia<T> *Patricia<T>::open(Storage *storage,
                               uint32_t storage_node_id) {
  std::unique_ptr<Patricia> map(new (std::nothrow) Patricia);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::Patricia failed";
    return nullptr;
  }
  if (!map->open_map(storage, storage_node_id)) {
    return nullptr;
  }
  return map.release();
}

template <typename T>
uint32_t Patricia<T>::storage_node_id() const {
  return storage_node_id_;
}

template <typename T>
MapType Patricia<T>::type() const {
  return MAP_DOUBLE_ARRAY;
}

template <typename T>
int64_t Patricia<T>::max_key_id() const {
  return header_->max_key_id;
}

template <typename T>
uint64_t Patricia<T>::num_keys() const {
  return header_->num_keys;
}

template <typename T>
bool Patricia<T>::create_map(Storage *storage, uint32_t storage_node_id,
                             const MapOptions &) {
  storage_ = storage;
  StorageNode storage_node =
      storage->create_node(storage_node_id, sizeof(PatriciaHeader));
  if (!storage_node) {
    return false;
  }
  storage_node_id_ = storage_node.id();
  header_ = static_cast<PatriciaHeader *>(storage_node.body());
  *header_ = PatriciaHeader();
  // TODO
  return false;
}

template <typename T>
bool Patricia<T>::open_map(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode storage_node = storage->open_node(storage_node_id);
  if (!storage_node) {
    return false;
  }
  if (storage_node.size() < sizeof(PatriciaHeader)) {
    GRNXX_ERROR() << "invalid format: size = " << storage_node.size()
                  << ", header_size = " << sizeof(PatriciaHeader);
    return false;
  }
  storage_node_id_ = storage_node_id;
  header_ = static_cast<PatriciaHeader *>(storage_node.body());
  // TODO
  return false;
}

template class Patricia<int8_t>;
template class Patricia<uint8_t>;
template class Patricia<int16_t>;
template class Patricia<uint16_t>;
template class Patricia<int32_t>;
template class Patricia<uint32_t>;
template class Patricia<int64_t>;
template class Patricia<uint64_t>;
template class Patricia<double>;
template class Patricia<GeoPoint>;
template class Patricia<Bytes>;

}  // namespace map
}  // namespace grnxx
