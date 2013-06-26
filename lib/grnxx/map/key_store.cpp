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
#include "grnxx/map/key_store.hpp"

#include "grnxx/geo_point.hpp"
#include "grnxx/intrinsic.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/map.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {
namespace map {
namespace {

constexpr uint64_t INVALID_LINK = ~0ULL;

}  // namespace

KeyStoreHeader::KeyStoreHeader()
    : max_key_id(MAP_MIN_KEY_ID - 1),
      num_keys(0),
      latest_link(INVALID_LINK),
      keys_storage_node_id(STORAGE_INVALID_NODE_ID),
      bits_storage_node_id(STORAGE_INVALID_NODE_ID),
      links_storage_node_id(STORAGE_INVALID_NODE_ID) {}

template <typename T>
KeyStore<T>::KeyStore() {}

template <typename T>
KeyStore<T>::~KeyStore() {}

template <typename T>
KeyStore<T> *KeyStore<T>::create(Storage *storage, uint32_t storage_node_id) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage == nullptr";
    return nullptr;
  }
  std::unique_ptr<KeyStore> store(new (std::nothrow) KeyStore);
  if (!store) {
    GRNXX_ERROR() << "new grnxx::map::KeyStore failed";
    return nullptr;
  }
  if (!store->create_store(storage, storage_node_id)) {
    return nullptr;
  }
  return store.release();
}

template <typename T>
KeyStore<T> *KeyStore<T>::open(Storage *storage, uint32_t storage_node_id) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage == nullptr";
    return nullptr;
  }
  std::unique_ptr<KeyStore> store(new (std::nothrow) KeyStore);
  if (!store) {
    GRNXX_ERROR() << "new grnxx::map::KeyStore failed";
    return nullptr;
  }
  if (!store->open_store(storage, storage_node_id)) {
    return nullptr;
  }
  return store.release();
}

template <typename T>
bool KeyStore<T>::unset(int64_t key_id) {
  // Prepare to update "bits_" and "links_".
  const uint64_t unit_id = key_id / bits_->unit_size();
  const uint64_t unit_bit = 1ULL << (key_id % bits_->unit_size());
  BitArrayUnit * const unit = bits_->get_unit(unit_id);
  if (!unit) {
    // Error.
    return false;
  }
  if ((*unit & unit_bit) == 0) {
    // Not found.
    return false;
  }
  // Set "link" if this operation creates the first 0 bit in the unit.
  uint64_t *link = nullptr;
  if (*unit == ~BitArrayUnit(0)) {
    link = links_->get_pointer(unit_id);
    if (!link) {
      // Error.
      return false;
    }
  }
  *unit &= ~unit_bit;
  if (link) {
    *link = header_->latest_link;
    header_->latest_link = *link;
  }
  --header_->num_keys;
  return true;
}

template <typename T>
bool KeyStore<T>::add(KeyArg key, int64_t *key_id) {
  // Find an unused key ID.
  const bool is_new_unit = (header_->latest_link == INVALID_LINK);
  uint64_t unit_id;
  if (is_new_unit) {
    unit_id = (header_->max_key_id + 1) / bits_->unit_size();
  } else {
    unit_id = header_->latest_link;
  }
  BitArrayUnit * const unit = bits_->get_unit(unit_id);
  if (!unit) {
    // Error.
    return false;
  }
  const uint8_t unit_bit_id = bit_scan_forward(~*unit);
  const uint64_t unit_bit = 1ULL << unit_bit_id;
  const int64_t next_key_id = (unit_id * bits_->unit_size()) + unit_bit_id;
  uint64_t *link = nullptr;
  if (is_new_unit) {
    if (!links_->set(unit_id, INVALID_LINK)) {
      // Error.
      return false;
    }
    *unit = 0;
    header_->latest_link = (header_->max_key_id + 1) / bits_->unit_size();
  } else if ((*unit | unit_bit) == ~BitArrayUnit(0)) {
    // The unit will be removed from "links_" if it becomes full.
    link = links_->get_pointer(header_->latest_link);
    if (!link) {
      // Error.
      return false;
    }
  }
  if (!keys_->set(next_key_id, key)) {
    // Error.
    return false;
  }
  if (link) {
    header_->latest_link = *link;
  }
  *unit |= unit_bit;
  if (next_key_id > header_->max_key_id) {
    header_->max_key_id = next_key_id;
  }
  ++header_->num_keys;
  if (key_id) {
    *key_id = next_key_id;
  }
  return true;
}

template <typename T>
bool KeyStore<T>::truncate() {
  header_->max_key_id = MAP_MIN_KEY_ID - 1;
  header_->num_keys = 0;
  header_->latest_link = INVALID_LINK;
  return true;
}

template <typename T>
bool KeyStore<T>::create_store(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode storage_node =
      storage->create_node(storage_node_id, sizeof(Header));
  if (!storage_node) {
    return false;
  }
  storage_node_id_ = storage_node.id();
  header_ = static_cast<Header *>(storage_node.body());
  *header_ = Header();
  keys_.reset(KeyArray::create(storage, storage_node_id_));
  bits_.reset(BitArray::create(storage, storage_node_id_));
  links_.reset(LinkArray::create(storage, storage_node_id_));
  if (!keys_ || !bits_ || !links_) {
    storage->unlink_node(storage_node_id_);
    return false;
  }
  header_->keys_storage_node_id = keys_->storage_node_id();
  header_->bits_storage_node_id = bits_->storage_node_id();
  header_->links_storage_node_id = links_->storage_node_id();
  return true;
}

template <typename T>
bool KeyStore<T>::open_store(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode storage_node = storage->open_node(storage_node_id);
  if (!storage_node) {
    return false;
  }
  storage_node_id_ = storage_node.id();
  header_ = static_cast<Header *>(storage_node.body());
  // TODO: Check the format.
  keys_.reset(KeyArray::open(storage, header_->keys_storage_node_id));
  bits_.reset(BitArray::open(storage, header_->bits_storage_node_id));
  links_.reset(LinkArray::open(storage, header_->links_storage_node_id));
  if (!keys_ || !bits_ || !links_) {
    return false;
  }
  return true;
}

template class KeyStore<int8_t>;
template class KeyStore<uint8_t>;
template class KeyStore<int16_t>;
template class KeyStore<uint16_t>;
template class KeyStore<int32_t>;
template class KeyStore<uint32_t>;
template class KeyStore<int64_t>;
template class KeyStore<uint64_t>;
template class KeyStore<double>;
template class KeyStore<GeoPoint>;
template class KeyStore<Bytes>;

}  // namespace map
}  // namespace grnxx
