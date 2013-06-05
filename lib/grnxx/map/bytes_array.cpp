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
#include "grnxx/map/bytes_array.hpp"

#include <cstring>
#include <new>

#include "grnxx/logger.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {
namespace map {

struct BytesArrayHeader {
  uint64_t default_value_size;
  uint32_t ids_storage_node_id;
  uint32_t store_storage_node_id;

  BytesArrayHeader();
};

BytesArrayHeader::BytesArrayHeader()
    : default_value_size(0),
      ids_storage_node_id(STORAGE_INVALID_NODE_ID),
      store_storage_node_id(STORAGE_INVALID_NODE_ID) {}

BytesArray::~BytesArray() {}

BytesArray *BytesArray::create(Storage *storage, uint32_t storage_node_id) {
  return create(storage, storage_node_id, "");
}

BytesArray *BytesArray::create(Storage *storage, uint32_t storage_node_id,
                               ValueArg default_value) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage = nullptr";
    return nullptr;
  }
  std::unique_ptr<BytesArray> array(new (std::nothrow) BytesArray);
  if (!array) {
    GRNXX_ERROR() << "new grnxx::map::BytesArray failed";
    return nullptr;
  }
  if (!array->create_array(storage, storage_node_id, default_value)) {
    return nullptr;
  }
  return array.release();
}

BytesArray *BytesArray::open(Storage *storage, uint32_t storage_node_id) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage = nullptr";
    return nullptr;
  }
  std::unique_ptr<BytesArray> array(new (std::nothrow) BytesArray);
  if (!array) {
    GRNXX_ERROR() << "new grnxx::map::BytesArray failed";
    return nullptr;
  }
  if (!array->open_array(storage, storage_node_id)) {
    return nullptr;
  }
  return array.release();
}

bool BytesArray::unlink(Storage *storage, uint32_t storage_node_id) {
  std::unique_ptr<BytesArray> array(open(storage, storage_node_id));
  if (!array) {
    return false;
  }
  return storage->unlink_node(storage_node_id);
}

bool BytesArray::get(uint64_t value_id, Value *value) {
  uint64_t bytes_id;
  if (!ids_->get(value_id, &bytes_id)) {
    return false;
  }
  if (value) {
    if (bytes_id == BYTES_STORE_INVALID_BYTES_ID) {
      *value = default_value_;
    } else {
      return store_->get(bytes_id, value);
    }
  }
  return true;
}

bool BytesArray::set(uint64_t value_id, ValueArg value) {
  uint64_t *src_bytes_id = ids_->get_pointer(value_id);
  if (!src_bytes_id) {
    return false;
  }
  uint64_t dest_bytes_id;
  if (!store_->add(value, &dest_bytes_id)) {
    return false;
  }
  if (*src_bytes_id != BYTES_STORE_INVALID_BYTES_ID) {
    if (!store_->unset(*src_bytes_id)) {
      // The following unset() may not fail due to the above add().
      store_->unset(dest_bytes_id);
      return false;
    }
  }
  *src_bytes_id = dest_bytes_id;
  return true;
}

BytesArray::BytesArray()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      default_value_(),
      ids_(),
      store_() {}

// Create an array with the default value.
bool BytesArray::create_array(Storage *storage, uint32_t storage_node_id,
                              ValueArg default_value) {
  storage_ = storage;
  uint64_t storage_node_size = sizeof(BytesArrayHeader) + default_value.size();
  StorageNode storage_node =
      storage->create_node(storage_node_id, storage_node_size);
  if (!storage_node) {
    return false;
  }
  storage_node_id_ = storage_node.id();
  header_ = static_cast<BytesArrayHeader *>(storage_node.body());
  *header_ = BytesArrayHeader();
  header_->default_value_size = default_value.size();
  std::memcpy(header_ + 1, default_value.ptr(), default_value.size());
  default_value_ = Value(header_ + 1, default_value.size());
  ids_.reset(IDArray::create(storage, storage_node_id_,
                             BYTES_STORE_INVALID_BYTES_ID));
  store_.reset(BytesStore::create(storage, storage_node_id_));
  if (!ids_ || !store_) {
    storage->unlink_node(storage_node_id_);
    return false;
  }
  header_->ids_storage_node_id = ids_->storage_node_id();
  header_->store_storage_node_id = store_->storage_node_id();
  return true;
}

// Open an array.
bool BytesArray::open_array(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode storage_node = storage->open_node(storage_node_id);
  if (!storage_node) {
    return false;
  }
  storage_node_id_ = storage_node.id();
  header_ = static_cast<BytesArrayHeader *>(storage_node.body());
  default_value_ = Value(header_ + 1, header_->default_value_size);
  ids_.reset(IDArray::open(storage, header_->ids_storage_node_id));
  store_.reset(BytesStore::open(storage, header_->store_storage_node_id));
  if (!ids_ || !store_) {
    return false;
  }
  return true;
}

}  // namespace map
}  // namespace grnxx
