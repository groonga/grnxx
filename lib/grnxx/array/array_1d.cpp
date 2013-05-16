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
#include "grnxx/array/array_1d.hpp"

#include <memory>
#include <new>

#include "grnxx/logger.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {

struct Array1DHeader {
  uint64_t value_size;
  uint64_t page_size;
  uint32_t page_storage_node_id;

  Array1DHeader(uint64_t value_size, uint64_t page_size);
};

Array1DHeader::Array1DHeader(uint64_t value_size, uint64_t page_size)
    : value_size(value_size),
      page_size(page_size),
      page_storage_node_id(STORAGE_INVALID_NODE_ID) {}

Array1D::Array1D()
    : storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      page_(nullptr) {}

Array1D::~Array1D() {}

Array1D *Array1D::create(Storage *storage, uint32_t storage_node_id,
                         uint64_t value_size, uint64_t page_size,
                         const void *default_value, FillPage fill_page) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage = nullptr";
    return nullptr;
  }
  std::unique_ptr<Array1D> array(new (std::nothrow) Array1D);
  if (!array) {
    GRNXX_ERROR() << "new grnxx::Array1D failed: "
                  << "storage_node_id = " << storage_node_id
                  << ", value_size = " << value_size
                  << ", page_size = " << page_size
                  << ", has_default_value = " << (default_value != nullptr);
    return nullptr;
  }
  if (!array->create_array(storage, storage_node_id, value_size, page_size,
                           default_value, fill_page)) {
    return nullptr;
  }
  return array.release();
}

Array1D *Array1D::open(Storage *storage, uint32_t storage_node_id,
                       uint64_t value_size, uint64_t page_size) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage = nullptr";
    return nullptr;
  }
  std::unique_ptr<Array1D> array(new (std::nothrow) Array1D);
  if (!array) {
    GRNXX_ERROR() << "new grnxx::Array1D failed: "
                  << "storage_node_id = " << storage_node_id
                  << ", value_size = " << value_size
                  << ", page_size = " << page_size;
    return nullptr;
  }
  if (!array->open_array(storage, storage_node_id, value_size, page_size)) {
    return nullptr;
  }
  return array.release();
}

bool Array1D::unlink(Storage *storage, uint32_t storage_node_id,
                     uint64_t value_size, uint64_t page_size) {
  Array1D array;
  if (!array.open(storage, storage_node_id, value_size, page_size)) {
    return false;
  }
  return storage->unlink_node(storage_node_id);
}

bool Array1D::create_array(Storage *storage, uint32_t storage_node_id,
                           uint64_t value_size, uint64_t page_size,
                           const void *default_value, FillPage fill_page) {
  StorageNode storage_node =
      storage->create_node(storage_node_id, sizeof(Array1DHeader));
  if (!storage_node) {
    return false;
  }
  storage_node_id_ = storage_node.id();
  header_ = static_cast<Array1DHeader *>(storage_node.body());
  *header_ = Array1DHeader(value_size, page_size);
  StorageNode page_node =
      storage->create_node(storage_node_id_, value_size * page_size);
  if (!page_node) {
    storage->unlink_node(storage_node_id_);
    return false;
  }
  header_->page_storage_node_id = page_node.id();
  page_ = page_node.body();
  if (default_value) {
    fill_page(page_, default_value);
  }
  return true;
}

bool Array1D::open_array(Storage *storage, uint32_t storage_node_id,
                         uint64_t value_size, uint64_t page_size) {
  StorageNode storage_node = storage->open_node(storage_node_id);
  if (!storage_node) {
    return false;
  }
  if (storage_node.size() < sizeof(Array1DHeader)) {
    GRNXX_ERROR() << "invalid format: node_size = " << storage_node.size()
                  << ", header_size = " << sizeof(Array1DHeader);
    return false;
  }
  storage_node_id_ = storage_node.id();
  header_ = static_cast<Array1DHeader *>(storage_node.body());
  if (header_->value_size != value_size) {
    GRNXX_ERROR() << "parameter conflict: value_size = " << value_size
                  << ", stored_value_size = " << header_->value_size;
    return false;
  }
  if (header_->page_size != page_size) {
    GRNXX_ERROR() << "parameter conflict: page_size = " << page_size
                  << ", stored_page_size = " << header_->page_size;
    return false;
  }
  StorageNode page_node = storage->open_node(header_->page_storage_node_id);
  if (!page_node) {
    return false;
  }
  page_ = page_node.body();
  return true;
}

}  // namespace grnxx
