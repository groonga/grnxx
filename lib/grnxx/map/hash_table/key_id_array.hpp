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
#ifndef GRNXX_MAP_HASH_TABLE_KEY_ID_ARRAY_HPP
#define GRNXX_MAP_HASH_TABLE_KEY_ID_ARRAY_HPP

#include "grnxx/features.hpp"

#include "grnxx/array_impl.hpp"
#include "grnxx/exception.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/map.hpp"
#include "grnxx/storage.hpp"
#include "grnxx/types.hpp"

namespace grnxx {
namespace map {
namespace hash_table {

// Change the array size based on the size of "T".
// Note that the size of link array is N/64 where N is the size of BitArray.
template <typename T, size_t T_SIZE = sizeof(T)>
struct KeyIDArrayHelper;

// Map<T> has at most 2^40 different keys.
template <typename T, size_t T_SIZE>
struct KeyIDArrayHelper {
  using ImplType = ArrayImpl<int64_t, 65536, 8192, 4096>;
};

// Map<T> has at most 2^8 different keys.
template <typename T>
struct KeyIDArrayHelper<T, 1> {
  using ImplType = ArrayImpl<int64_t, 512, 1, 1>;
};

// Map<T> has at most 2^16 different keys.
template <typename T>
struct KeyIDArrayHelper<T, 2> {
  using ImplType = ArrayImpl<int64_t, 512, 256, 1>;
};

// Map<T> has at most 2^32 different keys.
template <typename T>
struct KeyIDArrayHelper<T, 4> {
  using ImplType = ArrayImpl<int64_t, 65536, 512, 256>;
};

struct KeyIDArrayHeader {
  uint32_t impl_storage_node_id;
  uint32_t reserved;
  uint64_t mask;

  KeyIDArrayHeader()
      : impl_storage_node_id(STORAGE_INVALID_NODE_ID),
        reserved(0),
        mask(0) {}
};

template <typename T>
class KeyIDArray {
  using ArrayImpl = typename KeyIDArrayHelper<T>::ImplType;

 public:
  using Value = typename ArrayImpl::Value;
  using ValueArg = typename ArrayImpl::ValueArg;

  KeyIDArray()
      : storage_(nullptr),
        storage_node_id_(STORAGE_INVALID_NODE_ID),
        header_(nullptr),
        impl_(),
        mask_(0) {}
  ~KeyIDArray() {}

  // Create a table.
  static KeyIDArray *create(Storage *storage, uint32_t storage_node_id,
                            uint64_t mask) {
    std::unique_ptr<KeyIDArray> array(create_instance());
    if (!array) {
      return nullptr;
    }
    if (!array->create_array(storage, storage_node_id, mask)) {
      return nullptr;
    }
    return array.release();
  }

  // Open an array.
  static KeyIDArray *open(Storage *storage, uint32_t storage_node_id) {
    std::unique_ptr<KeyIDArray> array(create_instance());
    if (!array) {
      return nullptr;
    }
    if (!array->open_array(storage, storage_node_id)) {
      return nullptr;
    }
    return array.release();
  }

  // Unlink an array.
  static bool unlink(Storage *storage, uint32_t storage_node_id) {
    std::unique_ptr<KeyIDArray> array(open(storage, storage_node_id));
    if (!array) {
      return false;
    }
    return storage->unlink_node(storage_node_id);
  }

  // Return the number of values in each page.
  static constexpr uint64_t page_size() {
    return ArrayImpl::page_size();
  }
  // Return the number of pages in each table.
  static constexpr uint64_t table_size() {
    return ArrayImpl::table_size();
  }
  // Return the number of tables in each secondary table.
  static constexpr uint64_t secondary_table_size() {
    return ArrayImpl::secondary_table_size();
  }
  // Return the number of values in Array.
  static constexpr uint64_t size() {
    return ArrayImpl::size();
  }

  // Return the storage node ID.
  uint32_t storage_node_id() const {
    return storage_node_id_;
  }
  // Return the mask.
  uint64_t mask() const {
    return mask_;
  }

  // Get a value and return true on success.
  // The value is assigned to "*value" iff "value" != nullptr.
  bool get(uint64_t value_id, Value *value) {
    return impl_.get(value_id & mask_, value);
  }
  // Set a value and return true on success.
  bool set(uint64_t value_id, ValueArg value) {
    return impl_.set(value_id & mask_, value);
  }
  // Get a value and return its address on success.
  Value *get_pointer(uint64_t value_id) {
    return impl_.get_pointer(value_id & mask_);
  }
  // Get a page and return its starting address on success.
  Value *get_page(uint64_t page_id) {
    return impl_.get_page(page_id & (mask_ / page_size()));
  }

 private:
  Storage *storage_;
  uint32_t storage_node_id_;
  KeyIDArrayHeader *header_;
  ArrayImpl impl_;
  uint64_t mask_;

  static KeyIDArray *create_instance() {
    KeyIDArray * const array = new (std::nothrow) KeyIDArray;
    if (!array) {
      GRNXX_ERROR() << "new grnxx::map::hash_table::KeyIDArray failed";
      throw MemoryError();
    }
    return array;
  }

  bool create_array(Storage *storage, uint32_t storage_node_id,
                    uint64_t mask) {
    storage_ = storage;
    StorageNode storage_node =
        storage_->create_node(storage_node_id, sizeof(KeyIDArrayHeader));
    if (!storage_node) {
      return false;
    }
    storage_node_id_ = storage_node.id();
    try {
      header_ = static_cast<KeyIDArrayHeader *>(storage_node.body());
      *header_ = KeyIDArrayHeader();
      impl_.create(storage, storage_node_id_, MAP_INVALID_KEY_ID);
      mask_ = mask;
      header_->impl_storage_node_id = impl_.storage_node_id();
      header_->mask = mask_;
    } catch (...) {
      storage_->unlink_node(storage_node_id_);
      throw;
    }
    return true;
  }

  bool open_array(Storage *storage, uint32_t storage_node_id) {
    storage_ = storage;
    StorageNode storage_node = storage_->open_node(storage_node_id);
    if (!storage_node) {
      return false;
    }
    storage_node_id_ = storage_node.id();
    header_ = static_cast<KeyIDArrayHeader *>(storage_node.body());
    impl_.open(storage, header_->impl_storage_node_id);
    mask_ = header_->mask;
    return true;
  }
};

}  // namespace hash_table
}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_HASH_TABLE_KEY_ID_ARRAY_HPP
