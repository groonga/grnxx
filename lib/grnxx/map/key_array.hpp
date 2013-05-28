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
#ifndef GRNXX_MAP_KEY_ARRAY_HPP
#define GRNXX_MAP_KEY_ARRAY_HPP

#include "grnxx/features.hpp"

#include "grnxx/array.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Storage;

namespace map {

// Change the settings based on the key type.
template <typename T, size_t T_SIZE = sizeof(T)>
struct KeyArrayTraits {
  // Map<T> has at most 2^40 different keys.
  using ArrayType = Array<T>;
};
template <typename T>
struct KeyArrayTraits<T, 1> {
  // Map<T> has at most 2^8 different keys.
  using ArrayType = Array<T, 256, 1, 1>;
};
template <typename T>
struct KeyArrayTraits<T, 2> {
  // Map<T> has at most 2^16 different keys.
  using ArrayType = Array<T, 256, 256, 1>;
};
template <typename T>
struct KeyArrayTraits<T, 4> {
  // Map<T> has at most 2^32 different keys.
  using ArrayType = Array<T, 65536, 256, 256>;
};

template <typename T>
class KeyArray {
 public:
  using ArrayImpl = typename KeyArrayTraits<T>::ArrayType;
  using Key = typename ArrayImpl::Value;
  using KeyArg = typename ArrayImpl::ValueArg;

  // Return true iff the array is valid.
  explicit operator bool() const {
    return static_cast<bool>(impl_);
  }

  // Create an array.
  bool create(Storage *storage, uint32_t storage_node_id) {
    return impl_.create(storage, storage_node_id);
  }
  // Create an array with the default key.
  bool create(Storage *storage, uint32_t storage_node_id,
              KeyArg default_key) {
    return impl_.create(storage, storage_node_id, default_key);
  }
  // Open an array.
  bool open(Storage *storage, uint32_t storage_node_id) {
    return impl_.open(storage, storage_node_id);
  }

  // Unlink an array.
  static bool unlink(Storage *storage, uint32_t storage_node_id) {
    return ArrayImpl::unlink(storage, storage_node_id);
  }

  // Return the storage node ID.
  uint32_t storage_node_id() const {
    return impl_.storage_node_id();
  }

  // Get a key.
  // This function throws an exception on failure.
  Key operator[](uint64_t key_id) {
    return impl_[key_id];
  }

  // Get a key and return true on success.
  // The key is assigned to "*key" iff "key" != nullptr.
  bool get(uint64_t key_id, Key *key) {
    return impl_.get(key_id, key);
  }
  // Set a key and return true on success.
  bool set(uint64_t key_id, KeyArg key) {
    return impl_.set(key_id, key);
  }

 private:
  ArrayImpl impl_;
};

}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_KEY_ARRAY_HPP
