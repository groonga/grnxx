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
#ifndef GRNXX_MAP_BITMAP_HPP
#define GRNXX_MAP_BITMAP_HPP

#include "grnxx/features.hpp"

#include "grnxx/bit_array.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Storage;

namespace map {

// Change the settings based on the target type.
template <typename T, size_t T_SIZE = sizeof(T)>
struct BitmapTraits {
  using ArrayType = BitArray<>;
};
template <typename T>
struct BitmapTraits<T, 1> {
  // T has 2^8 different values.
  using ArrayType = BitArray<256, 1, 1>;
};
template <typename T>
struct BitmapTraits<T, 2> {
  // T has 2^16 different values.
  using ArrayType = BitArray<256, 256, 1>;
};
template <typename T>
struct BitmapTraits<T, 4> {
  // T has 2^32 different values.
  using ArrayType = BitArray<65536, 256, 256>;
};

// TODO: Unused member functions will be removed in future.
template <typename T>
class Bitmap {
 public:
  using ArrayImpl = typename BitmapTraits<T>::ArrayType;
  using Bit = typename ArrayImpl::Value;
  using BitArg = typename ArrayImpl::ValueArg;
  using Unit = typename ArrayImpl::Unit;

  // Return true iff the bitmap is valid.
  explicit operator bool() const {
    return static_cast<bool>(impl_);
  }

  // Create a bitmap.
  bool create(Storage *storage, uint32_t storage_node_id) {
    return impl_.create(storage, storage_node_id);
  }
  // Create a bitmap with the default bit.
  bool create(Storage *storage, uint32_t storage_node_id,
              BitArg default_bit) {
    return impl_.create(storage, storage_node_id, default_bit);
  }
  // Open a bitmap.
  bool open(Storage *storage, uint32_t storage_node_id) {
    return impl_.open(storage, storage_node_id);
  }

  // Unlink a bitmap.
  static bool unlink(Storage *storage, uint32_t storage_node_id) {
    return ArrayImpl::unlink(storage, storage_node_id);
  }

  // Return the storage node ID.
  uint32_t storage_node_id() const {
    return impl_.storage_node_id();
  }

  // Get a bit.
  // This function throws an exception on failure.
  Bit operator[](uint64_t bit_id) {
    return impl_[bit_id];
  }

  // Get a bit and return true on success.
  // The bit is assigned to "*bit" iff "bit" != nullptr.
  bool get(uint64_t bit_id, Bit *bit) {
    return impl_.get(bit_id, bit);
  }
  // Set a bit and return true on success.
  // Note that if bits in the same byte are set at the same time, the result is
  // undefined.
  bool set(uint64_t bit_id, BitArg bit) {
    return impl_.set(bit_id, bit);
  }

  // Get a unit and return its address on success.
  Unit *get_unit(uint64_t unit_id) {
    return impl_.get_unit(unit_id);
  }

 private:
  ArrayImpl impl_;
};

}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_BITMAP_HPP
