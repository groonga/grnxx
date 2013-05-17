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
#ifndef GRNXX_MAP_BYTES_STORE_HPP
#define GRNXX_MAP_BYTES_STORE_HPP

#include "grnxx/features.hpp"

#include "grnxx/bytes.hpp"
#include "grnxx/traits.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Storage;

namespace map {

constexpr uint64_t BYTES_STORE_ID_MASK = (1ULL << 60) - 1;
constexpr uint64_t BYTES_STORE_MIN_ID  = 0;
constexpr uint64_t BYTES_STORE_MAX_ID  = BYTES_STORE_ID_MASK;

class BytesStore {
 public:
  using BytesArg = typename Traits<Bytes>::ArgumentType;

  BytesStore();
  virtual ~BytesStore();

  // Create a store.
  static BytesStore *create(Storage *storage, uint32_t storage_node_id);
  // Open a store.
  static BytesStore *open(Storage *storage, uint32_t storage_node_id);

  // Unlink a store.
  static bool unlink(Storage *storage, uint32_t storage_node_id);

  // TODO: get() should be inlined?
  // Get a stored byte sequence.
  virtual bool get(uint64_t bytes_id, Bytes *bytes) = 0;
  // Remove a stored byte sequence.
  virtual bool unset(uint64_t bytes_id) = 0;
  // Add a byte sequence.
  virtual bool add(BytesArg bytes, uint64_t *bytes_id) = 0;
};

}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_BYTES_STORE_HPP
