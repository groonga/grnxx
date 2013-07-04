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
#ifndef GRNXX_MAP_BYTES_ARRAY_HPP
#define GRNXX_MAP_BYTES_ARRAY_HPP

#include "grnxx/features.hpp"

#include <memory>

#include "grnxx/array.hpp"
#include "grnxx/bytes.hpp"
#include "grnxx/duration.hpp"
#include "grnxx/traits.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Storage;

namespace map {

class BytesStore;

struct BytesArrayHeader;

class BytesArray {
 public:
  using Value = typename Traits<Bytes>::Type;
  using ValueArg = typename Traits<Bytes>::ArgumentType;

  using IDArray = Array<uint64_t>;

  ~BytesArray();

  // Create an array.
  static BytesArray *create(Storage *storage, uint32_t storage_node_id);
  // Create an array with the default value.
  static BytesArray *create(Storage *storage, uint32_t storage_node_id,
                            ValueArg default_value);
  // Open an array.
  static BytesArray *open(Storage *storage, uint32_t storage_node_id);

  // Unlink an array.
  static bool unlink(Storage *storage, uint32_t storage_node_id);

  // Return the number of values in each page.
  static constexpr uint64_t page_size() {
    return IDArray::page_size();
  }
  // Return the number of pages in each table.
  static constexpr uint64_t table_size() {
    return IDArray::table_size();
  }
  // Return the number of tables in each secondary table.
  static constexpr uint64_t secondary_table_size() {
    return IDArray::secondary_table_size();
  }
  // Return the number of values in Array.
  static constexpr uint64_t size() {
    return IDArray::size();
  }

  // Return the storage node ID.
  uint32_t storage_node_id() const {
    return storage_node_id_;
  }

  // Get a value and return true on success.
  // The value is assigned to "*value" iff "value" != nullptr.
  bool get(uint64_t value_id, Value *value);
  // Set a value and return true on success.
  bool set(uint64_t value_id, ValueArg value);

  // Sweep empty pages whose modified time < (now - lifetime).
  bool sweep(Duration lifetime);

 private:
  Storage *storage_;
  uint32_t storage_node_id_;
  BytesArrayHeader *header_;
  Value default_value_;
  std::unique_ptr<IDArray> ids_;
  std::unique_ptr<BytesStore> store_;

  BytesArray();

  // Create an array with the default value.
  void create_array(Storage *storage, uint32_t storage_node_id,
                    ValueArg default_value);
  // Open an array.
  void open_array(Storage *storage, uint32_t storage_node_id);
};

}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_BYTES_ARRAY_HPP
