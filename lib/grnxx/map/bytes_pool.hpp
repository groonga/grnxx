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
#ifndef GRNXX_MAP_BYTES_POOL_HPP
#define GRNXX_MAP_BYTES_POOL_HPP

#include "grnxx/features.hpp"

#include <memory>

#include "grnxx/array.hpp"
#include "grnxx/bytes.hpp"
#include "grnxx/duration.hpp"
#include "grnxx/periodic_clock.hpp"
#include "grnxx/time.hpp"
#include "grnxx/traits.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Storage;

namespace map {

constexpr uint64_t BYTES_POOL_VALUE_ID_MASK = (1ULL << 61) - 1;
constexpr uint64_t BYTES_POOL_MAX_VALUE_ID  = BYTES_POOL_VALUE_ID_MASK;

struct BytesPoolHeader;

enum BytesPoolPageStatus : uint32_t {
  // The next byte sequence will be added to this page.
  BYTES_POOL_PAGE_ACTIVE = 0,
  // This page is in use.
  BYTES_POOL_PAGE_IN_USE = 1,
  // This page is empty but not ready-to-use.
  BYTES_POOL_PAGE_EMPTY  = 2,
  // This page is empty and ready-to-use.
  BYTES_POOL_PAGE_IDLE   = 3
};

struct BytesPoolPageHeader {
  // ACTIVE, IN_USE, EMPTY, and IDLE.
  BytesPoolPageStatus status;
  union {
    // ACTIVE and IN_USE.
    uint32_t size_in_use;
    // EMPTY and IDLE.
    uint32_t next_page_id;
  };
  // ACTIVE, IN_USE, EMPTY, and IDLE.
  Time modified_time;

  // Initialize member variables.
  BytesPoolPageHeader();
};

class BytesPool {
  using Header     = BytesPoolHeader;
  using PageHeader = BytesPoolPageHeader;

  static constexpr uint32_t POOL_PAGE_SIZE     = 1U << 20;
  static constexpr uint32_t POOL_TABLE_SIZE    = 1U << 14;

  static constexpr uint32_t MAX_VALUE_SIZE     = 4096;

  // The number of bits allocated for representing a value size.
  static constexpr uint8_t  VALUE_ID_SIZE_BITS = 13;
  static constexpr uint64_t VALUE_ID_SIZE_MASK =
      (1ULL << VALUE_ID_SIZE_BITS) - 1;

  using Pool            = Array<uint8_t, POOL_PAGE_SIZE, POOL_TABLE_SIZE>;
  using PageHeaderArray = Array<PageHeader, POOL_TABLE_SIZE>;

 public:
  using Value    = typename Traits<Bytes>::Type;
  using ValueArg = typename Traits<Bytes>::ArgumentType;

  ~BytesPool();

  // Create a pool.
  static BytesPool *create(Storage *storage, uint32_t storage_node_id);
  // Opena pool.
  static BytesPool *open(Storage *storage, uint32_t storage_node_id);

  // Unlink a pool.
  static void unlink(Storage *storage, uint32_t storage_node_id);

  // Return the storage node ID.
  uint32_t storage_node_id() const {
    return storage_node_id_;
  }

  // Return the page size.
  static constexpr uint64_t page_size() {
    return POOL_PAGE_SIZE;
  }

  // Get a byte sequence.
  Value get(uint64_t value_id) {
    const uint64_t offset = get_offset(value_id);
    const uint32_t size = get_size(value_id);
    return Value(&pool_->get_value(offset), size);
  }
  // Remove a byte sequence.
  void unset(uint64_t value_id);
  // Add a byte sequence and return its ID.
  uint64_t add(ValueArg value);

  // Return the actually used size of a page in use.
  // If a page is not in use, return the page size.
  uint64_t get_page_size_in_use(uint64_t page_id) {
    const PageHeader &page_header = page_headers_->get_value(page_id);
    if (page_header.status == BYTES_POOL_PAGE_IN_USE) {
      return page_header.size_in_use;
    } else {
      return page_size();
    }
  }

  // Remove all the byte sequences.
  void truncate();

  // Sweep empty pages whose modified time <= (now - lifetime).
  void sweep(Duration lifetime);

 private:
  Storage *storage_;
  uint32_t storage_node_id_;
  BytesPoolHeader *header_;
  std::unique_ptr<Pool> pool_;
  std::unique_ptr<PageHeaderArray> page_headers_;
  PeriodicClock clock_;

  BytesPool();

  void create_pool(Storage *storage, uint32_t storage_node_id);
  void open_pool(Storage *storage, uint32_t storage_node_id);

  // Reserve a page.
  PageHeader *reserve_active_page(uint32_t *page_id);
  // Make a page empty.
  void make_page_empty(uint32_t page_id, PageHeader *page_header);
  // Make a page idle.
  void make_page_idle(uint32_t page_id, PageHeader *page_header);

  static uint64_t get_value_id(uint64_t offset, uint32_t size) {
    return (offset * (VALUE_ID_SIZE_MASK + 1)) | size;
  }
  static uint64_t get_offset(uint64_t value_id) {
    return value_id / (VALUE_ID_SIZE_MASK + 1);
  }
  static uint32_t get_size(uint64_t value_id) {
    return static_cast<uint32_t>(value_id & VALUE_ID_SIZE_MASK);
  }
  static uint32_t get_page_id(uint64_t offset) {
    return static_cast<uint32_t>(offset / POOL_PAGE_SIZE);
  }
  static uint32_t get_offset_in_page(uint64_t offset) {
    return static_cast<uint32_t>(offset % POOL_PAGE_SIZE);
  }
};

}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_BYTES_POOL_HPP
