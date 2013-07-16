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
#ifndef GRNXX_ALPHA_PAGED_ARRAY_HPP
#define GRNXX_ALPHA_PAGED_ARRAY_HPP

#include "grnxx/features.hpp"

#include <memory>

#include "grnxx/traits.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Storage;

namespace alpha {

struct PagedArrayHeader;

class PagedArrayImpl {
  using FillPage = void (*)(void *page, const void *value, uint64_t page_size);

 public:
  PagedArrayImpl();
  ~PagedArrayImpl();

  // Return true iff "*this" is initialized.
  explicit operator bool() const {
    return storage_ != nullptr;
  }

  // Create an array and fill it with "value".
  void create(Storage *storage, uint32_t storage_node_id,
              uint64_t value_size, uint64_t size, uint64_t page_size,
              const void *default_value = nullptr,
              FillPage fill_page = nullptr);
  // Open an array.
  void open(Storage *storage, uint32_t storage_node_id,
            uint64_t value_size, FillPage fill_page = nullptr);

  // Unlink an array.
  static bool unlink(Storage *storage, uint32_t storage_node_id,
                     uint64_t value_size);

  // Return the storage node ID.
  uint32_t storage_node_id() const {
    return storage_node_id_;
  }
  // Return the size.
  uint64_t size() const {
    return size_;
  }
  // Return the page size.
  uint64_t page_size() const {
    return page_size_;
  }

  // Return a reference to a value.
  template <typename T>
  T &get_reference(uint64_t value_id) {
    const uint64_t page_id = value_id >> page_shift_;
    if (page_id >= table_size_) {
      resize_table(page_id + 1);
    }
    void *page = pages_[page_id];
    if (page == invalid_page_address()) {
      page = reserve_page(page_id);
    }
    return static_cast<T *>(page)[value_id];
  }

 private:
  Storage *storage_;
  uint32_t storage_node_id_;
  uint64_t size_;
  uint64_t page_size_;
  uint64_t page_shift_;
  uint64_t page_mask_;
  uint64_t table_size_;
  std::unique_ptr<void *[]> pages_;
  uint32_t *table_;
  PagedArrayHeader *header_;
  const void *default_value_;
  FillPage fill_page_;

  void create_array(Storage *storage, uint32_t storage_node_id,
                    uint64_t value_size, uint64_t size, uint64_t page_size,
                    const void *default_value, FillPage fill_page);
  void open_array(Storage *storage, uint32_t storage_node_id,
                  uint64_t value_size, FillPage fill_page);

  void resize_table(uint64_t table_size);
  void *reserve_page(uint64_t page_id);
  void update_table();

  static void *invalid_page_address() {
    return reinterpret_cast<void *>(~uintptr_t(0));
  }

  void swap(PagedArrayImpl &rhs);
};

template <typename T>
class PagedArray {
 public:
  using Value = typename Traits<T>::Type;
  using ValueArg = typename Traits<T>::ArgumentType;

  PagedArray() : impl_() {}
  ~PagedArray() {}

  // Return true iff "*this" is initialized.
  explicit operator bool() const {
    return bool(impl_);
  }

  // Create an array.
  void create(Storage *storage, uint32_t storage_node_id,
              uint64_t size, uint64_t page_size) {
    impl_.create(storage, storage_node_id, sizeof(Value), size, page_size);
  }
  // Create an array and fill it with "value".
  void create(Storage *storage, uint32_t storage_node_id,
              uint64_t size, uint64_t page_size, ValueArg default_value) {
    impl_.create(storage, storage_node_id, sizeof(Value), size, page_size,
                 &default_value, fill_page);
  }
  // Open an array.
  void open(Storage *storage, uint32_t storage_node_id) {
    impl_.open(storage, storage_node_id, sizeof(Value));
  }

  // Unlink an array.
  static bool unlink(Storage *storage, uint32_t storage_node_id) {
    return PagedArrayImpl::unlink(storage, storage_node_id, sizeof(Value));
  }

  // Return the storage node ID.
  uint32_t storage_node_id() const {
    return impl_.storage_node_id();
  }
  // Return the size.
  uint64_t size() const {
    return impl_.size();
  }
  // Return the page size.
  uint64_t page_size() const {
    return impl_.page_size();
  }

  // Return a reference to a value.
  Value &operator[](uint64_t value_id) {
    return get_reference(value_id);
  }

  // Return a value.
  Value get(uint64_t value_id) {
    return get_reference(value_id);
  }
  // Set a value.
  void set(uint64_t value_id, ValueArg value) {
    get_reference(value_id) = value;
  }

 private:
  PagedArrayImpl impl_;

  // Return a reference to a value.
  Value &get_reference(uint64_t value_id) {
    return impl_.get_reference<Value>(value_id);
  }

  // This function is used to fill a new page with the default value.
  static void fill_page(void *page, const void *value, uint64_t page_size) {
    for (uint64_t i = 0; i < page_size; ++i) {
      static_cast<Value *>(page)[i] = *static_cast<const Value *>(value);
    }
  }
};

}  // namespace alpha
}  // namespace grnxx

#endif  // GRNXX_PAGED_ARRAY_HPP
