/*
  Copyright (C) 2012  Brazil, Inc.

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
#ifndef GRNXX_DB_VECTOR_HPP
#define GRNXX_DB_VECTOR_HPP

#include "../io/pool.hpp"

namespace grnxx {
namespace db {

constexpr uint64_t VECTOR_MIN_PAGE_SIZE     = uint64_t(1) << 0;
constexpr uint64_t VECTOR_MAX_PAGE_SIZE     = uint64_t(1) << 20;
constexpr uint64_t VECTOR_DEFAULT_PAGE_SIZE = uint64_t(1) << 16;

constexpr uint64_t VECTOR_MIN_TABLE_SIZE     = uint64_t(1) << 10;
constexpr uint64_t VECTOR_MAX_TABLE_SIZE     = uint64_t(1) << 20;
constexpr uint64_t VECTOR_DEFAULT_TABLE_SIZE = uint64_t(1) << 12;

constexpr uint64_t VECTOR_MIN_SECONDARY_TABLE_SIZE     = uint64_t(1) << 10;
constexpr uint64_t VECTOR_MAX_SECONDARY_TABLE_SIZE     = uint64_t(1) << 20;
constexpr uint64_t VECTOR_DEFAULT_SECONDARY_TABLE_SIZE = uint64_t(1) << 12;

extern class VectorCreate {} VECTOR_CREATE;
extern class VectorOpen {} VECTOR_OPEN;

class VectorHeader {
 public:
  VectorHeader(const void *default_value,
               uint64_t value_size,
               uint64_t page_size,
               uint64_t table_size,
               uint64_t secondary_table_size);

  uint64_t value_size() const {
    return value_size_;
  }
  uint64_t page_size() const {
    return page_size_;
  }
  uint64_t table_size() const {
    return table_size_;
  }
  uint64_t secondary_table_size() const {
    return secondary_table_size_;
  }
  bool has_default_value() const {
    return has_default_value_ != 0;
  }
  uint32_t first_table_block_id() const {
    return first_table_block_id_;
  }
  uint32_t secondary_table_block_id() const {
    return secondary_table_block_id_;
  }

  void set_first_table_block_id(uint32_t value) {
    first_table_block_id_ = value;
  }
  void set_secondary_table_block_id(uint32_t value) {
    secondary_table_block_id_ = value;
  }

  Mutex *mutable_inter_process_mutex() {
    return &inter_process_mutex_;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  uint64_t value_size_;
  uint64_t page_size_;
  uint64_t table_size_;
  uint64_t secondary_table_size_;
  uint32_t has_default_value_;
  uint32_t first_table_block_id_;
  uint32_t secondary_table_block_id_;
  Mutex inter_process_mutex_;
};

inline StringBuilder &operator<<(StringBuilder &builder,
                                 const VectorHeader &header) {
  return header.write_to(builder);
}

class VectorImpl {
  typedef void (*FillPage)(void *page_address, const void *value);

 public:
  static std::unique_ptr<VectorImpl> create(io::Pool pool,
                                            const void *default_value,
                                            uint64_t value_size,
                                            uint64_t page_size,
                                            uint64_t table_size,
                                            uint64_t secondary_table_size,
                                            FillPage fill_page);
  static std::unique_ptr<VectorImpl> open(io::Pool pool,
                                          uint32_t block_id,
                                          uint64_t value_size,
                                          uint64_t page_size,
                                          uint64_t table_size,
                                          uint64_t secondary_table_size,
                                          FillPage fill_page);

  template <uint64_t PAGE_SIZE,
            uint64_t TABLE_SIZE,
            uint64_t SECONDARY_TABLE_SIZE>
  void *get_page_address(uint64_t page_id) {
    if ((page_id < TABLE_SIZE) && first_table_cache_[page_id]) {
      return first_table_cache_[page_id];
    }
    if ((page_id < (TABLE_SIZE * SECONDARY_TABLE_SIZE)) && tables_cache_) {
      const uint64_t table_id = page_id / TABLE_SIZE;
      const std::unique_ptr<void *[]> &table_cache = tables_cache_[table_id];
      if (table_cache) {
        const uint64_t local_page_id = page_id % TABLE_SIZE;
        if (table_cache[local_page_id]) {
          return table_cache[local_page_id];
        }
      }
    }
    return get_page_address_on_failure(page_id);
  }

  bool scan_pages(bool (*callback)(uint64_t page_id, void *page_address,
                                   void *argument),
                  void *argument);

  uint32_t block_id() const {
    return block_info_->id();
  }

  StringBuilder &write_to(StringBuilder &builder) const;

  static void unlink(io::Pool pool,
                     uint32_t block_id,
                     uint64_t value_size,
                     uint64_t page_size,
                     uint64_t table_size,
                     uint64_t secondary_table_size);

 private:
  io::Pool pool_;
  FillPage fill_page_;
  const io::BlockInfo *block_info_;
  VectorHeader *header_;
  void *default_value_;
  uint8_t table_size_bits_;
  uint64_t table_size_mask_;
  uint64_t max_page_id_;
  uint32_t *first_table_;
  uint32_t *secondary_table_;
  std::unique_ptr<uint32_t *[]> secondary_table_cache_;
  std::unique_ptr<void *[]> first_table_cache_;
  std::unique_ptr<std::unique_ptr<void *[]>[]> tables_cache_;
  Mutex inter_thread_mutex_;

  VectorImpl();

  void create_vector(io::Pool pool,
                     const void *default_value,
                     uint64_t value_size,
                     uint64_t page_size,
                     uint64_t table_size,
                     uint64_t secondary_table_size,
                     FillPage fill_page);
  void open_vector(io::Pool pool,
                   uint32_t block_id,
                   uint64_t value_size,
                   uint64_t page_size,
                   uint64_t table_size,
                   uint64_t secondary_table_size,
                   FillPage fill_page);
  void restore_from_header();

  void *get_page_address_on_failure(uint64_t page_id);

  void initialize_secondary_table();
  void initialize_table(uint32_t *table_block_id);
  void initialize_page(uint32_t *page_block_id);
  void initialize_secondary_table_cache();
  void initialize_table_cache(std::unique_ptr<void *[]> *table_cache);
  void initialize_tables_cache();

  Mutex *mutable_inter_process_mutex() {
    return header_->mutable_inter_process_mutex();
  }
  Mutex *mutable_inter_thread_mutex() {
    return &inter_thread_mutex_;
  }
};

inline StringBuilder &operator<<(StringBuilder &builder,
                                 const VectorImpl &vector) {
  return vector.write_to(builder);
}

template <typename T,
          uint64_t PAGE_SIZE = VECTOR_DEFAULT_PAGE_SIZE,
          uint64_t TABLE_SIZE = VECTOR_DEFAULT_TABLE_SIZE,
          uint64_t SECONDARY_TABLE_SIZE = VECTOR_DEFAULT_SECONDARY_TABLE_SIZE>
class Vector {
  // Static assertions to reject invalid template parameters.
  static_assert(PAGE_SIZE >= VECTOR_MIN_PAGE_SIZE, "too small PAGE_SIZE");
  static_assert(PAGE_SIZE <= VECTOR_MAX_PAGE_SIZE, "too large PAGE_SIZE");
  static_assert((PAGE_SIZE & (PAGE_SIZE - 1)) == 0,
                "PAGE_SIZE must be a power of two");

  static_assert(TABLE_SIZE >= VECTOR_MIN_TABLE_SIZE, "too small TABLE_SIZE");
  static_assert(TABLE_SIZE <= VECTOR_MAX_TABLE_SIZE, "too large TABLE_SIZE");
  static_assert((TABLE_SIZE & (TABLE_SIZE - 1)) == 0,
                "TABLE_SIZE must be a power of two");

  static_assert(SECONDARY_TABLE_SIZE >= VECTOR_MIN_SECONDARY_TABLE_SIZE,
                "too small SECONDARY_TABLE_SIZE");
  static_assert(SECONDARY_TABLE_SIZE <= VECTOR_MAX_SECONDARY_TABLE_SIZE,
                "too large SECONDARY_TABLE_SIZE");
  static_assert((SECONDARY_TABLE_SIZE & (SECONDARY_TABLE_SIZE - 1)) == 0,
                "SECONDARY_TABLE_SIZE must be a power of two");

 public:
  typedef T Value;

  // VECTOR_CREATE is available as an instance of VectorCreate.
  // VECTOR_OPEN is available as an instance of VectorOpen.
  Vector() = default;
  Vector(const VectorCreate &, io::Pool pool)
    : impl_(VectorImpl::create(pool, nullptr, sizeof(Value), PAGE_SIZE,
                               TABLE_SIZE, SECONDARY_TABLE_SIZE, fill_page)) {}
  Vector(const VectorCreate &, io::Pool pool, const Value &default_value)
    : impl_(VectorImpl::create(pool, &default_value, sizeof(Value), PAGE_SIZE,
                               TABLE_SIZE, SECONDARY_TABLE_SIZE, fill_page)) {}
  Vector(const VectorOpen &, io::Pool pool, uint32_t block_id)
    : impl_(VectorImpl::open(pool, block_id, sizeof(Value), PAGE_SIZE,
                             TABLE_SIZE, SECONDARY_TABLE_SIZE, fill_page)) {}

  void create(io::Pool pool) {
    *this = Vector(VECTOR_CREATE, pool);
  }
  void create(io::Pool pool, const Value &default_value) {
    *this = Vector(VECTOR_CREATE, pool, default_value);
  }
  void open(io::Pool pool, uint32_t block_id) {
    *this = Vector(VECTOR_OPEN, pool, block_id);
  }
  void close() {
    *this = Vector();
  }

  explicit operator bool() const {
    return static_cast<bool>(impl_);
  }

  // Access a value. Return a reference to a value.
  // This operator may throw an exception on failure.
  Value &operator[](uint64_t id) {
    void * const page_address =
       impl_->get_page_address<PAGE_SIZE, TABLE_SIZE,
                               SECONDARY_TABLE_SIZE>(id / PAGE_SIZE);
    return static_cast<T *>(page_address)[id % PAGE_SIZE];
  }

  // Scan values in sequential order and call the given callback for each
  // value. This function terminates if the callback returns false.
  // bool (*callback)(uint64_t id, Value *value);
  template <typename Callback>
  bool scan(Callback callback) {
    return impl_->scan_pages(
        [](uint64_t page_id, void *page_address, void *argument) -> bool {
      const uint64_t offset = page_id * PAGE_SIZE;
      for (uint64_t id = 0; id < PAGE_SIZE; ++id) {
        if (!(*static_cast<Callback *>(argument))(
            offset + id, &static_cast<Value *>(page_address)[id])) {
          return false;
        }
      }
      return true;
    }, &callback);
  }

  // The ID of the lead block.
  uint32_t block_id() const {
    return impl_->block_id();
  }

  void swap(Vector &rhs) {
    impl_.swap(rhs.impl_);
  }

  StringBuilder &write_to(StringBuilder &builder) const {
    return impl_ ? impl_->write_to(builder) : (builder << "n/a");
  }

  static constexpr uint64_t value_size() {
    return sizeof(Value);
  }
  static constexpr uint64_t page_size() {
    return PAGE_SIZE;
  }
  static constexpr uint64_t table_size() {
    return TABLE_SIZE;
  }
  static constexpr uint64_t secondary_table_size() {
    return SECONDARY_TABLE_SIZE;
  }
  static constexpr uint64_t max_id() {
    return (PAGE_SIZE * TABLE_SIZE * SECONDARY_TABLE_SIZE) - 1;
  }

  // Free blocks associated with a vector.
  static void unlink(io::Pool pool, uint32_t block_id) {
    VectorImpl::unlink(pool, block_id, sizeof(Value),
                       PAGE_SIZE, TABLE_SIZE, SECONDARY_TABLE_SIZE);
  }

 private:
  std::shared_ptr<VectorImpl> impl_;

  // This function is used to fill a new page with default values.
  static void fill_page(void *page_address, const void *value) {
    Value *values = static_cast<Value *>(page_address);
    for (uint64_t i = 0; i < PAGE_SIZE; ++i) {
      std::memcpy(&values[i], value, sizeof(Value));
    }
  }
};

template <typename T, uint64_t PAGE_SIZE, uint64_t TABLE_SIZE,
                      uint64_t SECONDARY_TABLE_SIZE>
inline void swap(Vector<T, PAGE_SIZE, TABLE_SIZE, SECONDARY_TABLE_SIZE> &lhs,
                 Vector<T, PAGE_SIZE, TABLE_SIZE, SECONDARY_TABLE_SIZE> &rhs) {
  lhs.swap(rhs);
}

template <typename T, uint64_t PAGE_SIZE, uint64_t TABLE_SIZE,
                      uint64_t SECONDARY_TABLE_SIZE>
inline StringBuilder &operator<<(StringBuilder &builder,
    const Vector<T, PAGE_SIZE, TABLE_SIZE, SECONDARY_TABLE_SIZE> &vector) {
  return vector.write_to(builder);
}

}  // namespace db
}  // namespace grnxx

#endif  // GRNXX_DB_VECTOR_HPP
