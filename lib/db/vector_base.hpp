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
#ifndef GRNXX_DB_VECTOR_BASE_HPP
#define GRNXX_DB_VECTOR_BASE_HPP

#include "../exception.hpp"
#include "../logger.hpp"
#include "../io/pool.hpp"

namespace grnxx {
namespace db {

class VectorHeader {
 public:
  void initialize(uint64_t value_size,
                  uint64_t page_size,
                  uint64_t table_size,
                  uint64_t secondary_table_size,
                  const void *default_value);

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

  Mutex *mutex() {
    return &mutex_;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  uint64_t value_size_;
  uint64_t page_size_;
  uint64_t table_size_;
  uint64_t secondary_table_size_;
  uint8_t has_default_value_;
  uint32_t first_table_block_id_;
  uint32_t secondary_table_block_id_;
  Mutex mutex_;

  VectorHeader();
  ~VectorHeader();

  VectorHeader(const VectorHeader &);
  VectorHeader &operator=(const VectorHeader &);
};

class VectorBase {
 public:
  typedef void (*FillPage)(void *page_address, const void *value);

  VectorBase();
  ~VectorBase();

  VectorBase(VectorBase &&rhs);
  VectorBase &operator=(VectorBase &&rhs);

  bool is_open() const {
    return static_cast<bool>(pool_);
  }

  void create(io::Pool *pool,
              uint64_t value_size,
              uint64_t page_size,
              uint64_t table_size,
              uint64_t secondary_table_size,
              const void *default_value,
              FillPage fill_page);

  void open(io::Pool *pool,
            uint32_t block_id,
            uint64_t value_size,
            uint64_t page_size,
            uint64_t table_size,
            uint64_t secondary_table_size,
            FillPage fill_page);

  template <typename T,
            uint64_t PAGE_SIZE,
            uint64_t TABLE_SIZE,
            uint64_t SECONDARY_TABLE_SIZE>
  T *get_value_address(uint64_t id) {
    void * const page_address =
       get_page_address<PAGE_SIZE, TABLE_SIZE, SECONDARY_TABLE_SIZE>(
           id / PAGE_SIZE);
    return static_cast<T *>(page_address) + (id % PAGE_SIZE);
  }

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

  uint32_t block_id() const {
    return is_open() ? block_info_->id() : io::BLOCK_INVALID_ID;
  }
  uint64_t value_size() const {
    return is_open() ? header_->value_size() : 0;
  }
  uint64_t page_size() const {
    return is_open() ? header_->page_size() : 0;
  }
  uint64_t table_size() const {
    return is_open() ? header_->table_size() : 0;
  }
  uint64_t secondary_table_size() const {
    return is_open() ? header_->secondary_table_size() : 0;
  }
  uint64_t id_max() const {
    return (page_size() * table_size() * secondary_table_size()) - 1;
  }

  void swap(VectorBase &rhs);

  StringBuilder &write_to(StringBuilder &builder) const;

  static void unlink(io::Pool *pool,
                     uint32_t block_id,
                     uint64_t value_size,
                     uint64_t page_size,
                     uint64_t table_size,
                     uint64_t secondary_table_size);

 private:
  io::Pool pool_;
  const io::BlockInfo *block_info_;
  VectorHeader *header_;
  void *default_value_;
  FillPage fill_page_;
  uint8_t table_size_bits_;
  uint64_t table_size_mask_;
  uint64_t page_id_max_;
  uint32_t *first_table_;
  uint32_t *secondary_table_;
  std::unique_ptr<uint32_t *[]> secondary_table_cache_;
  std::unique_ptr<void *[]> first_table_cache_;
  std::unique_ptr<std::unique_ptr<void *[]>[]> tables_cache_;
  Mutex mutex_;

  void create_vector(io::Pool *pool,
                     uint64_t value_size,
                     uint64_t page_size,
                     uint64_t table_size,
                     uint64_t secondary_table_size,
                     const void *default_value,
                     FillPage fill_page);
  void open_vector(io::Pool *pool,
                   uint32_t block_id,
                   uint64_t value_size,
                   uint64_t page_size,
                   uint64_t table_size,
                   uint64_t secondary_table_size,
                   FillPage fill_page);

  void restore_from_header();
  uint64_t header_size() const;

  void *get_page_address_on_failure(uint64_t page_id);

  void initialize_secondary_table();
  void initialize_table(uint32_t *table_block_id);
  void initialize_page(uint32_t *page_block_id);

  void initialize_secondary_table_cache();
  void initialize_table_cache(
      std::unique_ptr<void *[]> *table_cache);
  void initialize_tables_cache();

  Mutex *inter_thread_mutex() {
    return &mutex_;
  }
  Mutex *inter_process_mutex() {
    return header_->mutex();
  }

  VectorBase(const VectorBase &);
  VectorBase &operator=(const VectorBase &);
};

inline StringBuilder &operator<<(StringBuilder &builder,
                                 const VectorHeader &header) {
  return header.write_to(builder);
}
inline StringBuilder &operator<<(StringBuilder &builder,
                                 const VectorBase &vector) {
  return vector.write_to(builder);
}

}  // namespace db
}  // namespace grnxx

#endif  // GRNXX_DB_VECTOR_BASE_HPP
