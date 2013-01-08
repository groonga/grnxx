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
#include "vector.hpp"

#include <vector>

#include "../exception.hpp"
#include "../lock.hpp"
#include "../logger.hpp"

namespace grnxx {
namespace db {

VectorCreate VECTOR_CREATE;
VectorOpen VECTOR_OPEN;

VectorHeader::VectorHeader(const void *default_value,
                           uint64_t value_size,
                           uint64_t page_size,
                           uint64_t table_size,
                           uint64_t secondary_table_size)
  : value_size_(value_size),
    page_size_(page_size),
    table_size_(table_size),
    secondary_table_size_(secondary_table_size),
    has_default_value_(default_value ? 1 : 0),
    first_table_block_id_(io::BLOCK_INVALID_ID),
    secondary_table_block_id_(io::BLOCK_INVALID_ID),
    inter_process_mutex_(MUTEX_UNLOCKED) {}

StringBuilder &VectorHeader::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  builder << "{ value_size = " << value_size_
          << ", page_size = " << page_size_
          << ", table_size = " << table_size_
          << ", secondary_table_size = " << secondary_table_size_
          << ", has_default_value = " << has_default_value_
          << ", first_table_block_id = " << first_table_block_id_
          << ", secondary_table_block_id = " << secondary_table_block_id_
          << ", inter_process_mutex = " << inter_process_mutex_;
  return builder << " }";
}

std::unique_ptr<VectorImpl> VectorImpl::create(io::Pool pool,
                                               const void *default_value,
                                               uint64_t value_size,
                                               uint64_t page_size,
                                               uint64_t table_size,
                                               uint64_t secondary_table_size,
                                               FillPage fill_page) {
  std::unique_ptr<VectorImpl> vector(new (std::nothrow) VectorImpl);
  if (!vector) {
    GRNXX_ERROR() << "new grnxx::db::VectorImpl failed";
    GRNXX_THROW();
  }
  vector->create_vector(pool, default_value, value_size, page_size, table_size,
                        secondary_table_size, fill_page);
  return vector;
}

std::unique_ptr<VectorImpl> VectorImpl::open(io::Pool pool,
                                             uint32_t block_id,
                                             uint64_t value_size,
                                             uint64_t page_size,
                                             uint64_t table_size,
                                             uint64_t secondary_table_size,
                                             FillPage fill_page) {
  std::unique_ptr<VectorImpl> vector(new (std::nothrow) VectorImpl);
  if (!vector) {
    GRNXX_ERROR() << "new grnxx::db::VectorImpl failed";
    GRNXX_THROW();
  }
  vector->open_vector(pool, block_id, value_size, page_size, table_size,
                      secondary_table_size, fill_page);
  return vector;
}

bool VectorImpl::scan_pages(bool (*callback)(uint64_t page_id,
                                             void *page_address,
                                             void *argument),
                            void *argument) {
  for (uint64_t page_id = 0; page_id < header_->table_size(); ++page_id) {
    if (!first_table_cache_[page_id]) {
      if (first_table_[page_id] == io::BLOCK_INVALID_ID) {
        continue;
      }
      first_table_cache_[page_id] =
          pool_.get_block_address(first_table_[page_id]);
    }
    if (!callback(page_id, first_table_cache_[page_id], argument)) {
      return false;
    }
  }

  if (header_->secondary_table_block_id() == io::BLOCK_INVALID_ID) {
    return true;
  }

  if (!tables_cache_) {
    if (!secondary_table_cache_) {
      if (!secondary_table_) {
        secondary_table_ = static_cast<uint32_t *>(
            pool_.get_block_address(header_->secondary_table_block_id()));
      }
      initialize_secondary_table_cache();
    }
    initialize_tables_cache();
  }

  for (uint64_t table_id = 0; table_id < header_->secondary_table_size();
       ++table_id) {
    std::unique_ptr<void *[]> &table_cache = tables_cache_[table_id];
    if (!table_cache) {
      if (secondary_table_[table_id] == io::BLOCK_INVALID_ID) {
        continue;
      }
      secondary_table_cache_[table_id] = static_cast<uint32_t *>(
          pool_.get_block_address(secondary_table_[table_id]));
      initialize_table_cache(&table_cache);
    }

    const uint64_t offset = table_id << table_size_bits_;
    for (uint64_t page_id = 0; page_id < header_->table_size(); ++page_id) {
      if (!table_cache[page_id]) {
        uint32_t * const table = secondary_table_cache_[table_id];
        if (table[page_id] == io::BLOCK_INVALID_ID) {
          continue;
        }
        table_cache[page_id] = pool_.get_block_address(table[page_id]);
      }
      if (!callback(offset + page_id, table_cache[page_id], argument)) {
        return false;
      }
    }
  }

  return true;
}

StringBuilder &VectorImpl::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  builder << "{ pool = " << pool_.path()
          << ", block_info = " << *block_info_
          << ", header = " << *header_
          << ", max_page_id = " << max_page_id_
          << ", inter_thread_mutex = " << inter_thread_mutex_;
  return builder << " }";
}

void VectorImpl::unlink(io::Pool pool,
                        uint32_t block_id,
                        uint64_t value_size,
                        uint64_t page_size,
                        uint64_t table_size,
                        uint64_t secondary_table_size) try {
  std::vector<uint32_t> block_ids;

  {
    std::unique_ptr<VectorImpl> vector = VectorImpl::open(
        pool, block_id, value_size,
        page_size, table_size, secondary_table_size, nullptr);
    const VectorHeader * const header = vector->header_;

    block_ids.push_back(block_id);

    block_ids.push_back(header->first_table_block_id());
    for (uint64_t i = 0; i < header->table_size(); ++i) {
      if (vector->first_table_[i] != io::BLOCK_INVALID_ID) {
        block_ids.push_back(vector->first_table_[i]);
      }
    }

    if (header->secondary_table_block_id() != io::BLOCK_INVALID_ID) {
      block_ids.push_back(header->secondary_table_block_id());
      uint32_t * const secondary_table = static_cast<uint32_t *>(
          pool.get_block_address(header->secondary_table_block_id()));
      for (uint64_t i = 0; i < header->secondary_table_size(); ++i) {
        if (secondary_table[i] != io::BLOCK_INVALID_ID) {
          block_ids.push_back(secondary_table[i]);
          uint32_t * const table = static_cast<uint32_t *>(
              pool.get_block_address(secondary_table[i]));
          for (uint64_t j = 0; j < header->table_size(); ++j) {
            if (table[j] != io::BLOCK_INVALID_ID) {
              block_ids.push_back(table[j]);
            }
          }
        }
      }
    }
  }

  for (size_t i = 0; i < block_ids.size(); ++i) {
    pool.free_block(block_ids[i]);
  }
} catch (const std::exception &exception) {
  GRNXX_ERROR() << exception;
  GRNXX_THROW();
}

VectorImpl::VectorImpl()
  : pool_(),
    fill_page_(nullptr),
    block_info_(nullptr),
    header_(nullptr),
    default_value_(nullptr),
    table_size_bits_(0),
    table_size_mask_(0),
    max_page_id_(0),
    first_table_(nullptr),
    secondary_table_(nullptr),
    secondary_table_cache_(),
    first_table_cache_(),
    tables_cache_(),
    inter_thread_mutex_(MUTEX_UNLOCKED) {}

void VectorImpl::create_vector(io::Pool pool,
                               const void *default_value,
                               uint64_t value_size,
                               uint64_t page_size,
                               uint64_t table_size,
                               uint64_t secondary_table_size,
                               FillPage fill_page) {
  pool_ = pool;

  std::unique_ptr<void *[]> first_table_cache(
      new (std::nothrow) void *[table_size]);
  if (!first_table_cache) {
    GRNXX_ERROR() << "new void *[" << table_size << "] failed";
    GRNXX_THROW();
  }

  uint64_t header_block_size = sizeof(VectorHeader);
  if (default_value) {
    header_block_size += value_size;
  }
  block_info_ = pool_.create_block(header_block_size);

  const io::BlockInfo *first_table_block_info;
  try {
    first_table_block_info = pool_.create_block(sizeof(uint32_t) * table_size);
  } catch (...) {
    pool_.free_block(*block_info_);
    throw;
  }

  void * const block_address = pool_.get_block_address(*block_info_);
  header_ = static_cast<VectorHeader *>(block_address);
  *header_ = VectorHeader(default_value, value_size, page_size, table_size,
                          secondary_table_size);
  restore_from_header();

  if (default_value_) {
    std::memcpy(default_value_, default_value,
                static_cast<size_t>(value_size));
    fill_page_ = fill_page;
  }

  header_->set_first_table_block_id(first_table_block_info->id());
  first_table_ = static_cast<uint32_t *>(
      pool_.get_block_address(*first_table_block_info));
  first_table_cache_ = std::move(first_table_cache);
  for (uint64_t i = 0; i < header_->table_size(); ++i) {
    first_table_[i] = io::BLOCK_INVALID_ID;
    first_table_cache_[i] = nullptr;
  }
}

void VectorImpl::open_vector(io::Pool pool,
                             uint32_t block_id,
                             uint64_t value_size,
                             uint64_t page_size,
                             uint64_t table_size,
                             uint64_t secondary_table_size,
                             FillPage fill_page) {
  pool_ = pool;

  block_info_ = pool_.get_block_info(block_id);
  if (block_info_->size() < sizeof(VectorHeader)) {
    GRNXX_ERROR() << "invalid argument: block_info = " << *block_info_
                  << ", header_size = " << sizeof(VectorHeader);
    GRNXX_THROW();
  }

  void * const block_address = pool_.get_block_address(*block_info_);
  header_ = static_cast<VectorHeader *>(block_address);
  restore_from_header();

  if (default_value_) {
    const uint64_t header_size = sizeof(VectorHeader) + value_size;
    if (block_info_->size() < header_size) {
      GRNXX_ERROR() << "invalid argument: block_info = " << *block_info_
                    << ", header_size = " << header_size;
      GRNXX_THROW();
    }
    fill_page_ = fill_page;
  }

  if (value_size != header_->value_size()) {
    GRNXX_ERROR() << "invalid value size: actual = " << header_->value_size()
                  << ", expected = " << value_size;
    GRNXX_THROW();
  }
  if (page_size != header_->page_size()) {
    GRNXX_ERROR() << "invalid page size: actual = " << header_->page_size()
                  << ", expected = " << page_size;
    GRNXX_THROW();
  }
  if (table_size != header_->table_size()) {
    GRNXX_ERROR() << "invalid table size: actual = " << header_->table_size()
                  << ", expected = " << table_size;
    GRNXX_THROW();
  }
  if (secondary_table_size != header_->secondary_table_size()) {
    GRNXX_ERROR() << "invalid secondary table size: actual = "
                  << header_->secondary_table_size()
                  << ", expected = " << secondary_table_size;
    GRNXX_THROW();
  }

  first_table_ = static_cast<uint32_t *>(
      pool_.get_block_address(header_->first_table_block_id()));

  first_table_cache_.reset(new (std::nothrow) void *[header_->table_size()]);
  if (!first_table_cache_) {
    GRNXX_ERROR() << "new void *[" << header_->table_size() << "] failed";
    GRNXX_THROW();
  }
  for (uint64_t i = 0; i < header_->table_size(); ++i) {
    first_table_cache_[i] = nullptr;
  }
}

void VectorImpl::restore_from_header() {
  if (header_->has_default_value()) {
    default_value_ = header_ + 1;
  }
  table_size_bits_ = bit_scan_reverse(header_->table_size());
  table_size_mask_ = header_->table_size() - 1;
  max_page_id_ = header_->table_size() * header_->secondary_table_size() - 1;
}

void *VectorImpl::get_page_address_on_failure(uint64_t page_id) {
  if (page_id < header_->table_size()) {
    if (!first_table_cache_[page_id]) {
      if (first_table_[page_id] == io::BLOCK_INVALID_ID) {
        initialize_page(&first_table_[page_id]);
      }
      first_table_cache_[page_id] =
          pool_.get_block_address(first_table_[page_id]);
    }
    return first_table_cache_[page_id];
  }

  if (page_id <= max_page_id_) {
    if (!tables_cache_) {
      if (!secondary_table_cache_) {
        if (!secondary_table_) {
          if (header_->secondary_table_block_id() == io::BLOCK_INVALID_ID) {
            initialize_secondary_table();
          }
          secondary_table_ = static_cast<uint32_t *>(
              pool_.get_block_address(header_->secondary_table_block_id()));
        }
        initialize_secondary_table_cache();
      }
      initialize_tables_cache();
    }

    const uint64_t table_id = page_id >> table_size_bits_;
    std::unique_ptr<void *[]> &table_cache = tables_cache_[table_id];
    if (!table_cache) {
      if (secondary_table_[table_id] == io::BLOCK_INVALID_ID) {
        initialize_table(&secondary_table_[table_id]);
      }
      secondary_table_cache_[table_id] = static_cast<uint32_t *>(
          pool_.get_block_address(secondary_table_[table_id]));
      initialize_table_cache(&table_cache);
    }

    page_id &= table_size_mask_;
    if (!table_cache[page_id]) {
      uint32_t * const table = secondary_table_cache_[table_id];
      if (table[page_id] == io::BLOCK_INVALID_ID) {
        initialize_page(&table[page_id]);
      }
      table_cache[page_id] = pool_.get_block_address(table[page_id]);
    }
    return table_cache[page_id];
  }

  GRNXX_ERROR() << "invalid argument: page_id = " << page_id
                << ": [0, " << max_page_id_ <<']';
  GRNXX_THROW();
}

void VectorImpl::initialize_secondary_table() {
  Lock lock(mutable_inter_process_mutex());
  if (header_->secondary_table_block_id() == io::BLOCK_INVALID_ID) {
    const auto block_info = pool_.create_block(
        sizeof(uint32_t) * header_->secondary_table_size());
    uint32_t * const body = static_cast<uint32_t *>(
        pool_.get_block_address(*block_info));
    for (uint64_t i = 0; i < header_->secondary_table_size(); ++i) {
      body[i] = io::BLOCK_INVALID_ID;
    }
    header_->set_secondary_table_block_id(block_info->id());
  }
}

void VectorImpl::initialize_table(uint32_t *table_block_id) {
  Lock lock(mutable_inter_process_mutex());
  if (*table_block_id == io::BLOCK_INVALID_ID) {
    const auto block_info = pool_.create_block(
        sizeof(uint32_t) * header_->table_size());
    uint32_t * const body = static_cast<uint32_t *>(
        pool_.get_block_address(*block_info));
    for (uint64_t i = 0; i < header_->table_size(); ++i) {
      body[i] = io::BLOCK_INVALID_ID;
    }
    *table_block_id = block_info->id();
  }
}

void VectorImpl::initialize_page(uint32_t *page_block_id) {
  Lock lock(mutable_inter_process_mutex());
  if (*page_block_id == io::BLOCK_INVALID_ID) {
    const io::BlockInfo *block_info = pool_.create_block(
        header_->value_size() * header_->page_size());
    if (fill_page_) {
      fill_page_(pool_.get_block_address(*block_info), default_value_);
    }
    *page_block_id = block_info->id();
  }
}

void VectorImpl::initialize_secondary_table_cache() {
  Lock lock(mutable_inter_thread_mutex());
  if (!secondary_table_cache_) {
    std::unique_ptr<uint32_t *[]> tables(
        new (std::nothrow) uint32_t *[header_->secondary_table_size()]);
    if (!tables) {
      GRNXX_ERROR() << "new grnxx::uint32_t *["
                    << header_->secondary_table_size() << "] failed";
      GRNXX_THROW();
    }
    for (uint64_t i = 0; i < header_->secondary_table_size(); ++i) {
      tables[i] = nullptr;
    }
    secondary_table_cache_ = std::move(tables);
  }
}

void VectorImpl::initialize_table_cache(
    std::unique_ptr<void *[]> *table_cache) {
  Lock lock(mutable_inter_thread_mutex());
  if (!*table_cache) {
    std::unique_ptr<void *[]> cache(
        new (std::nothrow) void *[header_->table_size()]);
    if (!cache) {
      GRNXX_ERROR() << "new void *[" << header_->table_size() << "] failed";
      GRNXX_THROW();
    }
    for (uint64_t i = 0; i < header_->table_size(); ++i) {
      cache[i] = nullptr;
    }
    *table_cache = std::move(cache);
  }
}

void VectorImpl::initialize_tables_cache() {
  Lock lock(mutable_inter_thread_mutex());
  if (!tables_cache_) {
    std::unique_ptr<std::unique_ptr<void *[]>[]> cache(new (std::nothrow)
        std::unique_ptr<void *[]>[header_->secondary_table_size()]);
    for (uint64_t i = 0; i < header_->secondary_table_size(); ++i) {
      cache[i] = nullptr;
    }
    tables_cache_ = std::move(cache);
  }
}

}  // namespace db
}  // namespace grnxx
