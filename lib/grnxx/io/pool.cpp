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
#include "grnxx/io/pool-impl.hpp"

#include <ostream>

#include "grnxx/exception.hpp"
#include "grnxx/logger.hpp"

namespace grnxx {
namespace io {

#define GRNXX_FLAGS_WRITE(flag) do { \
  if (flags & flag) { \
    if (!is_first) { \
      builder << " | "; \
    } \
    builder << #flag; \
    is_first = false; \
  } \
} while (false)

StringBuilder &operator<<(StringBuilder &builder, PoolFlags flags) {
  if (flags) {
    bool is_first = true;
    GRNXX_FLAGS_WRITE(POOL_READ_ONLY);
    GRNXX_FLAGS_WRITE(POOL_ANONYMOUS);
    GRNXX_FLAGS_WRITE(POOL_CREATE);
    GRNXX_FLAGS_WRITE(POOL_HUGE_TLB);
    GRNXX_FLAGS_WRITE(POOL_OPEN);
    GRNXX_FLAGS_WRITE(POOL_TEMPORARY);
    return builder;
  } else {
    return builder << "0";
  }
}

std::ostream &operator<<(std::ostream &stream, PoolFlags flags) {
  char buf[256];
  StringBuilder builder(buf);
  builder << flags;
  return stream.write(builder.c_str(), builder.length());
}

PoolOptions::PoolOptions()
  : max_block_size_(0),
    min_block_chunk_size_(0),
    max_block_chunk_size_(0),
    max_file_size_(0),
    next_block_chunk_size_ratio_(-1.0),
    frozen_duration_(-1),
    unfreeze_count_per_operation_(POOL_DEFAULT_UNFREEZE_COUNT_PER_OPERATION) {}

void PoolOptions::adjust() {
  if (max_file_size_ == 0) {
    max_file_size_ = POOL_DEFAULT_MAX_FILE_SIZE;
  } else {
    max_file_size_ = (max_file_size_ >> CHUNK_UNIT_SIZE_BITS)
        << CHUNK_UNIT_SIZE_BITS;
    if (max_file_size_ < CHUNK_UNIT_SIZE) {
      max_file_size_ = CHUNK_UNIT_SIZE;
    } else if (max_file_size_ > POOL_MAX_FILE_SIZE) {
      max_file_size_ = POOL_MAX_FILE_SIZE;
    }
  }

  if (max_block_chunk_size_ == 0) {
    max_block_chunk_size_ = max_file_size_;
  } else {
    max_block_chunk_size_ = (max_block_chunk_size_ >> CHUNK_UNIT_SIZE_BITS)
        << CHUNK_UNIT_SIZE_BITS;
    if (max_block_chunk_size_ < CHUNK_UNIT_SIZE) {
      max_block_chunk_size_ = CHUNK_UNIT_SIZE;
    } else if (max_block_chunk_size_ > CHUNK_MAX_SIZE) {
      max_block_chunk_size_ = CHUNK_MAX_SIZE;
    }
  }
  if (max_block_chunk_size_ > max_file_size_) {
    max_block_chunk_size_ = max_file_size_;
  }

  if (min_block_chunk_size_ == 0) {
    min_block_chunk_size_ = POOL_DEFAULT_MIN_BLOCK_CHUNK_SIZE;
  } else {
    min_block_chunk_size_ = (min_block_chunk_size_ >> CHUNK_UNIT_SIZE_BITS)
        << CHUNK_UNIT_SIZE_BITS;
    if (min_block_chunk_size_ < CHUNK_UNIT_SIZE) {
      min_block_chunk_size_ = CHUNK_UNIT_SIZE;
    } else if (min_block_chunk_size_ > CHUNK_MAX_SIZE) {
      min_block_chunk_size_ = CHUNK_MAX_SIZE;
    }
  }
  if (min_block_chunk_size_ > max_block_chunk_size_) {
    min_block_chunk_size_ = max_block_chunk_size_;
  }

  if (max_block_size_ == 0) {
    max_block_size_ = max_block_chunk_size_;
  } else {
    max_block_size_ = (max_block_size_ >> BLOCK_UNIT_SIZE_BITS)
        << BLOCK_UNIT_SIZE_BITS;
    if (max_block_size_ < BLOCK_UNIT_SIZE) {
      max_block_size_ = BLOCK_UNIT_SIZE;
    } else if (max_block_size_ > BLOCK_MAX_SIZE) {
      max_block_size_ = BLOCK_MAX_SIZE;
    }
  }
  if (max_block_size_ > max_block_chunk_size_) {
    max_block_size_ = max_block_chunk_size_;
  }

  if (next_block_chunk_size_ratio_ < 0.0) {
    next_block_chunk_size_ratio_ = POOL_DEFAULT_NEXT_BLOCK_CHUNK_SIZE_RATIO;
  } else if (next_block_chunk_size_ratio_ >
             POOL_MAX_NEXT_BLOCK_CHUNK_SIZE_RATIO) {
    next_block_chunk_size_ratio_ = POOL_MAX_NEXT_BLOCK_CHUNK_SIZE_RATIO;
  }

  if (frozen_duration_ < Duration(0)) {
    frozen_duration_ = POOL_DEFAULT_FROZEN_DURATION;
  } else if (frozen_duration_ > POOL_MAX_FROZEN_DURATION) {
    frozen_duration_ = POOL_MAX_FROZEN_DURATION;
  }
}

StringBuilder &PoolOptions::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  builder << "{ max_block_size = " << max_block_size()
          << ", min_block_chunk_size = " << min_block_chunk_size()
          << ", max_block_chunk_size = " << max_block_chunk_size()
          << ", max_file_size = " << max_file_size()
          << ", next_block_chunk_size_ratio = "
          << next_block_chunk_size_ratio()
          << ", frozen_duration = " << frozen_duration()
          << ", unfreeze_count_per_operation = "
          << unfreeze_count_per_operation();
  return builder << " }";
}

PoolHeader::PoolHeader(const PoolOptions &options)
  : format_string_(),
    version_string_(),
    options_(options),
    total_size_(POOL_HEADER_CHUNK_SIZE),
    num_blocks_(0),
    max_num_blocks_(0),
    next_block_chunk_id_(0),
    latest_phantom_block_id_(BLOCK_INVALID_ID),
    latest_frozen_block_id_(BLOCK_INVALID_ID),
    oldest_idle_block_ids_(),
    block_chunk_infos_(),
    block_info_chunk_infos_(),
    recycler_(),
    inter_process_data_mutex_(MUTEX_UNLOCKED),
    inter_process_file_mutex_(MUTEX_UNLOCKED) {
  std::memcpy(format_string_, POOL_HEADER_FORMAT_STRING,
              sizeof(format_string_));
  std::memcpy(version_string_, POOL_HEADER_VERSION_STRING,
              sizeof(version_string_));

  options_.adjust();

  for (uint8_t list_id = 0; list_id < 32; ++list_id) {
    oldest_idle_block_ids_[list_id] = BLOCK_INVALID_ID;
  }
  for (uint16_t chunk_id = 0; chunk_id < POOL_MAX_NUM_BLOCK_CHUNKS;
       ++chunk_id) {
    block_chunk_infos_[chunk_id].set_id(chunk_id);
  }
  for (uint16_t chunk_id = 0; chunk_id < POOL_MAX_NUM_BLOCK_INFO_CHUNKS;
       ++chunk_id) {
    block_info_chunk_infos_[chunk_id].set_id(chunk_id);
  }

  recycler_ = Recycler(options_.frozen_duration());
}

StringBuilder &PoolHeader::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  builder << "{ format_string = " << format_string()
          << ", version_string = " << version_string()
          << ", options = " << options()
          << ", total_size = " << total_size()
          << ", num_blocks = " << num_blocks()
          << ", max_num_blocks = " << max_num_blocks()
          << ", next_block_chunk_id = " << next_block_chunk_id()
          << ", latest_phantom_block_id = " << latest_phantom_block_id()
          << ", latest_frozen_block_id = " << latest_frozen_block_id();

  builder << ", oldest_idle_block_ids = ";
  bool is_empty = true;
  for (uint32_t i = 0; i < 32; ++i) {
    if (oldest_idle_block_ids_[i] != BLOCK_INVALID_ID) {
      if (is_empty) {
        builder << "{ ";
        is_empty = false;
      } else {
        builder << ", ";
      }
      builder << '[' << i << "] = " << oldest_idle_block_ids_[i];
    }
  }
  builder << (is_empty ? "{}" : " }");

  builder << ", block_chunk_infos = ";
  is_empty = true;
  for (uint32_t i = 0; i < POOL_MAX_NUM_BLOCK_CHUNKS; ++i) {
    if (block_chunk_infos_[i]) {
      if (is_empty) {
        builder << "{ ";
        is_empty = false;
      } else {
        builder << ", ";
      }
      builder << '[' << i << "] = " << block_chunk_infos_[i];
    }
  }
  builder << (is_empty ? "{}" : " }");

  builder << ", block_info_chunk_infos = ";
  is_empty = true;
  for (uint32_t i = 0; i < POOL_MAX_NUM_BLOCK_INFO_CHUNKS; ++i) {
    if (block_info_chunk_infos_[i]) {
      if (is_empty) {
        builder << "{ ";
        is_empty = false;
      } else {
        builder << ", ";
      }
      builder << '[' << i << "] = " << block_info_chunk_infos_[i];
    }
  }
  builder << (is_empty ? "{}" : " }");

  builder << ", recycler = " << recycler_
          << ", inter_process_data_mutex = " << inter_process_data_mutex_
          << ", inter_process_file_mutex = " << inter_process_file_mutex_;
  return builder << " }";
}

Pool::Pool() : impl_() {}

Pool::Pool(PoolFlags flags, const char *path, const PoolOptions &options)
  : impl_(PoolImpl::open(flags, path, options)) {}

Pool::~Pool() {}

Pool::Pool(const Pool &pool) : impl_(pool.impl_) {}

Pool &Pool::operator=(const Pool &pool) {
  impl_ = pool.impl_;
  return *this;
}

Pool::Pool(Pool &&pool) : impl_(std::move(pool.impl_)) {}

Pool &Pool::operator=(Pool &&pool) {
  impl_ = std::move(pool.impl_);
  return *this;
}

#define POOL_POOL_THROW_IF_IMPL_IS_INVALID() do {\
  if (!impl_) {\
    GRNXX_ERROR() << "invalid instance: pool = " << *this;\
    GRNXX_THROW();\
  }\
} while (false)

const BlockInfo *Pool::create_block(uint64_t size) {
  POOL_POOL_THROW_IF_IMPL_IS_INVALID();
  return impl_->create_block(size);
}

const BlockInfo *Pool::get_block_info(uint32_t block_id) {
  POOL_POOL_THROW_IF_IMPL_IS_INVALID();
  return impl_->get_block_info(block_id);
}

void *Pool::get_block_address(uint32_t block_id) {
  POOL_POOL_THROW_IF_IMPL_IS_INVALID();
  return impl_->get_block_address(block_id);
}

void *Pool::get_block_address(const BlockInfo &block_info) {
  POOL_POOL_THROW_IF_IMPL_IS_INVALID();
  return impl_->get_block_address(block_info);
}

void Pool::free_block(uint32_t block_id) {
  POOL_POOL_THROW_IF_IMPL_IS_INVALID();
  return impl_->free_block(block_id);
}

void Pool::free_block(const BlockInfo &block_info) {
  POOL_POOL_THROW_IF_IMPL_IS_INVALID();
  return impl_->free_block(block_info);
}

String Pool::path() const {
  POOL_POOL_THROW_IF_IMPL_IS_INVALID();
  return impl_->path();
}

PoolFlags Pool::flags() const {
  POOL_POOL_THROW_IF_IMPL_IS_INVALID();
  return impl_->flags();
}

const PoolOptions &Pool::options() const {
  POOL_POOL_THROW_IF_IMPL_IS_INVALID();
  return impl_->options();
}

const PoolHeader &Pool::header() const {
  POOL_POOL_THROW_IF_IMPL_IS_INVALID();
  return impl_->header();
}

Recycler *Pool::mutable_recycler() {
  POOL_POOL_THROW_IF_IMPL_IS_INVALID();
  return impl_->mutable_recycler();
}

#undef POOL_POOL_THROW_IF_IMPL_IS_INVALID

void Pool::swap(Pool &rhs) {
  impl_.swap(rhs.impl_);
}

StringBuilder &Pool::write_to(StringBuilder &builder) const {
  return impl_ ? impl_->write_to(builder) : (builder << "n/a");
}

bool Pool::exists(const char *path) {
  return PoolImpl::exists(path);
}

void Pool::unlink(const char *path) {
  PoolImpl::unlink(path);
}

bool Pool::unlink_if_exists(const char *path) {
  return PoolImpl::unlink_if_exists(path);
}

}  // namespace io
}  // namespace grnxx
