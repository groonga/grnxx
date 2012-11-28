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
#ifndef GRNXX_IO_POOL_HPP
#define GRNXX_IO_POOL_HPP

#include "../mutex.hpp"
#include "../recycler.hpp"
#include "chunk.hpp"

namespace grnxx {
namespace io {

const uint64_t POOL_MAX_FILE_SIZE = CHUNK_MAX_OFFSET;
const uint16_t POOL_MAX_NUM_FILES = 1000;

const uint32_t POOL_MAX_NUM_BLOCKS            = BLOCK_MAX_ID + 1;
const uint16_t POOL_MAX_NUM_BLOCK_CHUNKS      = uint16_t(1) << 11;
const uint16_t POOL_MAX_NUM_BLOCK_INFO_CHUNKS =
    32 - (CHUNK_UNIT_SIZE_BITS - BLOCK_INFO_SIZE_BITS) + 1;

const uint8_t  POOL_MIN_BLOCK_INFO_CHUNK_SIZE_BITS =
    CHUNK_UNIT_SIZE_BITS - BLOCK_INFO_SIZE_BITS;
const uint64_t POOL_MIN_BLOCK_INFO_CHUNK_SIZE      =
    uint64_t(1) << POOL_MIN_BLOCK_INFO_CHUNK_SIZE_BITS;

// For PoolOptions.

const uint64_t POOL_DEFAULT_MAX_FILE_SIZE        = uint64_t(1) << 40;

const uint64_t POOL_DEFAULT_MIN_BLOCK_CHUNK_SIZE = uint64_t(1) << 22;

const double POOL_MAX_NEXT_BLOCK_CHUNK_SIZE_RATIO     = 1.0;
const double POOL_DEFAULT_NEXT_BLOCK_CHUNK_SIZE_RATIO = 1.0 / 64;

const Duration POOL_MAX_FROZEN_DURATION     = Duration::days(1);
const Duration POOL_DEFAULT_FROZEN_DURATION = Duration::minutes(10);

const uint32_t POOL_DEFAULT_UNFREEZE_COUNT_PER_OPERATION = 32;

// For PoolHeader.

const uint8_t  POOL_HEADER_CHUNK_SIZE_BITS = CHUNK_UNIT_SIZE_BITS;
const uint64_t POOL_HEADER_CHUNK_SIZE      = CHUNK_UNIT_SIZE;

const char POOL_HEADER_FORMAT_STRING[64]  = "grnxx::io::Pool";
const char POOL_HEADER_VERSION_STRING[64] = "0.0.0";

class PoolOptions {
 public:
  PoolOptions();

  void adjust();

  uint64_t max_block_size() const {
    return max_block_size_;
  }
  uint64_t min_block_chunk_size() const {
    return min_block_chunk_size_;
  }
  uint64_t max_block_chunk_size() const {
    return max_block_chunk_size_;
  }
  uint64_t max_file_size() const {
    return max_file_size_;
  }
  double next_block_chunk_size_ratio() const {
    return next_block_chunk_size_ratio_;
  }
  Duration frozen_duration() const {
    return frozen_duration_;
  }
  uint32_t unfreeze_count_per_operation() const {
    return unfreeze_count_per_operation_;
  }

  void set_max_block_size(uint64_t value) {
    max_block_size_ = value;
  }
  void set_min_block_chunk_size(uint64_t value) {
    min_block_chunk_size_ = value;
  }
  void set_max_block_chunk_size(uint64_t value) {
    max_block_chunk_size_ = value;
  }
  void set_max_file_size(uint64_t value) {
    max_file_size_ = value;
  }
  void set_next_block_chunk_size_ratio(double value) {
    next_block_chunk_size_ratio_ = value;
  }
  void set_frozen_duration(Duration value) {
    frozen_duration_ = value;
  }
  void set_unfreeze_count_per_operation(uint32_t value) {
    unfreeze_count_per_operation_ = value;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  uint64_t max_block_size_;
  uint64_t min_block_chunk_size_;
  uint64_t max_block_chunk_size_;
  uint64_t max_file_size_;
  // The ratio of the next block size to the total size.
  double next_block_chunk_size_ratio_;
  Duration frozen_duration_;
  uint32_t unfreeze_count_per_operation_;
};

inline StringBuilder &operator<<(StringBuilder &builder,
                                 const PoolOptions &options) {
  return options.write_to(builder);
}

class PoolHeader {
 public:
  explicit PoolHeader(const PoolOptions &options = PoolOptions());

  const char *format_string() const {
    return format_string_;
  }
  const char *version_string() const {
    return format_string_;
  }
  const PoolOptions &options() const {
    return options_;
  }

  uint64_t total_size() const {
    return total_size_;
  }
  uint32_t num_blocks() const {
    return num_blocks_;
  };
  uint32_t max_num_blocks() const {
    return max_num_blocks_;
  }
  uint16_t next_block_chunk_id() const {
    return next_block_chunk_id_;
  }
  uint32_t latest_phantom_block_id() const {
    return latest_phantom_block_id_;
  }
  uint32_t latest_frozen_block_id() const {
    return latest_frozen_block_id_;
  }
  uint32_t oldest_idle_block_ids(uint8_t list_id) const {
    return oldest_idle_block_ids_[list_id];
  }
  const ChunkInfo &block_chunk_infos(uint16_t chunk_id) const {
    return block_chunk_infos_[chunk_id];
  }
  const ChunkInfo &block_info_chunk_infos(uint16_t chunk_id) const {
    return block_info_chunk_infos_[chunk_id];
  }

  void set_total_size(uint64_t value) {
    total_size_ = value;
  }
  void set_num_blocks(uint32_t value) {
    num_blocks_ = value;
  };
  void set_max_num_blocks(uint32_t value) {
    max_num_blocks_ = value;
  }
  void set_next_block_chunk_id(uint16_t value) {
    next_block_chunk_id_ = value;
  }
  void set_latest_phantom_block_id(uint32_t value) {
    latest_phantom_block_id_ = value;
  }
  void set_latest_frozen_block_id(uint32_t value) {
    latest_frozen_block_id_ = value;
  }
  void set_oldest_idle_block_ids(uint8_t list_id, uint32_t value) {
    oldest_idle_block_ids_[list_id] = value;
  }
  void set_block_chunk_infos(const ChunkInfo &value) {
    block_chunk_infos_[value.id()] = value;
  }
  void set_block_info_chunk_infos(const ChunkInfo & value) {
    block_info_chunk_infos_[value.id()] = value;
  }

  Recycler *mutable_recycler() {
    return &recycler_;
  }
  Mutex *mutable_inter_process_data_mutex() {
    return &inter_process_data_mutex_;
  }
  Mutex *mutable_inter_process_file_mutex() {
    return &inter_process_file_mutex_;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  char format_string_[sizeof(POOL_HEADER_FORMAT_STRING)];
  char version_string_[sizeof(POOL_HEADER_VERSION_STRING)];
  PoolOptions options_;
  uint64_t total_size_;
  uint32_t num_blocks_;
  uint32_t max_num_blocks_;
  uint16_t next_block_chunk_id_;
  uint32_t latest_phantom_block_id_;
  uint32_t latest_frozen_block_id_;
  uint32_t oldest_idle_block_ids_[32];
  ChunkInfo block_chunk_infos_[POOL_MAX_NUM_BLOCK_CHUNKS];
  ChunkInfo block_info_chunk_infos_[POOL_MAX_NUM_BLOCK_INFO_CHUNKS];
  Recycler recycler_;
  Mutex inter_process_data_mutex_;
  Mutex inter_process_file_mutex_;
};

static_assert(sizeof(PoolHeader) <= POOL_HEADER_CHUNK_SIZE,
              "sizeof(PoolHeader) > POOL_HEADER_CHUNK_SIZE");

inline StringBuilder &operator<<(StringBuilder &builder,
                                 const PoolHeader &header) {
  return header.write_to(builder);
}

class PoolImpl;

class Pool {
 public:
  Pool();
  // Available flags are as follows:
  //  GRNXX_IO_READ_ONLY, GRNXX_IO_ANONYMOUS, GRNXX_IO_CREATE,
  //  GRNXX_IO_OPEN, GRNXX_IO_TEMPORARY.
  Pool(const char *path, Flags flags = Flags(),
       const PoolOptions &options = PoolOptions());
  ~Pool();

  Pool(const Pool &pool);
  Pool &operator=(const Pool &pool);

  Pool(Pool &&pool);
  Pool &operator=(Pool &&pool);

  GRNXX_EXPLICIT_CONVERSION operator bool() const {
    return static_cast<bool>(impl_);
  }

  bool operator==(const Pool &rhs) const {
    return impl_ == rhs.impl_;
  }
  bool operator!=(const Pool &rhs) const {
    return impl_ != rhs.impl_;
  }

  const BlockInfo *create_block(uint64_t size);

  const BlockInfo *get_block_info(uint32_t block_id);

  void *get_block_address(uint32_t block_id);
  void *get_block_address(const BlockInfo &block_info);

  void free_block(uint32_t block_id);
  void free_block(const BlockInfo &block_info);

  String path() const;
  Flags flags() const;
  const PoolOptions &options() const;
  const PoolHeader &header() const;
  Recycler *mutable_recycler();

  void swap(Pool &rhs);

  StringBuilder &write_to(StringBuilder &builder) const;

  static bool exists(const char *path);
  static void unlink(const char *path);
  static bool unlink_if_exists(const char *path);

 private:
  std::shared_ptr<PoolImpl> impl_;
};

inline void swap(Pool &lhs, Pool &rhs) {
  lhs.swap(rhs);
}

inline StringBuilder &operator<<(StringBuilder &builder, const Pool &pool) {
  return pool.write_to(builder);
}

}  // namespace io
}  // namespace grnxx

#endif  // GRNXX_IO_POOL_HPP
