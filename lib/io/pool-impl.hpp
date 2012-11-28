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
#ifndef GRNXX_IO_POOL_IMPL_HPP
#define GRNXX_IO_POOL_IMPL_HPP

#include "pool.hpp"

namespace grnxx {
namespace io {

class PoolImpl {
 public:
  ~PoolImpl();

  static std::unique_ptr<PoolImpl> open(
      const char *path, Flags flags,
      const PoolOptions &options = PoolOptions());

  BlockInfo *create_block(uint64_t size);

  BlockInfo *get_block_info(uint32_t block_id);

  void *get_block_address(uint32_t block_id) {
    return get_block_address(*get_block_info(block_id));
  }
  void *get_block_address(const BlockInfo &block_info) {
    if (!block_chunks_[block_info.chunk_id()]) {
      mmap_block_chunk(block_info.chunk_id());
    }
    return static_cast<char *>(block_chunks_[block_info.chunk_id()].address())
        + block_info.offset();
  }

  void free_block(uint32_t block_id) {
    free_block(get_block_info(block_id));
  }
  void free_block(const BlockInfo &block_info) {
    free_block(const_cast<BlockInfo *>(&block_info));
  }
  void free_block(BlockInfo *block_info);

  String path() const {
    return path_;
  }
  Flags flags() const {
    return flags_;
  }
  const PoolOptions &options() const {
    return header_->options();
  }
  const PoolHeader &header() const {
    return *header_;
  }
  Recycler *mutable_recycler() {
    return header_->mutable_recycler();
  }

  StringBuilder &write_to(StringBuilder &builder) const;

  static bool exists(const char *path);
  static void unlink(const char *path);
  static bool unlink_if_exists(const char *path);

 private:
  String path_;
  Flags flags_;
  PoolHeader *header_;
  File files_[POOL_MAX_NUM_FILES];
  Chunk header_chunk_;
  Chunk block_chunks_[POOL_MAX_NUM_BLOCK_CHUNKS];
  Chunk block_info_chunks_[POOL_MAX_NUM_BLOCK_INFO_CHUNKS];
  Mutex inter_thread_chunk_mutex_;

  PoolImpl();

  void open_anonymous_pool(Flags flags, const PoolOptions &options);
  void open_temporary_pool(const char *path, Flags flags,
                           const PoolOptions &options);
  void open_regular_pool(const char *path, Flags flags,
                         const PoolOptions &options);

  void setup_header(const PoolOptions &options);
  void check_header();

  void mmap_block_chunk(uint16_t chunk_id);
  void mmap_block_info_chunk(uint16_t chunk_id);
  View mmap_chunk(const ChunkInfo &chunk_info);

  Flags get_view_flags() const;

  String generate_path(uint16_t file_id) const;

  BlockInfo *create_phantom_block();
  BlockInfo *create_active_block(uint64_t size);
  BlockInfo *create_idle_block();

  void phantomize_block(BlockInfo *block_info);

  uint32_t unfreeze_oldest_frozen_blocks(uint32_t max_count);
  bool unfreeze_oldest_frozen_block();

  BlockInfo *find_idle_block(uint64_t size);
  BlockInfo *activate_idle_block(BlockInfo *block_info, uint64_t size);

  void merge_idle_blocks(BlockInfo *center_block_info);
  void merge_idle_blocks(BlockInfo *block_info, BlockInfo *next_block_info);

  void register_idle_block(BlockInfo *block_info);
  void unregister_idle_block(BlockInfo *block_info);

  Mutex *mutable_inter_process_data_mutex() {
    return header_->mutable_inter_process_data_mutex();
  }
  Mutex *mutable_inter_process_file_mutex() {
    return header_->mutable_inter_process_file_mutex();
  }
  Mutex *mutable_inter_thread_chunk_mutex() {
    return &inter_thread_chunk_mutex_;
  }

  PoolImpl(const PoolImpl &);
  PoolImpl &operator=(const PoolImpl &);
};

inline StringBuilder &operator<<(StringBuilder &builder,
                                 const PoolImpl &pool) {
  return pool.write_to(builder);
}

}  // namespace io
}  // namespace grnxx

#endif  // GRNXX_IO_ALPHA_POOL_IMPL_HPP
