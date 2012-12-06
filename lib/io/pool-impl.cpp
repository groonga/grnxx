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
#include "pool-impl.hpp"

#include <vector>

#include "../exception.hpp"
#include "../lock.hpp"
#include "../logger.hpp"
#include "../string_format.hpp"
#include "../thread.hpp"
#include "../time.hpp"
#include "path.hpp"

namespace grnxx {
namespace io {

PoolImpl::~PoolImpl() {}

std::unique_ptr<PoolImpl> PoolImpl::open(const char *path, Flags flags,
                                         const PoolOptions &options) {
  std::unique_ptr<PoolImpl> pool(new (std::nothrow) PoolImpl);
  if (!pool) {
    GRNXX_ERROR() << "new grnxx::io::PoolImpl failed";
    GRNXX_THROW();
  }

  if (flags & GRNXX_IO_ANONYMOUS) {
    pool->open_anonymous_pool(flags, options);
  } else if (flags & GRNXX_IO_TEMPORARY) {
    pool->open_temporary_pool(path, flags, options);
  } else {
    pool->open_regular_pool(path, flags, options);
  }

  return pool;
}

BlockInfo *PoolImpl::create_block(uint64_t size) {
  if (flags_ & GRNXX_IO_READ_ONLY) {
    GRNXX_ERROR() << "invalid operation: flags = " << flags_;
    GRNXX_THROW();
  }

  if (size > options().max_block_size()) {
    GRNXX_ERROR() << "invalid argument: size = " << size
                  << ", max_block_size = " << options().max_block_size();
    GRNXX_THROW();
  }

  // A block size must be a multiple of BLOCK_UNIT_SIZE.
  if (size == 0) {
    size = BLOCK_UNIT_SIZE;
  } else {
    size = (size + (BLOCK_UNIT_SIZE - 1)) & ~(BLOCK_UNIT_SIZE - 1);
  }

  Lock lock(mutable_inter_process_data_mutex());
  if (!lock) {
    GRNXX_ERROR() << "failed to lock data";
    GRNXX_THROW();
  }

  unfreeze_oldest_frozen_blocks(options().unfreeze_count_per_operation());

  BlockInfo * const block_info = find_idle_block(size);
  if (block_info) {
    return activate_idle_block(block_info, size);
  } else {
    return create_active_block(size);
  }
}

BlockInfo *PoolImpl::get_block_info(uint32_t block_id) {
  if (block_id >= header_->num_blocks()) {
    GRNXX_ERROR() << "invalid argument: block_id = " << block_id
                  << ", num_blocks = " << header_->num_blocks();
    GRNXX_THROW();
  }

  const uint8_t block_info_chunk_size_bits =
      bit_scan_reverse(block_id | POOL_MIN_BLOCK_INFO_CHUNK_SIZE);
  const uint16_t block_info_chunk_id = static_cast<uint16_t>(
      (block_id >> block_info_chunk_size_bits)
      + block_info_chunk_size_bits - POOL_MIN_BLOCK_INFO_CHUNK_SIZE_BITS);
  const uint32_t block_info_chunk_size =
      uint32_t(1) << block_info_chunk_size_bits;

  if (!block_info_chunks_[block_info_chunk_id]) {
    mmap_block_info_chunk(block_info_chunk_id);
  }

  const uint32_t offset_mask = block_info_chunk_size - 1;
  BlockInfo * const block_infos = static_cast<BlockInfo *>(
      block_info_chunks_[block_info_chunk_id].address());
  return &block_infos[block_id & offset_mask];
}

void PoolImpl::free_block(BlockInfo *block_info) {
  if (flags_ & GRNXX_IO_READ_ONLY) {
    GRNXX_ERROR() << "invalid operation: flags = " << flags_;
    GRNXX_THROW();
  }

  Lock lock(mutable_inter_process_data_mutex());
  if (!lock) {
    GRNXX_ERROR() << "failed to lock data";
    GRNXX_THROW();
  }

  unfreeze_oldest_frozen_blocks(options().unfreeze_count_per_operation());

  switch (block_info->status()) {
    case BLOCK_ACTIVE: {
      block_info->set_frozen_stamp(mutable_recycler()->stamp());
      block_info->set_status(BLOCK_FROZEN);
      if (header_->latest_frozen_block_id() != BLOCK_INVALID_ID) {
        BlockInfo * const latest_frozen_block_info =
            get_block_info(header_->latest_frozen_block_id());
        block_info->set_next_frozen_block_id(
            latest_frozen_block_info->next_frozen_block_id());
        latest_frozen_block_info->set_next_frozen_block_id(block_info->id());
      } else {
        block_info->set_next_frozen_block_id(block_info->id());
      }
      header_->set_latest_frozen_block_id(block_info->id());
      break;
    }
    case BLOCK_FROZEN: {
      break;
    }
    default: {
      GRNXX_ERROR() << "invalid argument: block_info = " << *block_info;
      GRNXX_THROW();
    }
  }
}

StringBuilder &PoolImpl::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  builder << "{ path = " << path_
          << ", flags = " << flags_
          << ", header = " << *header_;

  builder << ", oldest_idle_block_ids = ";
  bool is_empty = true;
  for (uint32_t i = 0; i < POOL_MAX_NUM_FILES; ++i) {
    if (files_[i]) {
      if (is_empty) {
        builder << "{ ";
        is_empty = false;
      } else {
        builder << ", ";
      }
      builder << '[' << i << "] = " << files_[i];
    }
  }
  builder << (is_empty ? "{}" : " }");

  builder << ", header_chunk = " << header_chunk_;

  builder << ", block_chunks = ";
  is_empty = true;
  for (uint32_t i = 0; i < POOL_MAX_NUM_BLOCK_CHUNKS; ++i) {
    if (block_chunks_[i]) {
      if (is_empty) {
        builder << "{ ";
        is_empty = false;
      } else {
        builder << ", ";
      }
      builder << i;
    }
  }
  builder << (is_empty ? "{}" : " }");

  builder << ", block_info_chunks = ";
  is_empty = true;
  for (uint32_t i = 0; i < POOL_MAX_NUM_BLOCK_INFO_CHUNKS; ++i) {
    if (block_info_chunks_[i]) {
      if (is_empty) {
        builder << "{ ";
        is_empty = false;
      } else {
        builder << ", ";
      }
      builder << i;
    }
  }
  builder << (is_empty ? "{}" : " }");

  return builder << " }";
}

bool PoolImpl::exists(const char *path) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = " << path;
    GRNXX_THROW();
  }
  // TODO: Check the file format.
  return File::exists(path);
}

void PoolImpl::unlink(const char *path) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = " << path;
    GRNXX_THROW();
  } else {
    // FIXME
    File file(FILE_OPEN, path);
    if (!file.try_lock(FILE_LOCK_EXCLUSIVE)) {
      GRNXX_ERROR() << "failed to lock file: path = " << path;
      GRNXX_THROW();
    }
  }

  std::vector<String> paths;
  try {
    std::unique_ptr<PoolImpl> pool = PoolImpl::open(path, GRNXX_IO_READ_ONLY);
    const PoolOptions &options = pool->options();
    const PoolHeader &header = pool->header();
    const uint16_t max_file_id = static_cast<uint16_t>(
        (header.total_size() - 1) / options.max_file_size());
    for (uint16_t file_id = 0; file_id <= max_file_id; ++file_id) {
      paths.push_back(pool->generate_path(file_id));
    }
  } catch (const std::exception &exception) {
    GRNXX_ERROR() << exception;
    GRNXX_THROW();
  }

  File::unlink(paths[0].c_str());
  for (std::size_t path_id = 1; path_id < paths.size(); ++path_id) {
    File::unlink_if_exists(paths[path_id].c_str());
  }
}

bool PoolImpl::unlink_if_exists(const char *path) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = " << path;
    GRNXX_THROW();
  }
  // TODO: Check the file format.
  if (exists(path)) {
    unlink(path);
    return true;
  }
  return false;
}

PoolImpl::PoolImpl()
  : path_(),
    flags_(Flags::none()),
    header_(nullptr),
    files_(),
    header_chunk_(),
    block_chunks_(),
    block_info_chunks_(),
    inter_thread_chunk_mutex_(MUTEX_UNLOCKED) {}

void PoolImpl::open_anonymous_pool(Flags flags, const PoolOptions &options) {
  flags_ = GRNXX_IO_ANONYMOUS;
  if (flags & GRNXX_IO_HUGE_TLB) {
    flags_ |= GRNXX_IO_HUGE_TLB;
  }
  setup_header(options);
}

void PoolImpl::open_temporary_pool(const char *path, Flags,
                                   const PoolOptions &options) {
  path_ = Path::full_path(path);
  flags_ = GRNXX_IO_TEMPORARY;
  files_[0].open(FILE_TEMPORARY, path_.c_str());
  setup_header(options);
}

void PoolImpl::open_regular_pool(const char *path, Flags flags,
                                 const PoolOptions &options) {
  if (!path) {
    GRNXX_ERROR() << "invalid argument: path = " << path;
    GRNXX_THROW();
  }
  path_ = Path::full_path(path);

  FileFlags file_flags = FileFlags::none();
  if ((~flags & GRNXX_IO_CREATE) && (flags & GRNXX_IO_READ_ONLY)) {
    flags_ |= GRNXX_IO_READ_ONLY;
    file_flags |= FILE_READ_ONLY;
  }
  if (flags & GRNXX_IO_CREATE) {
    flags_ |= GRNXX_IO_CREATE;
    file_flags |= FILE_CREATE;
  }
  if ((~flags & GRNXX_IO_CREATE) || flags & GRNXX_IO_OPEN) {
    flags_ |= GRNXX_IO_OPEN;
    file_flags |= FILE_OPEN;
  }
  files_[0].open(file_flags, path_.c_str());
  files_[0].set_unlink_at_close(true);

  if (flags & GRNXX_IO_CREATE) {
    if (files_[0].size() == 0) {
      if (files_[0].lock(FILE_LOCK_EXCLUSIVE, Duration::seconds(10))) {
        if (files_[0].size() == 0) {
          setup_header(options);
          files_[0].unlock();
          if (!files_[0].lock(FILE_LOCK_SHARED, Duration::seconds(10))) {
            GRNXX_ERROR() << "failed to lock file: path = " << path
                          << ", full_path = " << path_
                          << ", flags = " << flags;
            GRNXX_THROW();
          }
        } else {
          files_[0].unlock();
        }
      }
    }
  }

  if (!header_) {
    if ((flags & GRNXX_IO_OPEN) || (~flags & GRNXX_IO_CREATE)) {
      const Time start_time = Time::now();
      while ((Time::now() - start_time) < Duration::seconds(10)) {
        if (files_[0].size() != 0) {
          break;
        }
        Thread::sleep(Duration::milliseconds(10));
      }
    }
    if (files_[0].lock(FILE_LOCK_SHARED, Duration::seconds(10))) {
      check_header();
    }
  }

  if (!header_) {
    GRNXX_ERROR() << "failed to open pool: path = " << path
                  << ", full_path = " << path_ << ", flags = " << flags;
    GRNXX_THROW();
  }

  files_[0].set_unlink_at_close(false);
}

void PoolImpl::setup_header(const PoolOptions &options) {
  if (files_[0]) {
    files_[0].resize(POOL_HEADER_CHUNK_SIZE);
    header_chunk_ = View(files_[0], get_view_flags(),
                         0, POOL_HEADER_CHUNK_SIZE);
  } else {
    header_chunk_ = View(get_view_flags(), POOL_HEADER_CHUNK_SIZE);
  }
  std::memset(header_chunk_.address(), 0, POOL_HEADER_CHUNK_SIZE);
  header_ = static_cast<PoolHeader *>(header_chunk_.address());
  *header_ = PoolHeader(options);
}

void PoolImpl::check_header() {
  header_chunk_ = View(files_[0], get_view_flags(), 0, POOL_HEADER_CHUNK_SIZE);
  header_ = static_cast<PoolHeader *>(header_chunk_.address());

  // TODO: Check the header.
}

void PoolImpl::mmap_block_chunk(uint16_t chunk_id) {
  Lock lock(mutable_inter_thread_chunk_mutex());
  if (!lock) {
    GRNXX_ERROR() << "failed to lock chunks";
    GRNXX_THROW();
  }

  if (block_chunks_[chunk_id]) {
    return;
  }

  const ChunkInfo &chunk_info = header_->block_chunk_infos(chunk_id);
  if (!chunk_info) {
    GRNXX_ERROR() << "invalid argument: chunk_id = " << chunk_id;
    GRNXX_THROW();
  }

  block_chunks_[chunk_id] = mmap_chunk(chunk_info);
}

void PoolImpl::mmap_block_info_chunk(uint16_t chunk_id) {
  Lock lock(mutable_inter_thread_chunk_mutex());
  if (!lock) {
    GRNXX_ERROR() << "failed to lock chunks";
    GRNXX_THROW();
  }

  if (block_info_chunks_[chunk_id]) {
    return;
  }

  const ChunkInfo &chunk_info = header_->block_info_chunk_infos(chunk_id);
  if (!chunk_info) {
    GRNXX_ERROR() << "invalid argument: chunk_id = " << chunk_id;
    GRNXX_THROW();
  }

  block_info_chunks_[chunk_id] = mmap_chunk(chunk_info);
}

View PoolImpl::mmap_chunk(const ChunkInfo &chunk_info) {
  if (flags_ & GRNXX_IO_ANONYMOUS) {
    return View(get_view_flags(), chunk_info.size());
  } else {
    File &file = files_[chunk_info.file_id()];
    if (!file) {
      FileFlags file_flags = FILE_CREATE_OR_OPEN;
      if (flags_ & GRNXX_IO_TEMPORARY) {
        file_flags = FILE_TEMPORARY;
      } else if (flags_ & GRNXX_IO_READ_ONLY) {
        file_flags = FILE_READ_ONLY;
      }
      const String &path = generate_path(chunk_info.file_id());
      file.open(file_flags, path.c_str());
    }

    const uint64_t min_file_size = chunk_info.offset() + chunk_info.size();
    if (file.size() < min_file_size) {
      Lock lock(mutable_inter_process_file_mutex());
      if (!lock) {
        GRNXX_ERROR() << "failed to lock files";
        GRNXX_THROW();
      }
      if (file.size() < min_file_size) {
        file.resize(min_file_size);
      }
    }

    return View(file, get_view_flags(),
                chunk_info.offset(), chunk_info.size());
  }
}

Flags PoolImpl::get_view_flags() const {
  if (flags_ & GRNXX_IO_ANONYMOUS) {
    return (flags_ & GRNXX_IO_HUGE_TLB) ? GRNXX_IO_HUGE_TLB : Flags::none();
  } else {
    Flags view_flags = GRNXX_IO_SHARED;
    if (flags_ & GRNXX_IO_READ_ONLY) {
      view_flags |= GRNXX_IO_READ_ONLY;
    }
    return view_flags;
  }
}

String PoolImpl::generate_path(uint16_t file_id) const {
  if (file_id == 0) {
    return path_;
  }

  // If path_ ends with ".grn", the result also ends with ".grn".
  // In other words, file_id is inserted before the ".grn".
  // Otherwise, fild_id is appended as suffix.
  const size_t FILE_ID_WIDTH = 3;

  StringBuilder path(path_.length() + FILE_ID_WIDTH + 2);
  const bool has_extension = path_.ends_with(".grn");
  if (has_extension) {
    path.append(path_.c_str(), path_.length() - 4);
  } else {
    path.append(path_.c_str(), path_.length());
  }
  path << '_' << StringFormat::align_right(file_id, FILE_ID_WIDTH, '0');
  if (has_extension) {
    path.append(".grn", 4);
  }
  if (!path) {
    GRNXX_ERROR() << "failed to generate path: file_id = " << file_id;
    GRNXX_THROW();
  }
  return path.str();
}

BlockInfo *PoolImpl::create_phantom_block() {
  if (header_->num_blocks() >= POOL_MAX_NUM_BLOCKS) {
    GRNXX_ERROR() << "too many blocks: num_blocks = " << header_->num_blocks()
                  << ", max_num_blocks = " << POOL_MAX_NUM_BLOCKS;
    GRNXX_THROW();
  }

  const uint32_t block_id = header_->num_blocks();

  const uint8_t block_info_chunk_size_bits =
      bit_scan_reverse(block_id | POOL_MIN_BLOCK_INFO_CHUNK_SIZE);
  const uint16_t block_info_chunk_id = static_cast<uint16_t>(
      (block_id >> block_info_chunk_size_bits)
      + block_info_chunk_size_bits - POOL_MIN_BLOCK_INFO_CHUNK_SIZE_BITS);
  const uint32_t block_info_chunk_size =
      uint32_t(1) << block_info_chunk_size_bits;

  if (block_id == header_->max_num_blocks()) {
    const uint64_t block_info_chunk_size_in_bytes =
        BLOCK_INFO_SIZE << block_info_chunk_size_bits;
    if (block_info_chunk_size_in_bytes > options().max_file_size()) {
      GRNXX_ERROR() << "too large chunk: chunk_size = "
                    << block_info_chunk_size_in_bytes
                    << ", max_file_size = " << options().max_file_size();
      GRNXX_THROW();
    }

    const uint16_t file_id =
        header_->total_size() / options().max_file_size();
    const uint64_t file_size =
        header_->total_size() % options().max_file_size();
    const uint64_t file_size_left = options().max_file_size() - file_size;

    ChunkInfo chunk_info;
    chunk_info.set_id(block_info_chunk_id);
    if (file_size_left < block_info_chunk_size_in_bytes) {
      if (file_id >= POOL_MAX_NUM_FILES) {
        GRNXX_ERROR() << "too many files: fild_id = " << file_id
                      << ", max_num_files = " << POOL_MAX_NUM_FILES;
        GRNXX_THROW();
      }
      chunk_info.set_file_id(file_id + 1);
      chunk_info.set_offset(0);
    } else {
      chunk_info.set_file_id(file_id);
      chunk_info.set_offset(file_size);
    }
    chunk_info.set_size(block_info_chunk_size_in_bytes);
    header_->set_block_info_chunk_infos(chunk_info);

    header_->set_total_size((chunk_info.file_id() * options().max_file_size())
        + chunk_info.offset() + chunk_info.size());

    // Note: block_id == header_->max_num_blocks().
    const uint32_t num_blocks_left = POOL_MAX_NUM_BLOCKS - block_id;
    if (num_blocks_left > block_info_chunk_size) {
      header_->set_max_num_blocks(block_id + block_info_chunk_size);
    } else {
      header_->set_max_num_blocks(POOL_MAX_NUM_BLOCKS);
    }

    if (file_id != chunk_info.file_id()) {
      if (header_->next_block_chunk_id() >= POOL_MAX_NUM_BLOCK_CHUNKS) {
        GRNXX_ERROR() << "too many block chunks: next_block_chunk_id = "
                      << header_->next_block_chunk_id()
                      << ", max_num_block_chunks = "
                      << POOL_MAX_NUM_BLOCK_CHUNKS;
        GRNXX_THROW();
      }

      ChunkInfo idle_chunk_info;
      idle_chunk_info.set_id(header_->next_block_chunk_id());
      idle_chunk_info.set_file_id(file_id);
      idle_chunk_info.set_offset(file_size);
      idle_chunk_info.set_size(file_size_left);
      header_->set_block_chunk_infos(idle_chunk_info);
      header_->set_next_block_chunk_id(idle_chunk_info.id() + 1);

      // The following create_idle_block() calls this function but it never
      // comes here and never makes an endless loop.
      BlockInfo * const idle_block_info = create_idle_block();
      idle_block_info->set_chunk_id(idle_chunk_info.id());
      idle_block_info->set_offset(0);
      idle_block_info->set_size(idle_chunk_info.size());
      idle_block_info->set_next_block_id(BLOCK_INVALID_ID);
      idle_block_info->set_prev_block_id(BLOCK_INVALID_ID);
      register_idle_block(idle_block_info);
    }
  }

  if (!block_info_chunks_[block_info_chunk_id]) {
    mmap_block_info_chunk(block_info_chunk_id);
  }

  const uint32_t offset_mask = block_info_chunk_size - 1;
  BlockInfo * const block_infos = static_cast<BlockInfo *>(
      block_info_chunks_[block_info_chunk_id].address());

  BlockInfo * const block_info = &block_infos[block_id & offset_mask];
  block_info->set_id(block_id);
  phantomize_block(block_info);

  header_->set_num_blocks(block_id + 1);
  header_->set_latest_phantom_block_id(block_id);
  return block_info;
}

BlockInfo *PoolImpl::create_active_block(uint64_t size) {
  if (header_->next_block_chunk_id() >= POOL_MAX_NUM_BLOCK_CHUNKS) {
    GRNXX_ERROR() << "too many block chunks: next_block_chunk_id = "
                  << header_->next_block_chunk_id()
                  << ", max_num_block_chunks = "
                  << POOL_MAX_NUM_BLOCK_CHUNKS;
    GRNXX_THROW();
  }

  uint64_t chunk_size = static_cast<uint64_t>(
      header_->total_size() * options().next_block_chunk_size_ratio());
  if (chunk_size < size) {
    chunk_size = size;
  }
  chunk_size = (chunk_size + (CHUNK_UNIT_SIZE - 1)) & ~(CHUNK_UNIT_SIZE - 1);
  if (chunk_size < options().min_block_chunk_size()) {
    chunk_size = options().min_block_chunk_size();
  }
  if (chunk_size > options().max_block_chunk_size()) {
    chunk_size = options().max_block_chunk_size();
  }

  const uint16_t file_id = header_->total_size() / options().max_file_size();
  const uint64_t file_size = header_->total_size() % options().max_file_size();
  const uint64_t file_size_left = options().max_file_size() - file_size;

  ChunkInfo chunk_info;
  chunk_info.set_id(header_->next_block_chunk_id());
  if (file_size_left < chunk_size) {
    if (file_id >= POOL_MAX_NUM_FILES) {
      GRNXX_ERROR() << "too many files: fild_id = " << file_id
                    << ", max_num_files = " << POOL_MAX_NUM_FILES;
      GRNXX_THROW();
    }
    chunk_info.set_file_id(file_id + 1);
    chunk_info.set_offset(0);
  } else {
    chunk_info.set_file_id(file_id);
    chunk_info.set_offset(file_size);
  }
  chunk_info.set_size(chunk_size);
  header_->set_block_chunk_infos(chunk_info);
  header_->set_next_block_chunk_id(chunk_info.id() + 1);

  header_->set_total_size((chunk_info.file_id() * options().max_file_size())
      + chunk_info.offset() + chunk_info.size());

  BlockInfo *block_info;
  if (header_->latest_phantom_block_id() != BLOCK_INVALID_ID) {
    block_info = get_block_info(header_->latest_phantom_block_id());
  } else {
    block_info = create_phantom_block();
  }
  header_->set_latest_phantom_block_id(block_info->next_phantom_block_id());
  block_info->set_status(BLOCK_ACTIVE);
  block_info->set_chunk_id(chunk_info.id());
  block_info->set_offset(0);
  block_info->set_size(size);
  block_info->set_next_block_id(BLOCK_INVALID_ID);
  block_info->set_prev_block_id(BLOCK_INVALID_ID);

  if (size < chunk_size) {
    // Create an idle block for the left space.
    BlockInfo * const idle_block_info = create_idle_block();
    idle_block_info->set_chunk_id(chunk_info.id());
    idle_block_info->set_offset(block_info->size());
    idle_block_info->set_size(chunk_info.size() - block_info->size());
    idle_block_info->set_next_block_id(BLOCK_INVALID_ID);
    idle_block_info->set_prev_block_id(block_info->id());
    register_idle_block(idle_block_info);

    block_info->set_next_block_id(idle_block_info->id());
  }

  if (file_id != chunk_info.file_id()) {
    // Create a block chunk and an idle block for the left space.
    // In this case, the block chunk size might be less than
    // options().min_block_chunk_size().
    if (header_->next_block_chunk_id() >= POOL_MAX_NUM_BLOCK_CHUNKS) {
      GRNXX_ERROR() << "too many block chunks: next_block_chunk_id = "
                    << header_->next_block_chunk_id()
                    << ", max_num_block_chunks = "
                    << POOL_MAX_NUM_BLOCK_CHUNKS;
      GRNXX_THROW();
    }

    ChunkInfo chunk_info;
    chunk_info.set_id(header_->next_block_chunk_id());
    chunk_info.set_file_id(file_id);
    chunk_info.set_offset(file_size);
    chunk_info.set_size(file_size_left);
    header_->set_block_chunk_infos(chunk_info);
    header_->set_next_block_chunk_id(chunk_info.id() + 1);

    BlockInfo * const idle_block_info = create_idle_block();
    idle_block_info->set_chunk_id(chunk_info.id());
    idle_block_info->set_offset(0);
    idle_block_info->set_size(chunk_info.size());
    idle_block_info->set_next_block_id(BLOCK_INVALID_ID);
    idle_block_info->set_prev_block_id(BLOCK_INVALID_ID);
    register_idle_block(idle_block_info);
  }

  return block_info;
}

BlockInfo *PoolImpl::create_idle_block() {
  BlockInfo *block_info;
  if (header_->latest_phantom_block_id() != BLOCK_INVALID_ID) {
    block_info = get_block_info(header_->latest_phantom_block_id());
  } else {
    block_info = create_phantom_block();
  }
  header_->set_latest_phantom_block_id(block_info->next_phantom_block_id());
  block_info->set_status(BLOCK_IDLE);
  return block_info;
}

BlockInfo *PoolImpl::find_idle_block(uint64_t size) {
  for (uint8_t list_id = bit_scan_reverse(size >> BLOCK_UNIT_SIZE_BITS);
       list_id < 32; ++list_id) {
    if (header_->oldest_idle_block_ids(list_id) != BLOCK_INVALID_ID) {
      BlockInfo * const block_info =
          get_block_info(header_->oldest_idle_block_ids(list_id));
      if (block_info->size() >= size) {
        return block_info;
      }
    }
  }
  return nullptr;
}

void PoolImpl::phantomize_block(BlockInfo *block_info) {
  block_info->set_status(BLOCK_PHANTOM);
  block_info->set_next_phantom_block_id(header_->latest_phantom_block_id());
  header_->set_latest_phantom_block_id(block_info->id());
}

uint32_t PoolImpl::unfreeze_oldest_frozen_blocks(uint32_t max_count) {
  for (uint32_t count = 0; count < max_count; ++count) {
    if (!unfreeze_oldest_frozen_block()) {
      return count;
    }
  }
  return max_count;
}

bool PoolImpl::unfreeze_oldest_frozen_block() {
  if (header_->latest_frozen_block_id() == BLOCK_INVALID_ID) {
    return false;
  }

  BlockInfo * const latest_frozen_block_info =
      get_block_info(header_->latest_frozen_block_id());
  BlockInfo * const oldest_frozen_block_info =
      get_block_info(latest_frozen_block_info->next_frozen_block_id());

  // Recently frozen blocks are rejected.
  if (!mutable_recycler()->check(oldest_frozen_block_info->frozen_stamp())) {
    return false;
  }

  if (latest_frozen_block_info == oldest_frozen_block_info) {
    header_->set_latest_frozen_block_id(BLOCK_INVALID_ID);
  } else {
    latest_frozen_block_info->set_next_frozen_block_id(
        oldest_frozen_block_info->next_frozen_block_id());
  }
  oldest_frozen_block_info->set_status(BLOCK_IDLE);
  register_idle_block(oldest_frozen_block_info);
  merge_idle_blocks(oldest_frozen_block_info);

  return true;
}

BlockInfo *PoolImpl::activate_idle_block(BlockInfo *block_info,
                                         uint64_t size) {
  unregister_idle_block(block_info);
  if (size < block_info->size()) {
    BlockInfo * const idle_block_info = create_idle_block();
    idle_block_info->set_chunk_id(block_info->chunk_id());
    idle_block_info->set_offset(block_info->offset() + size);
    idle_block_info->set_size(block_info->size() - size);
    idle_block_info->set_next_block_id(block_info->next_block_id());
    idle_block_info->set_prev_block_id(block_info->id());
    register_idle_block(idle_block_info);

    if (block_info->next_block_id() != BLOCK_INVALID_ID) {
      BlockInfo * const next_block_info =
          get_block_info(block_info->next_block_id());
      next_block_info->set_prev_block_id(idle_block_info->id());
    }
    block_info->set_size(size);
    block_info->set_next_block_id(idle_block_info->id());
  }
  block_info->set_status(BLOCK_ACTIVE);
  return block_info;
}

void PoolImpl::merge_idle_blocks(BlockInfo *center_block_info) {
  if (center_block_info->next_block_id() != BLOCK_INVALID_ID) {
    BlockInfo * const next_block_info =
        get_block_info(center_block_info->next_block_id());
    if (next_block_info->status() == BLOCK_IDLE) {
      merge_idle_blocks(center_block_info, next_block_info);
    }
  }

  if (center_block_info->prev_block_id() != BLOCK_INVALID_ID) {
    BlockInfo * const prev_block_info =
        get_block_info(center_block_info->prev_block_id());
    if (prev_block_info->status() == BLOCK_IDLE) {
      merge_idle_blocks(prev_block_info, center_block_info);
    }
  }
}

void PoolImpl::merge_idle_blocks(BlockInfo *block_info,
                                 BlockInfo *next_block_info) {
  unregister_idle_block(block_info);
  unregister_idle_block(next_block_info);

  block_info->set_next_block_id(next_block_info->next_block_id());
  if (next_block_info->next_block_id() != BLOCK_INVALID_ID) {
    BlockInfo * const next_next_block_info =
        get_block_info(next_block_info->next_block_id());
    next_next_block_info->set_prev_block_id(block_info->id());
  }
  block_info->set_size(block_info->size() + next_block_info->size());

  phantomize_block(next_block_info);

  register_idle_block(block_info);
}

void PoolImpl::register_idle_block(BlockInfo *block_info) {
  const uint8_t list_id =
      bit_scan_reverse(block_info->size() >> BLOCK_UNIT_SIZE_BITS);
  if (header_->oldest_idle_block_ids(list_id) == BLOCK_INVALID_ID) {
    block_info->set_next_idle_block_id(block_info->id());
    block_info->set_prev_idle_block_id(block_info->id());
    header_->set_oldest_idle_block_ids(list_id, block_info->id());
  } else {
    // latest_idle_block <-> new_idle_block <-> oldest_idle_block.
    BlockInfo * const oldest_idle_block_info =
        get_block_info(header_->oldest_idle_block_ids(list_id));
    BlockInfo * const latest_idle_block_info = 
        get_block_info(oldest_idle_block_info->prev_idle_block_id());
    block_info->set_next_idle_block_id(oldest_idle_block_info->id());
    block_info->set_prev_idle_block_id(latest_idle_block_info->id());
    latest_idle_block_info->set_next_idle_block_id(block_info->id());
    oldest_idle_block_info->set_prev_idle_block_id(block_info->id());
  }
}

void PoolImpl::unregister_idle_block(BlockInfo *block_info) {
  const uint8_t list_id =
      bit_scan_reverse(block_info->size() >> BLOCK_UNIT_SIZE_BITS);
  if (block_info->id() == block_info->next_idle_block_id()) {
    header_->set_oldest_idle_block_ids(list_id, BLOCK_INVALID_ID);
  } else {
    // prev_idle_block <-> next_idle_block.
    BlockInfo * const next_idle_block_info =
        get_block_info(block_info->next_idle_block_id());
    BlockInfo * const prev_idle_block_info =
        get_block_info(block_info->prev_idle_block_id());
    next_idle_block_info->set_prev_idle_block_id(prev_idle_block_info->id());
    prev_idle_block_info->set_next_idle_block_id(next_idle_block_info->id());
    if (block_info->id() == header_->oldest_idle_block_ids(list_id)) {
      header_->set_oldest_idle_block_ids(list_id, next_idle_block_info->id());
    }
  }
}

}  // namespace io
}  // namespace grnxx
