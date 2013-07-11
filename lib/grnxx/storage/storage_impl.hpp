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
#ifndef GRNXX_STORAGE_STORAGE_IMPL_HPP
#define GRNXX_STORAGE_STORAGE_IMPL_HPP

#include "grnxx/features.hpp"

#include <memory>

#include "grnxx/duration.hpp"
#include "grnxx/mutex.hpp"
#include "grnxx/periodic_clock.hpp"
#include "grnxx/storage.hpp"
#include "grnxx/time.hpp"

namespace grnxx {
namespace storage {

struct Header;
class File;
class Chunk;
struct ChunkIndex;
struct NodeHeader;

class StorageImpl : public Storage {
 public:
  StorageImpl();
  ~StorageImpl();

  static StorageImpl *create(const char *path,
                             StorageFlags flags,
                             const StorageOptions &options);
  static StorageImpl *open(const char *path,
                           StorageFlags flags);
  static StorageImpl *open_or_create(const char *path,
                                     StorageFlags flags,
                                     const StorageOptions &options);

  static bool exists(const char *path);
  static void unlink(const char *path);

  StorageNode create_node(uint32_t parent_node_id, uint64_t size);
  StorageNode open_node(uint32_t node_id);

  void unlink_node(uint32_t node_id);

  void sweep(Duration lifetime);

  const char *path() const;
  StorageFlags flags() const;
  uint64_t max_file_size() const;
  uint16_t max_num_files() const;
  uint32_t num_nodes() const;
  uint16_t num_chunks() const;
  uint64_t body_usage() const;
  uint64_t body_size() const;
  uint64_t total_size() const;

 private:
  std::unique_ptr<char[]> path_;
  StorageFlags flags_;
  Header *header_;
  ChunkIndex *header_chunk_indexes_;
  ChunkIndex *body_chunk_indexes_;
  std::unique_ptr<std::unique_ptr<File>[]> files_;
  std::unique_ptr<Chunk> root_chunk_;
  std::unique_ptr<std::unique_ptr<Chunk>[]> header_chunks_;
  std::unique_ptr<std::unique_ptr<Chunk>[]> body_chunks_;
  Mutex mutex_;
  PeriodicClock clock_;

  void create_file_backed_storage(const char *path, StorageFlags flags,
                                  const StorageOptions &options);
  void create_anonymous_storage(StorageFlags flags,
                                const StorageOptions &options);
  void open_storage(const char *path, StorageFlags flags);
  void open_or_create_storage(const char *path, StorageFlags flags,
                              const StorageOptions &options);
  void unlink_storage();

  void prepare_pointers();
  void prepare_indexes();

  NodeHeader *create_active_node(uint64_t size);
  NodeHeader *find_idle_node(uint64_t size);
  NodeHeader *create_idle_node(uint64_t size);
  void divide_idle_node(NodeHeader *node_header, uint64_t size);
  void activate_idle_node(NodeHeader *node_header);
  NodeHeader *reserve_phantom_node();
  NodeHeader *create_phantom_node();
  void associate_node_with_chunk(NodeHeader *node_header,
                                 ChunkIndex *chunk_index);

  void sweep_subtree(NodeHeader *node_header);
  void merge_idle_nodes(NodeHeader *node_header, NodeHeader *next_node_header);

  ChunkIndex *create_header_chunk(ChunkIndex **remainder_chunk_index);
  ChunkIndex *create_body_chunk(uint64_t size,
                                ChunkIndex **remainder_chunk_index);
  ChunkIndex *create_body_chunk(uint64_t size);

  void register_idle_node(NodeHeader *node_header);
  void unregister_idle_node(NodeHeader *node_header);

  NodeHeader *get_node_header(uint32_t node_id);
  void *get_node_body(const NodeHeader *node_header);
  Chunk *get_header_chunk(uint16_t chunk_id);
  Chunk *get_body_chunk(uint16_t chunk_id);
  File *reserve_file(uint16_t file_id, uint64_t size);
  char *generate_path(uint16_t file_id);

  Chunk *create_chunk(File *file, uint64_t offset, uint64_t size);
};

}  // namespace storage
}  // namespace grnxx

#endif  // GRNXX_STORAGE_STORAGE_IMPL_HPP
