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

#include "grnxx/storage.hpp"
#include "grnxx/time/periodic_clock.hpp"

namespace grnxx {
namespace storage {

struct Header;
class File;
class Chunk;
struct ChunkIndex;

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
  static bool unlink(const char *path);

  StorageNode create_node(uint32_t parent_node_id, uint64_t size);
  StorageNode open_node(uint32_t node_id);

  bool unlink_node(uint32_t node_id);

  bool sweep(Duration lifetime);

  const char *path() const;
  StorageFlags flags() const;

  // TODO: Member functions to get details, such as total size, #nodes, etc.

 private:
  std::unique_ptr<char[]> path_;
  StorageFlags flags_;
  Header *header_;
  ChunkIndex *node_header_chunk_indexes_;
  ChunkIndex *node_body_chunk_indexes_;
  std::unique_ptr<std::unique_ptr<File>[]> files_;
  std::unique_ptr<Chunk> header_chunk_;
  std::unique_ptr<std::unique_ptr<Chunk>[]> node_header_chunks_;
  std::unique_ptr<std::unique_ptr<Chunk>[]> node_body_chunks_;
  PeriodicClock clock_;

  bool create_file_backed_storage(const char *path, StorageFlags flags,
                                  const StorageOptions &options);
  bool create_anonymous_storage(StorageFlags flags,
                                const StorageOptions &options);
  bool open_storage(const char *path, StorageFlags flags);
  bool open_or_create_storage(const char *path, StorageFlags flags,
                              const StorageOptions &options);

  bool prepare_pointers();
  void prepare_indexes();

  NodeHeader *create_active_node(uint64_t size);
  NodeHeader *find_idle_node(uint64_t size);
  NodeHeader *create_idle_node(uint64_t size);
  bool divide_idle_node(NodeHeader *node_header, uint64_t size);
  bool activate_idle_node(NodeHeader *node_header);
  NodeHeader *reserve_phantom_node();
  NodeHeader *create_phantom_node();
  bool associate_node_with_chunk(NodeHeader *node_header,
                                 ChunkIndex *chunk_index);

  ChunkIndex *create_node_header_chunk(ChunkIndex **remainder_chunk_index);
  ChunkIndex *create_node_body_chunk(uint64_t size,
                                     ChunkIndex **remainder_chunk_index);
  ChunkIndex *create_node_body_chunk(uint64_t size);

  bool register_idle_node(NodeHeader *node_header);
  bool unregister_idle_node(NodeHeader *node_header);

  NodeHeader *get_node_header(uint32_t node_id);
  void *get_node_body(const NodeHeader *node_header);
  Chunk *get_node_header_chunk(uint16_t chunk_id);
  Chunk *get_node_body_chunk(uint16_t chunk_id);
  File *get_file(uint16_t file_id);
  char *generate_path(uint16_t file_id);

  Chunk *create_chunk(File *file, int64_t offset, int64_t size);
};

}  // namespace storage
}  // namespace grnxx

#endif  // GRNXX_STORAGE_STORAGE_IMPL_HPP
