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
#ifndef GRNXX_STORAGE_HPP
#define GRNXX_STORAGE_HPP

#include "grnxx/features.hpp"

#include "grnxx/duration.hpp"
#include "grnxx/flags_impl.hpp"
#include "grnxx/time.hpp"
#include "grnxx/types.hpp"

namespace grnxx {
namespace storage {

struct NodeHeader;

}  // namespace storage

class Storage;
using StorageFlags = FlagsImpl<Storage>;

// Use the default settings.
constexpr StorageFlags STORAGE_DEFAULT   = StorageFlags::define(0x00);
// Create an anonymous storage.
// This flag is implicitly enabled if "path" == nullptr and "flags" does not
// contain STORAGE_TEMPORARY.
constexpr StorageFlags STORAGE_ANONYMOUS = StorageFlags::define(0x01);
// Use huge pages if available, or use regular pages.
constexpr StorageFlags STORAGE_HUGE_TLB  = StorageFlags::define(0x02);
// Open a storage in read-only mode.
// If not specified, a storage is opened in read-write mode.
constexpr StorageFlags STORAGE_READ_ONLY = StorageFlags::define(0x04);
// Create a file-backed temporary storage.
constexpr StorageFlags STORAGE_TEMPORARY = StorageFlags::define(0x08);

StringBuilder &operator<<(StringBuilder &builder, StorageFlags flags);

enum StorageNodeStatus : uint8_t {
  // A node without body.
  STORAGE_NODE_PHANTOM  = 0,
  // An active node.
  STORAGE_NODE_ACTIVE   = 1,
  // An unlinked node.
  STORAGE_NODE_UNLINKED = 2,
  // An unused node.
  STORAGE_NODE_IDLE     = 3
};

StringBuilder &operator<<(StringBuilder &builder, StorageNodeStatus status);

constexpr uint32_t STORAGE_ROOT_NODE_ID    = 0;
constexpr uint32_t STORAGE_INVALID_NODE_ID =
    std::numeric_limits<uint32_t>::max();

struct StorageOptions {
  // The maximum size of each file.
  uint64_t max_file_size;
  // The maximum number of files.
  uint16_t max_num_files;
  // The size of the root node.
  uint64_t root_size;

  // Initialize the members with the default parameters.
  StorageOptions();

  // Return true iff the options are valid.
  explicit operator bool() const;
};

StringBuilder &operator<<(StringBuilder &builder,
                          const StorageOptions &options);

class StorageNode {
 public:
  StorageNode() = default;
  explicit StorageNode(nullptr_t) : header_(nullptr), body_(nullptr) {}
  StorageNode(storage::NodeHeader *header, void *body)
      : header_(header),
        body_(body) {}

  // Return true iff the node is valid.
  explicit operator bool() const {
    return header_ != nullptr;
  }

  // Return the ID.
  uint32_t id() const;
  // Return the status.
  StorageNodeStatus status() const;
  // Return the ID of the chunk to which the node belongs.
  uint16_t chunk_id() const;
  // Return the offset in chunk.
  uint64_t offset() const;
  // Return the body size.
  uint64_t size() const;
  // Return the last modified time.
  Time modified_time() const;
  // Return the address to the user data (8 bytes) in the header.
  void *user_data() const;
  // Return the address to the body.
  void *body() const {
    return body_;
  }

 private:
  // The address to the node header.
  storage::NodeHeader *header_;
  // The address to the node body.
  void *body_;
};

class Storage {
 public:
  Storage();
  virtual ~Storage();

  // Create a storage.
  // STORAGE_ANONYMOUS is implicitly enabled if "path" == nullptr and "flags"
  // does not contain STORAGE_TEMPORARY.
  // Available flags are STORAGE_HUGE_TLB and STORAGE_TEMPORARY.
  static Storage *create(const char *path,
                         StorageFlags flags = STORAGE_DEFAULT,
                         const StorageOptions &options = StorageOptions());
  // Open a storage.
  // Available flags are STORAGE_HUGE_TLB and STORAGE_READ_ONLY.
  static Storage *open(const char *path,
                       StorageFlags flags = STORAGE_DEFAULT);
  // Open or create a storage.
  // The available flags is STORAGE_HUGE_TLB.
  static Storage *open_or_create(
      const char *path, StorageFlags flags = STORAGE_DEFAULT,
      const StorageOptions &options = StorageOptions());

  // Return true iff "path" refers to a valid storage.
  static bool exists(const char *path);
  // Remove a storage and return true on success.
  static bool unlink(const char *path);

  // Create a node of at least "size" bytes under the specified parent node.
  virtual StorageNode create_node(uint32_t parent_node_id, uint64_t size) = 0;
  // Open a node.
  virtual StorageNode open_node(uint32_t node_id) = 0;

  // Unlink a node and return true on success.
  // The unlinked node and its descendants will be removed by sweep().
  virtual bool unlink_node(uint32_t node_id) = 0;

  // Sweep unlinked nodes whose modified time < (now - lifetime).
  virtual void sweep(Duration lifetime) = 0;

  // Return the storage path.
  // Note that an anonymous or temporary storage may return nullptr.
  virtual const char *path() const = 0;
  // Return the activated flags.
  virtual StorageFlags flags() const = 0;
  // Return the maximum size of each file.
  virtual uint64_t max_file_size() const = 0;
  // Return the maximum number of files.
  virtual uint16_t max_num_files() const = 0;
  // Return the number of active or unlinked nodes.
  virtual uint32_t num_nodes() const = 0;
  // Return the number of chunks for node body.
  virtual uint16_t num_chunks() const = 0;
  // Return the total usage of body chunks (including unlinked nodes).
  virtual uint64_t body_usage() const = 0;
  // Return the total size of body chunks.
  virtual uint64_t body_size() const = 0;
  // Return the total size.
  virtual uint64_t total_size() const = 0;
};

}  // namespace grnxx

#endif  // GRNXX_STORAGE_HPP
