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

#include "grnxx/basic.hpp"
#include "grnxx/time/time.hpp"

namespace grnxx {
namespace storage {

struct NodeHeader;
class StorageImpl;

}  // namespace storage

using StorageNodeHeader = storage::NodeHeader;
using StorageImpl = storage::StorageImpl;

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
  // A marked node to be unlinked.
  STORAGE_NODE_MARKED   = 2,
  // An unlinked node.
  STORAGE_NODE_UNLINKED = 3,
  // An unused node.
  STORAGE_NODE_IDLE     = 4
};

StringBuilder &operator<<(StringBuilder &builder, StorageNodeStatus status);

struct StorageOptions {
  // The maximum number of files.
  uint64_t max_num_files;
  // The maximum size of each file.
  uint64_t max_file_size;
  // The size of the root node.
  uint64_t root_size;

  // Initialize the members with the default parameters.
  StorageOptions();
};

class StorageNode {
 public:
  StorageNode() = default;
  StorageNode(StorageNodeHeader *header, void *body)
      : header_(header),
        body_(body) {}

  // Return the ID.
  uint32_t id() const;
  // Return the status.
  StorageNodeStatus status() const;
  // Return the body size.
  uint64_t size() const;
  // Return the last modified time.
  Time modified_time() const;
  // Return the address to the user data (16 bytes) in the header.
  void *user_data() const;
  // Return the address to the body.
  void *body() const {
    return body_;
  }

 private:
  // The address to the node header.
  StorageNodeHeader *header_;
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
  static Storage *open_or_create(const char *path,
                                 StorageFlags flags = STORAGE_DEFAULT,
                                 const StorageOptions &options = StorageOptions());

  // Return true iff "path" refers to a valid storage.
  static bool exists(const char *path);
  // Remove a storage and return true on success.
  static bool unlink(const char *path);

  // Return the ID of the root node.
  static constexpr uint32_t root_id() {
    return 0U;
  }
  // Return the invalid node ID.
  static constexpr uint32_t invalid_id() {
    return std::numeric_limits<uint32_t>::max();
  }

  // Create a node of at least "size" bytes under the specified parent node.
  virtual StorageNode create_node(uint32_t parent_node_id, uint64_t size) = 0;
  // Open a node.
  virtual StorageNode open_node(uint32_t node_id) = 0;

  // Mark a node to be unlinked. Note that the marked node and its descendants
  // will be unlinked by sweep().
  virtual bool unlink_node(uint32_t node_id) = 0;

  // Sweep marked nodes whose last modified time < (now - lifetime).
  virtual bool sweep(Duration lifetime) = 0;

  // Return the storage path.
  virtual const char *path() const = 0;
  // Return the activated flags.
  virtual StorageFlags flags() const = 0;

  // TODO: Member functions to get details, such as total size, #nodes, etc.
};

}  // namespace grnxx

#endif  // GRNXX_STORAGE_HPP
