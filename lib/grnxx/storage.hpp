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

enum StorageNodeStatus : uint8_t {
  // A node without body.
  STORAGE_PHANTOM  = 0,
  // An active node.
  STORAGE_ACTIVE   = 1,
  // A marked node to be unlinked.
  STORAGE_MARKED   = 2,
  // An unlinked node.
  STORAGE_UNLINKED = 3,
  // An unused node.
  STORAGE_IDLE     = 4,
};

struct StorageNodeHeader {
  // The ID of this node.
  uint32_t id;
  // The status of this node.
  StorageNodeStatus status;
  // (Non-phantom)
  // For calculating the actual offset and size, see also "offset" and "size".
  uint8_t bits;
  // (Non-phantom)
  // The ID of the chunk to which this node belongs.
  uint16_t chunk_id;
  // (Non-phantom)
  // The offset of this node in chunk. The actual offset is "offset" << "bits".
  uint32_t offset;
  // (Non-phantom)
  // The size of this node. The actual size is "size" << "bits".
  uint32_t size;
  // (Non-phantom)
  // The ID of the next node in chunk. INVALID_ID indicates that this node is
  // the last node in chunk.
  uint32_t next_node_id;
  // (Non-phantom)
  // The ID of the previous node in chunk. INVALID_ID indicates that this node
  // is the first node in chunk.
  uint32_t prev_node_id;
  union {
    // (Phantom)
    // The ID of the next phantom node.
    uint32_t next_phantom_node_id;
    // (Active, marked, or unlinked)
    // The ID of the latest child node. INVALID_ID indicates that this node has
    // no children.
    uint32_t child_id;
    // (Idle)
    // The ID of the next idle node.
    uint32_t next_idle_node_id;
  };
  union {
    // (Active or marked)
    // The ID of the next sibling node. INVALID_ID indicates that this node has
    // no elder siblings.
    uint32_t sibling_node_id;
    // (Unlinked)
    // The ID of the next unlinked node. INVALID_ID indicates that this node
    // is the last unlinked node.
    uint32_t next_unlinked_node_id;
    // (Idle)
    // The ID of the previous idle node.
    uint32_t prev_idle_node_id;
  };
  // The time of the last modification.
  Time last_modified_time;
  // Reserved for future use.
  uint8_t reserved[8];
  // User data.
  uint8_t user_data[16];

  // Initialize the members.
  StorageNodeHeader();
};

static_assert(sizeof(StorageNodeHeader) == 64,
              "sizeof(StorageNodeHeader) != 64");

struct StorageNode {
  // The address to the node header.
  StorageNodeHeader *header;
  // The address to the node body.
  void *body;
};

class Storage;
typedef FlagsImpl<Storage> StorageFlags;

// Create an anonymous (non-file-backed) temporary storage. All other flags,
// except STORAGE_HUGE_TLB, are ignored.
constexpr StorageFlags STORAGE_ANONYMOUS = StorageFlags::define(0x01);
// Try to use huge pages. If huge pages are not available, regular pages will
// be used.
constexpr StorageFlags STORAGE_HUGE_TLB  = StorageFlags::define(0x02);
// Open a storage in read-only mode. If this flag is not specified, a storage
// is opened in read-write mode.
constexpr StorageFlags STORAGE_READ_ONLY = StorageFlags::define(0x04);
// Create a file-backed temporary storage. All other flags, except
// STORAGE_ANONYMOUS and STORAGE_HUGE_TLB, are ignored.
constexpr StorageFlags STORAGE_TEMPORARY = StorageFlags::define(0x08);

// Generate a string from a set of flags.
StringBuilder &operator<<(StringBuilder &builder, StorageFlags flags);
std::ostream &operator<<(std::ostream &builder, StorageFlags flags);

struct StorageOptions {
  // The maximum number of files.
  uint64_t max_num_files;
  // The maximum size of each file.
  uint64_t max_file_size;
  // The ratio of the new chunk size to the storage total size.
  double chunk_size_ratio;
  // The size of the root node.
  uint64_t root_size;

  // Initialize the members with the default parameters.
  StorageOptions();
};

struct StorageHeader {
  // TODO

  // Initialize the members.
  StorageHeader();
};

class Storage {
 public:
  Storage();
  virtual ~Storage();

  // Create a storage if missing. If "flags" contains STORAGE_ANONYMOUS or
  // STORAGE_TEMPORARY, "path" == nullptr is acceptable. Available flags are
  // STORAGE_ANONYMOUS, STORAGE_HUGE_TLB, and STORAGE_TEMPORARY.
  static Storage *create(StorageFlags flags,
                         const char *path = nullptr,
                         const StorageOptions &options = StorageOptions());
  // Open an existing storage. Available flags are STORAGE_HUGE_TLB and
  // STORAGE_READ_ONLY.
  static Storage *open(StorageFlags flags,
                       const char *path = nullptr);
  // Create a storage if missing, or open an existing storage. The available
  // flag is STORAGE_HUGE_TLB.
  static Storage *create_or_open(StorageFlags flags,
                                 const char *path = nullptr,
                                 const StorageOptions &options = StorageOptions());

  // Return true iff "path" refers to a valid storage.
  static bool exists(const char *path);
  // Remove a storage and return true on success.
  static bool unlink(const char *path);

  // Return the ID of the root node.
  static constexpr uint32_t root_id() {
    return 0U;
  }
  // Return an invalid ID.
  static constexpr uint32_t invalid_id() {
    return std::numeric_limits<uint32_t>::max();
  }

  // Return the address to the header.
  virtual const StorageHeader *header() const = 0;

  // Open an existing node.
  virtual StorageNode open_node(uint32_t node_id) = 0;
  // Create a node of at least "size" bytes under "parent_node".
  virtual StorageNode create_node(const StorageNode &parent_node,
                                  uint64_t size) = 0;
  // Mark a node to be unlinked. Note that the marked node and its descendants
  // will be unlinked by sweep().
  virtual bool unlink_node(const StorageNode &node) = 0;

  // Sweep marked nodes whose last modified time < (now - lifetime).
  virtual void sweep(Duration lifetime) = 0;
};

}  // namespace grnxx

#endif  // GRNXX_STORAGE_HPP
