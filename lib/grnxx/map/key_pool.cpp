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
#include "grnxx/map/key_pool.hpp"

#include "grnxx/common_header.hpp"
#include "grnxx/exception.hpp"
#include "grnxx/geo_point.hpp"
#include "grnxx/intrinsic.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/map.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {
namespace map {
namespace {

constexpr char FORMAT_STRING[] = "grnxx::map::KeyPool";

constexpr int64_t MIN_KEY_ID = MAP_MIN_KEY_ID;
constexpr int64_t MAX_KEY_ID = MAP_MAX_KEY_ID;

constexpr uint64_t INVALID_UNIT_ID  = ~0ULL;
constexpr uint64_t INVALID_ENTRY_ID = (1ULL << 63) - 1;

}  // namespace

struct KeyPoolHeader {
  grnxx::CommonHeader common_header;
  int64_t max_key_id;
  uint64_t num_keys;
  // For other than Bytes.
  uint64_t latest_available_unit_id;
  uint32_t keys_storage_node_id;
  uint32_t bits_storage_node_id;
  uint32_t links_storage_node_id;
  // For Bytes.
  uint64_t latest_free_entry_id;
  uint32_t pool_storage_node_id;
  uint32_t entries_storage_node_id;

  // Initialize the member variables.
  KeyPoolHeader();

  // Return true iff the header seems to be correct.
  explicit operator bool() const;
};

KeyPoolHeader::KeyPoolHeader()
    : common_header(FORMAT_STRING),
      max_key_id(MIN_KEY_ID - 1),
      num_keys(0),
      latest_available_unit_id(INVALID_UNIT_ID),
      keys_storage_node_id(STORAGE_INVALID_NODE_ID),
      bits_storage_node_id(STORAGE_INVALID_NODE_ID),
      links_storage_node_id(STORAGE_INVALID_NODE_ID),
      latest_free_entry_id(INVALID_ENTRY_ID),
      pool_storage_node_id(STORAGE_INVALID_NODE_ID),
      entries_storage_node_id(STORAGE_INVALID_NODE_ID) {}

KeyPoolHeader::operator bool() const {
  return common_header.format() == FORMAT_STRING;
}

template <typename T>
KeyPool<T>::KeyPool()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      max_key_id_(nullptr),
      num_keys_(nullptr),
      keys_(),
      bits_(),
      links_() {}

template <typename T>
KeyPool<T>::~KeyPool() {}

template <typename T>
KeyPool<T> *KeyPool<T>::create(Storage *storage, uint32_t storage_node_id) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage == nullptr";
    throw LogicError();
  }
  std::unique_ptr<KeyPool> pool(new (std::nothrow) KeyPool);
  if (!pool) {
    GRNXX_ERROR() << "new grnxx::map::KeyPool failed";
    throw MemoryError();
  }
  pool->create_pool(storage, storage_node_id);
  return pool.release();
}

template <typename T>
KeyPool<T> *KeyPool<T>::open(Storage *storage, uint32_t storage_node_id) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage == nullptr";
    throw LogicError();
  }
  std::unique_ptr<KeyPool> pool(new (std::nothrow) KeyPool);
  if (!pool) {
    GRNXX_ERROR() << "new grnxx::map::KeyPool failed";
    throw MemoryError();
  }
  pool->open_pool(storage, storage_node_id);
  return pool.release();
}

template <typename T>
void KeyPool<T>::unset(int64_t key_id) {
  // Prepare to update "bits_" and "links_".
  const uint64_t unit_id = key_id / UNIT_SIZE;
  const uint64_t unit_bit = 1ULL << (key_id % UNIT_SIZE);
  BitArrayUnit * const unit = &bits_->get_unit(unit_id);
  if ((*unit & unit_bit) == 0) {
    GRNXX_ERROR() << "not found: key_id = " << key_id;
    throw LogicError();
  }
  // Set "link" if this operation creates the first 0 bit in the unit.
  Link *link = nullptr;
  if (*unit == ~BitArrayUnit(0)) {
    link = &links_->get_value(unit_id);
  }
  *unit &= ~unit_bit;
  if (link) {
    if (header_->latest_available_unit_id != INVALID_UNIT_ID) {
      // "*link" points to the next unit.
      *link = static_cast<Link>(header_->latest_available_unit_id);
    } else {
      // "*link" points to itself because there are no more units.
      *link = static_cast<Link>(unit_id);
    }
    header_->latest_available_unit_id = *link;
  }
  --header_->num_keys;
}

template <typename T>
void KeyPool<T>::reset(int64_t key_id, KeyArg dest_key) {
  if (!get_bit(key_id)) {
    GRNXX_ERROR() << "not found: key_id = " << key_id;
    throw LogicError();
  }
  keys_->set(key_id, dest_key);
}

template <typename T>
int64_t KeyPool<T>::add(KeyArg key) {
  // Find an unused key ID.
  const bool is_new_unit = header_->latest_available_unit_id == INVALID_UNIT_ID;
  uint64_t unit_id;
  if (is_new_unit) {
    if (header_->max_key_id == MAX_KEY_ID) {
      GRNXX_ERROR() << "too many keys: key_id = " << (header_->max_key_id + 1)
                    << ", max_key_id = " << MAX_KEY_ID;
      throw LogicError();
    }
    unit_id = (header_->max_key_id + 1) / UNIT_SIZE;
  } else {
    unit_id = header_->latest_available_unit_id;
  }
  BitArrayUnit * const unit = &bits_->get_unit(unit_id);
  uint8_t unit_bit_id;
  uint64_t unit_bit;
  const Link *link = nullptr;
  if (is_new_unit) {
    // "links_[unit_id]" points to itself because there are no more units.
    links_->set(unit_id, unit_id);
    *unit = 0;
    unit_bit_id = 0;
    unit_bit = 1;
    header_->latest_available_unit_id = unit_id;
  } else {
    unit_bit_id = bit_scan_forward(~*unit);
    unit_bit = 1ULL << unit_bit_id;
    if ((*unit | unit_bit) == ~BitArrayUnit(0)) {
      // The unit must be removed from "links_" because it becomes full.
      link = &links_->get_value(header_->latest_available_unit_id);
    }
  }
  const int64_t next_key_id = (unit_id * UNIT_SIZE) + unit_bit_id;
  keys_->set(next_key_id, key);
  if (link) {
    if (*link != unit_id) {
      // Move to the next unit.
      header_->latest_available_unit_id = *link;
    } else {
      // There are no more units.
      header_->latest_available_unit_id = INVALID_UNIT_ID;
    }
  }
  *unit |= unit_bit;
  if (next_key_id > header_->max_key_id) {
    header_->max_key_id = next_key_id;
  }
  ++header_->num_keys;
  return next_key_id;
}

template <typename T>
void KeyPool<T>::truncate() {
  header_->max_key_id = MIN_KEY_ID - 1;
  header_->num_keys = 0;
  header_->latest_available_unit_id = INVALID_UNIT_ID;
}

template <typename T>
void KeyPool<T>::create_pool(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode storage_node =
      storage->create_node(storage_node_id, sizeof(Header));
  storage_node_id_ = storage_node.id();
  try {
    header_ = static_cast<Header *>(storage_node.body());
    *header_ = Header();
    max_key_id_ = &header_->max_key_id;
    num_keys_ = &header_->num_keys;
    keys_.reset(KeyArray::create(storage, storage_node_id_,
                                 KeyPoolHelper<T>::KEY_ARRAY_SIZE));
    bits_.reset(BitArray::create(storage, storage_node_id_,
                                 KeyPoolHelper<T>::BIT_ARRAY_SIZE));
    links_.reset(LinkArray::create(storage, storage_node_id_,
                                   KeyPoolHelper<T>::LINK_ARRAY_SIZE));
    header_->keys_storage_node_id = keys_->storage_node_id();
    header_->bits_storage_node_id = bits_->storage_node_id();
    header_->links_storage_node_id = links_->storage_node_id();
  } catch (...) {
    storage->unlink_node(storage_node_id_);
    throw;
  }
}

template <typename T>
void KeyPool<T>::open_pool(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode storage_node = storage->open_node(storage_node_id);
  storage_node_id_ = storage_node.id();
  header_ = static_cast<Header *>(storage_node.body());
  if (!*header_) {
    GRNXX_ERROR() << "wrong format: expected = " << FORMAT_STRING
                  << ", actual = " << header_->common_header.format();
    throw LogicError();
  }
  max_key_id_ = &header_->max_key_id;
  num_keys_ = &header_->num_keys;
  keys_.reset(KeyArray::open(storage, header_->keys_storage_node_id));
  bits_.reset(BitArray::open(storage, header_->bits_storage_node_id));
  links_.reset(LinkArray::open(storage, header_->links_storage_node_id));
}

template class KeyPool<int8_t>;
template class KeyPool<uint8_t>;
template class KeyPool<int16_t>;
template class KeyPool<uint16_t>;
template class KeyPool<int32_t>;
template class KeyPool<uint32_t>;
template class KeyPool<int64_t>;
template class KeyPool<uint64_t>;
template class KeyPool<double>;
template class KeyPool<GeoPoint>;

KeyPool<Bytes>::KeyPool()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      max_key_id_(nullptr),
      num_keys_(nullptr),
      pool_(),
      entries_() {}

KeyPool<Bytes>::~KeyPool() {}

KeyPool<Bytes> *KeyPool<Bytes>::create(Storage *storage,
                                       uint32_t storage_node_id) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage == nullptr";
    throw LogicError();
  }
  std::unique_ptr<KeyPool> pool(new (std::nothrow) KeyPool);
  if (!pool) {
    GRNXX_ERROR() << "new grnxx::map::KeyPool failed";
    throw MemoryError();
  }
  pool->create_pool(storage, storage_node_id);
  return pool.release();
}

KeyPool<Bytes> *KeyPool<Bytes>::open(Storage *storage,
                                     uint32_t storage_node_id) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage == nullptr";
    throw LogicError();
  }
  std::unique_ptr<KeyPool> pool(new (std::nothrow) KeyPool);
  if (!pool) {
    GRNXX_ERROR() << "new grnxx::map::KeyPool failed";
    throw MemoryError();
  }
  pool->open_pool(storage, storage_node_id);
  return pool.release();
}

void KeyPool<Bytes>::unset(int64_t key_id) {
  Entry &entry = entries_->get_value(key_id);
  if (!entry) {
    GRNXX_ERROR() << "not found: key_id = " << key_id;
    throw LogicError();
  }
  pool_->unset(entry.bytes_id());
  entry.set_next_free_entry_id(header_->latest_free_entry_id);
  header_->latest_free_entry_id = key_id;
  --header_->num_keys;
}

void KeyPool<Bytes>::reset(int64_t key_id, KeyArg dest_key) {
  Entry &entry = entries_->get_value(key_id);
  if (!entry) {
    GRNXX_ERROR() << "not found: key_id = " << key_id;
    throw LogicError();
  }
  const uint64_t src_bytes_id = entry.bytes_id();
  entry.set_bytes_id(pool_->add(dest_key));
  pool_->unset(src_bytes_id);
}

int64_t KeyPool<Bytes>::add(KeyArg key) {
  uint64_t entry_id;
  if (header_->latest_free_entry_id != INVALID_ENTRY_ID) {
    entry_id = header_->latest_free_entry_id;
  } else {
    entry_id = header_->max_key_id + 1;
  }
  Entry &entry = entries_->get_value(entry_id);
  const uint64_t bytes_id = pool_->add(key);
  if (header_->latest_free_entry_id != INVALID_ENTRY_ID) {
    header_->latest_free_entry_id = entry.next_free_entry_id();
  }
  entry.set_bytes_id(bytes_id);
  if (static_cast<int64_t>(entry_id) > header_->max_key_id) {
    header_->max_key_id = entry_id;
  }
  ++header_->num_keys;
  return entry_id;
}

void KeyPool<Bytes>::truncate() {
  header_->max_key_id = MIN_KEY_ID - 1;
  header_->num_keys = 0;
  header_->latest_free_entry_id = INVALID_ENTRY_ID;
  pool_->truncate();
}

void KeyPool<Bytes>::create_pool(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode storage_node =
      storage->create_node(storage_node_id, sizeof(Header));
  storage_node_id_ = storage_node.id();
  try {
    header_ = static_cast<Header *>(storage_node.body());
    *header_ = Header();
    max_key_id_ = &header_->max_key_id;
    num_keys_ = &header_->num_keys;
    pool_.reset(BytesPool::create(storage, storage_node_id_));
    entries_.reset(EntryArray::create(storage, storage_node_id_,
                                      ENTRY_ARRAY_SIZE));
    header_->pool_storage_node_id = pool_->storage_node_id();
    header_->entries_storage_node_id = entries_->storage_node_id();
  } catch (...) {
    storage->unlink_node(storage_node_id_);
    throw;
  }
}

void KeyPool<Bytes>::open_pool(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode storage_node = storage->open_node(storage_node_id);
  storage_node_id_ = storage_node.id();
  header_ = static_cast<Header *>(storage_node.body());
  if (!*header_) {
    GRNXX_ERROR() << "wrong format: expected = " << FORMAT_STRING
                  << ", actual = " << header_->common_header.format();
    throw LogicError();
  }
  max_key_id_ = &header_->max_key_id;
  num_keys_ = &header_->num_keys;
  pool_.reset(BytesPool::open(storage, header_->pool_storage_node_id));
  entries_.reset(EntryArray::open(storage, header_->entries_storage_node_id));
}

}  // namespace map
}  // namespace grnxx
