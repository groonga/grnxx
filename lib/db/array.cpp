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
#include "array.hpp"

namespace grnxx {
namespace db {

void ArrayHeader::initialize(uint64_t value_size, uint64_t array_size) {
  std::memset(this, 0, sizeof(ArrayHeader));
  value_size_ = value_size;
  array_size_ = array_size;
}

ArrayImpl::ArrayImpl()
  : pool_(), block_id_(io::BLOCK_INVALID_ID),
    header_(nullptr), address_(nullptr) {}

ArrayImpl::ArrayImpl(io::Pool *pool, uint64_t value_size, uint64_t array_size)
  : pool_(), block_id_(io::BLOCK_INVALID_ID),
    header_(nullptr), address_(nullptr) {
  if (!pool) {
    GRNXX_ERROR() << "invalid argument: pool = " << pool;
    GRNXX_THROW();
  }
  if (value_size == 0) {
    GRNXX_ERROR() << "invalid argument: value_size = " << value_size;
    GRNXX_THROW();
  }

  pool_ = *pool;

  const io::BlockInfo *block_info =
      pool->create_block(ARRAY_HEADER_SIZE + (value_size * array_size));

  header_ = static_cast<ArrayHeader *>(pool->get_block_address(*block_info));
  header_->initialize(value_size, array_size);

  block_id_ = block_info->id();
  address_ = header_ + 1;
}

ArrayImpl::ArrayImpl(io::Pool *pool, uint32_t block_id)
  : pool_(), block_id_(io::BLOCK_INVALID_ID),
    header_(nullptr), address_(nullptr) {
  if (!pool) {
    GRNXX_ERROR() << "invalid argument: pool = " << pool;
    GRNXX_THROW();
  }

  pool_ = *pool;

  const io::BlockInfo *block_info = pool->get_block_info(block_id);
  if (block_info->size() < ARRAY_HEADER_SIZE) {
    GRNXX_ERROR() << "too small block: block_size = " << block_info->size();
    GRNXX_THROW();
  }

  block_id_ = block_info->id();
  header_ = static_cast<ArrayHeader *>(pool->get_block_address(*block_info));
  address_ = header_ + 1;

  if (value_size() == 0) {
    GRNXX_ERROR() << "invalid parameter: value_size = " << value_size();
    GRNXX_THROW();
  }

  const uint64_t required_block_size =
      ARRAY_HEADER_SIZE + (value_size() * array_size());
  if (block_info->size() < required_block_size) {
    GRNXX_ERROR() << "block size conflict: block_size = " << block_info->size()
                  << ", required_block_size = " << required_block_size;
    GRNXX_THROW();
  }
}

ArrayImpl::~ArrayImpl() {}

ArrayImpl::ArrayImpl(ArrayImpl &&array)
  : pool_(std::move(array.pool_)), block_id_(std::move(array.block_id_)),
    header_(std::move(array.header_)), address_(std::move(array.address_)) {}

ArrayImpl &ArrayImpl::operator=(ArrayImpl &&array) {
  pool_ = std::move(array.pool_);
  block_id_ = std::move(array.block_id_);
  header_ = std::move(array.header_);
  address_ = std::move(array.address_);
  return *this;
}

void ArrayImpl::create(io::Pool *pool, uint64_t value_size,
                       uint64_t array_size) {
  ArrayImpl(pool, value_size, array_size).swap(*this);
}

void ArrayImpl::open(io::Pool *pool, uint32_t block_id) {
  ArrayImpl(pool, block_id).swap(*this);
}

void ArrayImpl::swap(ArrayImpl &array) {
  using std::swap;
  swap(pool_, array.pool_);
  swap(block_id_, array.block_id_);
  swap(header_, array.header_);
  swap(address_, array.address_);
}

void ArrayImpl::unlink(io::Pool *pool, uint32_t block_id) {
  pool->free_block(block_id);
}

StringBuilder &ArrayHeader::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  return builder << "{ value_size = " << value_size_
                 << ", array_size = " << array_size_ << " }";
}

StringBuilder &ArrayImpl::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  if (!pool_) {
    return builder << "n/a";
  }

  builder << "{ pool = " << pool_.path()
          << ", block_id = " << block_id_
          << ", header = ";
  if (header_) {
    builder << *header_;
  } else {
    builder << "n/a";
  }
  return builder << ", address = " << address_ << " }";
}

}  // namespace db
}  // namespace grnxx
