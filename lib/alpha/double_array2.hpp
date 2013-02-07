/*
  Copyright (C) 2013  Brazil, Inc.

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
#ifndef GRNXX_ALPHA_DOUBLE_ARRAY2_HPP
#define GRNXX_ALPHA_DOUBLE_ARRAY2_HPP

#include "../io/pool.hpp"

namespace grnxx {
namespace alpha {

class DoubleArray2 {
 public:
  DoubleArray2();
  virtual ~DoubleArray2();

  static DoubleArray2 *create(io::Pool pool);
  static DoubleArray2 *open(io::Pool pool, uint32_t block_id);

  virtual uint32_t block_id() const = 0;

  virtual bool search_by_id(int64_t key_id, const void **ptr,
                            uint64_t *length) = 0;
  virtual bool search_by_key(const void *ptr, uint64_t length,
                             int64_t *key_id = nullptr) = 0;

  virtual bool insert(const void *ptr, uint64_t length,
                      int64_t *key_id = nullptr) = 0;

  virtual bool remove_by_id(int64_t key_id) = 0;
  virtual bool remove_by_key(const void *ptr, uint64_t length) = 0;

  virtual bool update_by_id(int64_t key_id, const void *ptr,
                            uint64_t length) = 0;
  virtual bool update_by_key(const void *src_ptr, uint64_t src_length,
                             const void *dest_ptr, uint64_t dest_length,
                             int64_t *key_id = nullptr) = 0;
};

}  // namespace alpha
}  // namespace grnxx

#endif  // GRNXX_ALPHA_DOUBLE_ARRAY2_HPP
