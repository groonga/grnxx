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
#include "double_array.hpp"

namespace grnxx {
namespace alpha {

DoubleArrayCreate DOUBLE_ARRAY_CREATE;
DoubleArrayOpen DOUBLE_ARRAY_OPEN;

std::unique_ptr<DoubleArrayImpl> DoubleArrayImpl::create(io::Pool pool) {
  // TODO
  return nullptr;
}

std::unique_ptr<DoubleArrayImpl> DoubleArrayImpl::open(io::Pool pool,
                                                       uint32_t block_id) {
  // TODO
  return nullptr;
}

}  // namespace alpha
}  // namespace grnxx
