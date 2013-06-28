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
#ifndef GRNXX_LOCK_HPP
#define GRNXX_LOCK_HPP

#include "grnxx/features.hpp"

#include "grnxx/duration.hpp"
#include "grnxx/mutex.hpp"

namespace grnxx {

class Lock {
 public:
  Lock() = delete;
  explicit Lock(Mutex *mutex) : mutex_((mutex->lock(), mutex)) {}
  Lock(Mutex *mutex, Duration timeout)
      : mutex_(mutex->lock(timeout) ? mutex : nullptr) {}
  ~Lock() {
    if (mutex_) {
      mutex_->unlock();
    }
  }

  Lock(const Lock &) = delete;
  Lock &operator=(const Lock &) = delete;

  explicit operator bool() const {
    return mutex_ != nullptr;
  }

 private:
  Mutex *mutex_;
};

}  // namespace grnxx

#endif  // GRNXX_LOCK_HPP
