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

#include "basic.hpp"
#include "mutex.hpp"

namespace grnxx {

class Lock {
 public:
  explicit Lock(Mutex *mutex)
    : mutex_object_(mutex->lock() ?
                    reinterpret_cast<Mutex::Object *>(mutex) : nullptr) {}
  explicit Lock(Mutex::Object *mutex_object)
    : mutex_object_(Mutex::lock(mutex_object) ? mutex_object : nullptr) {}
  ~Lock() {
    if (mutex_object_) {
      Mutex::unlock(mutex_object_);
    }
  }

  explicit operator bool() const {
    return mutex_object_ != nullptr;
  }

 private:
  Mutex::Object *mutex_object_;

  Lock(const Lock &);
  Lock &operator=(const Lock &);
};

}  // namespace grnxx

#endif  // GRNXX_LOCK_HPP
