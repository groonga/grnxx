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
#ifndef GRNXX_IO_FILE_HPP
#define GRNXX_IO_FILE_HPP

#include "flags.hpp"
#include "../duration.hpp"

namespace grnxx {
namespace io {

const int FILE_UNIQUE_PATH_GENERATION_MAX_NUM_TRIALS = 10;

class FileImpl;

class File {
 public:
  File();
  // Available flags are as follows:
  //  GRNXX_IO_READ_ONLY, GRNXX_IO_WRITE_ONLY, GRNXX_IO_APPEND,
  //  GRNXX_IO_CREATE, GRNXX_IO_OPEN, GRNXX_IO_TEMPORARY, GRNXX_IO_TRUNCATE.
  // Windows ignores permission.
  explicit File(const char *path, Flags flags = Flags::none(),
                int permission = 0644);
  ~File();

  File(const File &file);
  File &operator=(const File &file);

  File(File &&file);
  File &operator=(File &&file);

  explicit operator bool() const {
    return static_cast<bool>(impl_);
  }

  // The following functions operate advisory locks for files, not for
  // FileImpl instances. The word "advisory" indicates that the file is
  // accessible even if it is locked.

  // lock() returns false on time-out or deadlock.
  bool lock(LockMode mode, int sleep_count = 1000,
            Duration sleep_duration = Duration::milliseconds(1));
  // try_lock() returns false if the file is already locked.
  bool try_lock(LockMode mode);
  // unlock() returns false if the file is not locked.
  bool unlock();

  bool locked() const;
  bool unlocked() const;

  // The following functions are not thread-safe.

  // read() reads data from file at most size bytes and returns the number of
  // actually read bytes.
  uint64_t read(void *buf, uint64_t size);
  uint64_t read(void *buf, uint64_t size, uint64_t offset);
  // write() writes data into file at most size bytes and returns the number
  // of actually written bytes.
  uint64_t write(const void *buf, uint64_t size);
  uint64_t write(const void *buf, uint64_t size, uint64_t offset);

  void sync();

  // seek() moves the file pointer and returns the new position.
  uint64_t seek(int64_t offset, int whence = SEEK_SET);
  // tell() returns the current position.
  uint64_t tell() const;

  // resize() resizes the file and moves the file pointer to the new
  // end-of-file.
  void resize(uint64_t size);
  // size() returns the file size in bytes.
  uint64_t size() const;

  // If true, the associated path will be unlinked after closing the file
  // handle.
  bool unlink_at_close() const;
  void set_unlink_at_close(bool value);

  String path() const;
  Flags flags() const;

  const void *handle() const;

  void swap(File &file);

  StringBuilder &write_to(StringBuilder &builder) const;

  static bool exists(const char *path);
  static void unlink(const char *path);
  static bool unlink_if_exists(const char *path);

 private:
  std::shared_ptr<FileImpl> impl_;

  void throw_if_impl_is_invalid() const;
};

inline void swap(File &lhs, File &rhs) {
  lhs.swap(rhs);
}

inline StringBuilder &operator<<(StringBuilder &builder, const File &file) {
  return file.write_to(builder);
}

}  // namespace io
}  // namespace grnxx

#endif  // GRNXX_IO_FILE_HPP
