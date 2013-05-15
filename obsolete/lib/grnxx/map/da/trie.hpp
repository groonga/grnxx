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
#ifndef GRNXX_MAP_DA_TRIE_HPP
#define GRNXX_MAP_DA_TRIE_HPP

#include "grnxx/exception.hpp"
#include "grnxx/map.hpp"

namespace grnxx {
namespace map {
namespace da {

enum TrieType : int32_t {
  TRIE_UNKNOWN = 0,
  TRIE_BASIC   = 1,
  TRIE_LARGE   = 2
};

class TrieException : Exception {
 public:
  TrieException() noexcept : Exception() {}
  ~TrieException() noexcept {}

  TrieException(const TrieException &x) noexcept : Exception(x) {}
  TrieException &operator=(const TrieException &) noexcept {
    return *this;
  }

  const char *what() const noexcept {
    return "";
  }
};

struct TrieOptions {
  uint64_t nodes_size;
  uint64_t entries_size;
  uint64_t keys_size;

  TrieOptions();
};

class Trie : public Map {
 public:
  Trie();
  virtual ~Trie();

  static Trie *create(const TrieOptions &options, io::Pool pool);
  static Trie *open(io::Pool pool, uint32_t block_id);

  static void unlink(io::Pool pool, uint32_t block_id);

  virtual Trie *defrag(const TrieOptions &options) = 0;
};

}  // namespace da
}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_DA_TRIE_HPP