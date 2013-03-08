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
#ifndef GRNXX_MAP_DOUBLE_ARRAY_HPP
#define GRNXX_MAP_DOUBLE_ARRAY_HPP

#include "map.hpp"
#include "map/da/trie.hpp"

namespace grnxx {
namespace map {

struct DoubleArrayHeader {
  MapHeader map_header;
  uint32_t front_block_id;
  uint32_t back_block_id;

  DoubleArrayHeader();
};

class DoubleArray : public Map {
 public:
  ~DoubleArray();

  static DoubleArray *create(const MapOptions &options, io::Pool pool);
  static DoubleArray *open(io::Pool pool, uint32_t block_id);

  static void unlink(io::Pool pool, uint32_t block_id);

  uint32_t block_id() const;

  bool search(int64_t key_id, MapKey *key = nullptr);
  bool search(const Slice &key, int64_t *key_id = nullptr);

  bool lcp_search(const Slice &query, int64_t *key_id = nullptr,
                  MapKey *key = nullptr);

  bool insert(const Slice &key, int64_t *key_id = nullptr);

  bool remove(int64_t key_id);
  bool remove(const Slice &key);

  bool update(int64_t key_id, const Slice &dest_key);
  bool update(const Slice &src_key, const Slice &dest_key,
              int64_t *key_id = nullptr);

 private:
  io::Pool pool_;
  const io::BlockInfo *block_info_;
  DoubleArrayHeader *header_;
  std::unique_ptr<da::Trie> front_;
  std::unique_ptr<da::Trie> back_;
  uint32_t front_block_id_;
  Mutex inter_thread_mutex_;

  DoubleArray();

  void create_double_array(const MapOptions &options, io::Pool pool);
  void open_double_array(io::Pool pool, uint32_t block_id);

  bool open_trie_if_needed();
  void defrag_trie();
};

}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_DOUBLE_ARRAY_HPP
