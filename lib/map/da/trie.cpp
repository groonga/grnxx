#include "trie.hpp"
#include "basic_trie.hpp"

namespace grnxx {
namespace map {
namespace da {

TrieOptions::TrieOptions()
  : nodes_size(0),
    entries_size(0),
    keys_size(0) {}

Trie::Trie() {}
Trie::~Trie() {}

Trie *Trie::create(const TrieOptions &options, io::Pool pool) {
  return basic::Trie::create(options, pool);
}

Trie *Trie::open(io::Pool pool, uint32_t block_id) {
  return basic::Trie::open(pool, block_id);
}

void Trie::unlink(io::Pool pool, uint32_t block_id) {
  return basic::Trie::unlink(pool, block_id);
}

}  // namespace da
}  // namespace map
}  // namespace grnxx
