#include "trie.hpp"
#include "basic_trie.hpp"

namespace grnxx {
namespace map {
namespace da {

TrieOptions::TrieOptions() {}

Trie::Trie() {}
Trie::~Trie() {}

Trie *Trie::create(const TrieOptions &options, io::Pool pool) {
  // TODO
  return nullptr;
}

Trie *Trie::open(io::Pool pool, uint32_t block_id) {
  // TODO
  return nullptr;
}

}  // namespace da
}  // namespace map
}  // namespace grnxx
