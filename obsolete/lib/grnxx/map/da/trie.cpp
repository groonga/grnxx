#include "grnxx/map/da/trie.hpp"

#include "grnxx/logger.hpp"
#include "grnxx/map/da/basic/trie.hpp"
#include "grnxx/map/da/large/trie.hpp"

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
  // Check the type and call the appropriate function.
  auto block_info = pool.get_block_info(block_id);
  auto block_address = pool.get_block_address(*block_info);
  const TrieType type = *static_cast<const TrieType *>(block_address);
  switch (type) {
    case TRIE_UNKNOWN: {
      break;
    }
    case TRIE_BASIC: {
      return basic::Trie::open(pool, block_id);
    }
    case TRIE_LARGE: {
      return large::Trie::open(pool, block_id);
    }
  }

  GRNXX_ERROR() << "unknown trie type";
  GRNXX_THROW();

//  return basic::Trie::open(pool, block_id);
}

void Trie::unlink(io::Pool pool, uint32_t block_id) {
  // Check the type and call the appropriate function.
  auto block_info = pool.get_block_info(block_id);
  auto block_address = pool.get_block_address(*block_info);
  const TrieType type = *static_cast<const TrieType *>(block_address);
  switch (type) {
    case TRIE_UNKNOWN: {
      break;
    }
    case TRIE_BASIC: {
      return basic::Trie::unlink(pool, block_id);
    }
    case TRIE_LARGE: {
      return large::Trie::unlink(pool, block_id);
    }
  }

  GRNXX_ERROR() << "unknown trie type";
  GRNXX_THROW();

//  return basic::Trie::unlink(pool, block_id);
}

}  // namespace da
}  // namespace map
}  // namespace grnxx
