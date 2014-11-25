#include "grnxx/impl/merger.hpp"

#include <new>
#include <unordered_map>

namespace grnxx {
namespace impl {
namespace merger {

// -- AndMerger --

class AndMerger : public Merger {
 public:
  // -- Public API (grnxx/merger.hpp) --

  AndMerger(const MergerOptions &options) : Merger(options) {}
  ~AndMerger() = default;

  void finish();
};

void AndMerger::finish() {
  // Create a hash table from the smaller input.
  Array<Record> *filter_records;
  Array<Record> *stream_records;
  if (input_records_1_->size() < input_records_2_->size()) {
    filter_records = input_records_1_;
    stream_records = input_records_2_;
  } else {
    filter_records = input_records_2_;
    stream_records = input_records_1_;
  }
  std::unordered_map<int64_t, Float> filter;
  for (size_t i = 0; i < filter_records->size(); ++i) try {
    filter[(*filter_records)[i].row_id.raw()] = (*filter_records)[i].score;
  } catch (const std::bad_alloc &) {
    throw "Memory allocation failed";  // TODO
  }

  // Filter the stream (the larger input) with the hash table.
  const bool stream_is_1 = (stream_records == input_records_1_);
  for (size_t i = 0; i < stream_records->size(); ++i) {
    auto it = filter.find((*stream_records)[i].row_id.raw());
    if (it != filter.end()) {
      Record record;
      record.row_id = Int(it->first);
      switch (score_operator_type_) {
        case MERGER_SCORE_PLUS: {
          record.score = (*stream_records)[i].score + it->second;
          break;
        }
        case MERGER_SCORE_MINUS: {
          if (stream_is_1) {
            record.score = (*stream_records)[i].score - it->second;
          } else {
            record.score = it->second - (*stream_records)[i].score;
          }
          break;
        }
        case MERGER_SCORE_MULTIPLICATION: {
          record.score = (*stream_records)[i].score * it->second;
          break;
        }
        case MERGER_SCORE_LEFT: {
          if (stream_is_1) {
            record.score = (*stream_records)[i].score;
          } else {
            record.score = it->second;
          }
          break;
        }
        case MERGER_SCORE_RIGHT: {
          if (stream_is_1) {
            record.score = it->second;
          } else {
            record.score = (*stream_records)[i].score;
          }
          break;
        }
        case MERGER_SCORE_ZERO: {
          record.score = Float(0.0);
          break;
        }
      }
      output_records_->push_back(record);
    }
  }

  // Remove out-of-range records.
  if (offset_ > 0) {
    for (size_t i = offset_; i < output_records_->size(); ++i) {
      (*output_records_)[i - offset_] = (*output_records_)[i];
    }
    output_records_->resize(output_records_->size() - offset_);
  }
  if (limit_ < output_records_->size()) {
    output_records_->resize(limit_);
  }
  input_records_1_->clear();
  input_records_2_->clear();
}

// -- OrMerger --

class OrMerger : public Merger {
 public:
  // -- Public API (grnxx/merger.hpp) --

  OrMerger(const MergerOptions &options) : Merger(options) {}
  ~OrMerger() = default;

  void finish();
};

void OrMerger::finish() {
  // Create a hash table from the smaller input.
  Array<Record> *filter_records;
  Array<Record> *stream_records;
  if (input_records_1_->size() < input_records_2_->size()) {
    filter_records = input_records_1_;
    stream_records = input_records_2_;
  } else {
    filter_records = input_records_2_;
    stream_records = input_records_1_;
  }
  std::unordered_map<int64_t, Float> filter;
  for (size_t i = 0; i < filter_records->size(); ++i) try {
    filter[(*filter_records)[i].row_id.raw()] = (*filter_records)[i].score;
  } catch (const std::bad_alloc &) {
    throw "Memory allocation failed";  // TODO
  }

  // Filter the stream (the larger input) with the hash table.
  const bool stream_is_1 = (stream_records == input_records_1_);
  for (size_t i = 0; i < stream_records->size(); ++i) {
    Record record;
    record.row_id = (*stream_records)[i].row_id;
    auto it = filter.find((*stream_records)[i].row_id.raw());
    if (it == filter.end()) {
      switch (score_operator_type_) {
        case MERGER_SCORE_PLUS: {
          record.score = (*stream_records)[i].score + missing_score_;
          break;
        }
        case MERGER_SCORE_MINUS: {
          if (stream_is_1) {
            record.score = (*stream_records)[i].score - missing_score_;
          } else {
            record.score = missing_score_ - (*stream_records)[i].score;
          }
          break;
        }
        case MERGER_SCORE_MULTIPLICATION: {
          record.score = (*stream_records)[i].score * missing_score_;
          break;
        }
        case MERGER_SCORE_LEFT: {
          if (stream_is_1) {
            record.score = (*stream_records)[i].score;
          } else {
            record.score = missing_score_;
          }
          break;
        }
        case MERGER_SCORE_RIGHT: {
          if (stream_is_1) {
            record.score = missing_score_;
          } else {
            record.score = (*stream_records)[i].score;
          }
          break;
        }
        case MERGER_SCORE_ZERO: {
          record.score = Float(0.0);
          break;
        }
      }
    } else {
      switch (score_operator_type_) {
        case MERGER_SCORE_PLUS: {
          record.score = it->second + (*stream_records)[i].score;
          break;
        }
        case MERGER_SCORE_MINUS: {
          if (stream_is_1) {
            record.score = (*stream_records)[i].score - it->second;
          } else {
            record.score = it->second - (*stream_records)[i].score;
          }
          break;
        }
        case MERGER_SCORE_MULTIPLICATION: {
          record.score = it->second * (*stream_records)[i].score;
          break;
        }
        case MERGER_SCORE_LEFT: {
          if (stream_is_1) {
            record.score = (*stream_records)[i].score;
          } else {
            record.score = it->second;
          }
          break;
        }
        case MERGER_SCORE_RIGHT: {
          if (!stream_is_1) {
            record.score = (*stream_records)[i].score;
          } else {
            record.score = it->second;
          }
          break;
        }
        case MERGER_SCORE_ZERO: {
          record.score = Float(0.0);
          break;
        }
      }
      filter.erase(it);
    }
    output_records_->push_back(record);
  }

  for (auto it : filter) {
    switch (score_operator_type_) {
      case MERGER_SCORE_PLUS: {
        it.second += missing_score_;
        break;
      }
      case MERGER_SCORE_MINUS: {
        if (stream_is_1) {
          it.second = missing_score_ - it.second;
        } else {
          it.second -= missing_score_;
        }
        break;
      }
      case MERGER_SCORE_MULTIPLICATION: {
        it.second *= missing_score_;
        break;
      }
      case MERGER_SCORE_LEFT: {
        if (stream_is_1) {
          it.second = missing_score_;
        }
        break;
      }
      case MERGER_SCORE_RIGHT: {
        if (!stream_is_1) {
          it.second = missing_score_;
        }
        break;
      }
      case MERGER_SCORE_ZERO: {
        it.second = Float(0.0);
        break;
      }
    }
    output_records_->push_back(Record(Int(it.first), it.second));
  }

  // Remove out-of-range records.
  if (offset_ > 0) {
    for (size_t i = offset_; i < output_records_->size(); ++i) {
      (*output_records_)[i - offset_] = (*output_records_)[i];
    }
    output_records_->resize(output_records_->size() - offset_);
  }
  if (limit_ < output_records_->size()) {
    output_records_->resize(limit_);
  }
  input_records_1_->clear();
  input_records_2_->clear();
}

class XorMerger : public Merger {
 public:
  // -- Public API (grnxx/merger.hpp) --

  XorMerger(const MergerOptions &options) : Merger(options) {}
  ~XorMerger() = default;

  void finish();
};

void XorMerger::finish() {
  // Create a hash table from the smaller input.
  Array<Record> *filter_records;
  Array<Record> *stream_records;
  if (input_records_1_->size() < input_records_2_->size()) {
    filter_records = input_records_1_;
    stream_records = input_records_2_;
  } else {
    filter_records = input_records_2_;
    stream_records = input_records_1_;
  }
  std::unordered_map<int64_t, Float> filter;
  for (size_t i = 0; i < filter_records->size(); ++i) try {
    filter[(*filter_records)[i].row_id.raw()] = (*filter_records)[i].score;
  } catch (...) {
    throw "Memory allocation failed";  // TODO
  }

  // Filter the stream (the larger input) with the hash table.
  const bool stream_is_1 = (stream_records == input_records_1_);
  for (size_t i = 0; i < stream_records->size(); ++i) {
    auto it = filter.find((*stream_records)[i].row_id.raw());
    if (it != filter.end()) {
      filter.erase(it);
    } else {
      Record record;
      record.row_id = (*stream_records)[i].row_id;
      switch (score_operator_type_) {
        case MERGER_SCORE_PLUS: {
          record.score = (*stream_records)[i].score + missing_score_;
          break;
        }
        case MERGER_SCORE_MINUS: {
          if (stream_is_1) {
            record.score = (*stream_records)[i].score - missing_score_;
          } else {
            record.score = missing_score_ - (*stream_records)[i].score;
          }
          break;
        }
        case MERGER_SCORE_MULTIPLICATION: {
          record.score = (*stream_records)[i].score * missing_score_;
          break;
        }
        case MERGER_SCORE_LEFT: {
          if (stream_is_1) {
            record.score = (*stream_records)[i].score;
          } else {
            record.score = missing_score_;
          }
          break;
        }
        case MERGER_SCORE_RIGHT: {
          if (stream_is_1) {
            record.score = missing_score_;
          } else {
            record.score = (*stream_records)[i].score;
          }
          break;
        }
        case MERGER_SCORE_ZERO: {
          record.score = Float(0.0);
          break;
        }
      }
      output_records_->push_back(record);
    }
  }

  for (auto it : filter) {
    switch (score_operator_type_) {
      case MERGER_SCORE_PLUS: {
        it.second += missing_score_;
        break;
      }
      case MERGER_SCORE_MINUS: {
        if (stream_is_1) {
          it.second = missing_score_ - it.second;
        } else {
          it.second -= missing_score_;
        }
        break;
      }
      case MERGER_SCORE_MULTIPLICATION: {
        it.second *= missing_score_;
        break;
      }
      case MERGER_SCORE_LEFT: {
        if (stream_is_1) {
          it.second = missing_score_;
        }
        break;
      }
      case MERGER_SCORE_RIGHT: {
        if (!stream_is_1) {
          it.second = missing_score_;
        }
        break;
      }
      case MERGER_SCORE_ZERO: {
        it.second = Float(0.0);
        break;
      }
    }
    output_records_->push_back(Record(Int(it.first), it.second));
  }

  // Remove out-of-range records.
  if (offset_ > 0) {
    for (size_t i = offset_; i < output_records_->size(); ++i) {
      (*output_records_)[i - offset_] = (*output_records_)[i];
    }
    output_records_->resize(output_records_->size() - offset_);
  }
  if (limit_ < output_records_->size()) {
    output_records_->resize(limit_);
  }
  input_records_1_->clear();
  input_records_2_->clear();
}

class MinusMerger : public Merger {
 public:
  // -- Public API (grnxx/merger.hpp) --

  MinusMerger(const MergerOptions &options) : Merger(options) {}
  ~MinusMerger() = default;

  void finish();
};

void MinusMerger::finish() {
  // Create a hash table from the smaller input.
  Array<Record> *filter_records;
  Array<Record> *stream_records;
  if (input_records_1_->size() < input_records_2_->size()) {
    filter_records = input_records_1_;
    stream_records = input_records_2_;
  } else {
    filter_records = input_records_2_;
    stream_records = input_records_1_;
  }
  std::unordered_map<int64_t, Float> filter;
  for (size_t i = 0; i < filter_records->size(); ++i) try {
    filter[(*filter_records)[i].row_id.raw()] = (*filter_records)[i].score;
  } catch (...) {
    throw "Memory allocation failed";  // TODO
  }

  // Filter the stream (the larger input) with the hash table.
  const bool stream_is_1 = (stream_records == input_records_1_);
  if (stream_is_1) {
    for (size_t i = 0; i < stream_records->size(); ++i) {
      auto it = filter.find((*stream_records)[i].row_id.raw());
      if (it != filter.end()) {
        continue;
      }
      Record record = stream_records->get(i);
      switch (score_operator_type_) {
        case MERGER_SCORE_PLUS: {
          record.score += missing_score_;
          break;
        }
        case MERGER_SCORE_MINUS: {
          record.score -= missing_score_;
          break;
        }
        case MERGER_SCORE_MULTIPLICATION: {
          record.score *= missing_score_;
          break;
        }
        case MERGER_SCORE_LEFT: {
          break;
        }
        case MERGER_SCORE_RIGHT: {
          record.score = missing_score_;
          break;
        }
        case MERGER_SCORE_ZERO: {
          record.score = Float(0.0);
          break;
        }
      }
      output_records_->push_back(record);
    }
  } else {
    for (size_t i = 0; i < stream_records->size(); ++i) {
      auto it = filter.find((*stream_records)[i].row_id.raw());
      if (it != filter.end()) {
        filter.erase(it);
      }
    }
    for (auto it : filter) {
      Record record;
      record.row_id = Int(it.first);
      switch (score_operator_type_) {
        case MERGER_SCORE_PLUS: {
          record.score = it.second + missing_score_;
          break;
        }
        case MERGER_SCORE_MINUS: {
          record.score = it.second - missing_score_;
          break;
        }
        case MERGER_SCORE_MULTIPLICATION: {
          record.score = it.second * missing_score_;
          break;
        }
        case MERGER_SCORE_LEFT: {
          record.score = it.second;
          break;
        }
        case MERGER_SCORE_RIGHT: {
          record.score = missing_score_;
          break;
        }
        case MERGER_SCORE_ZERO: {
          record.score = Float(0.0);
          break;
        }
      }
      output_records_->push_back(record);
    }
  }

  // Remove out-of-range records.
  if (offset_ > 0) {
    for (size_t i = offset_; i < output_records_->size(); ++i) {
      (*output_records_)[i - offset_] = (*output_records_)[i];
    }
    output_records_->resize(output_records_->size() - offset_);
  }
  if (limit_ < output_records_->size()) {
    output_records_->resize(limit_);
  }
  input_records_1_->clear();
  input_records_2_->clear();
}

class LeftMerger : public Merger {
 public:
  // -- Public API (grnxx/merger.hpp) --

  LeftMerger(const MergerOptions &options) : Merger(options) {}
  ~LeftMerger() = default;

  void finish();
};

void LeftMerger::finish() {
  // Create a hash table from the second input.
  std::unordered_map<int64_t, Float> filter;
  for (size_t i = 0; i < input_records_2_->size(); ++i) {
    filter[(*input_records_2_)[i].row_id.raw()] =
        (*input_records_2_)[i].score;
  }

  // Adjust score of the first input.
  for (size_t i = 0; i < input_records_1_->size(); ++i) {
    Record record = input_records_1_->get(i);
    auto it = filter.find(record.row_id.raw());
    if (it != filter.end()) {
      switch (score_operator_type_) {
        case MERGER_SCORE_PLUS: {
          record.score += it->second;
          break;
        }
        case MERGER_SCORE_MINUS: {
          record.score -= it->second;
          break;
        }
        case MERGER_SCORE_MULTIPLICATION: {
          record.score *= it->second;
          break;
        }
        case MERGER_SCORE_LEFT: {
          break;
        }
        case MERGER_SCORE_RIGHT: {
          record.score = it->second;
          break;
        }
        case MERGER_SCORE_ZERO: {
          record.score = Float(0.0);
          break;
        }
      }
    } else {
      switch (score_operator_type_) {
        case MERGER_SCORE_PLUS: {
          record.score += missing_score_;
          break;
        }
        case MERGER_SCORE_MINUS: {
          record.score -= missing_score_;
          break;
        }
        case MERGER_SCORE_MULTIPLICATION: {
          record.score *= missing_score_;
          break;
        }
        case MERGER_SCORE_LEFT: {
          break;
        }
        case MERGER_SCORE_RIGHT: {
          record.score = missing_score_;
          break;
        }
        case MERGER_SCORE_ZERO: {
          record.score = Float(0.0);
          break;
        }
      }
    }
    output_records_->push_back(record);
  }

  // Remove out-of-range records.
  if (offset_ > 0) {
    for (size_t i = offset_; i < output_records_->size(); ++i) {
      (*output_records_)[i - offset_] = (*output_records_)[i];
    }
    output_records_->resize(output_records_->size() - offset_);
  }
  if (limit_ < output_records_->size()) {
    output_records_->resize(limit_);
  }
  input_records_1_->clear();
  input_records_2_->clear();
}

class RightMerger : public Merger {
 public:
  // -- Public API (grnxx/merger.hpp) --

  RightMerger(const MergerOptions &options) : Merger(options) {}
  ~RightMerger() = default;

  void finish();
};

void RightMerger::finish() {
  // Create a hash table from the first input.
  std::unordered_map<int64_t, Float> filter;
  for (size_t i = 0; i < input_records_1_->size(); ++i) {
    filter[(*input_records_1_)[i].row_id.raw()] =
        (*input_records_1_)[i].score;
  }

  // Adjust score of the first input.
  for (size_t i = 0; i < input_records_2_->size(); ++i) {
    Record record;
    record.row_id = (*input_records_2_)[i].row_id;
    auto it = filter.find(record.row_id.raw());
    if (it != filter.end()) {
      switch (score_operator_type_) {
        case MERGER_SCORE_PLUS: {
          record.score = it->second + (*input_records_2_)[i].score;
          break;
        }
        case MERGER_SCORE_MINUS: {
          record.score = it->second - (*input_records_2_)[i].score;
          break;
        }
        case MERGER_SCORE_MULTIPLICATION: {
          record.score = it->second * (*input_records_2_)[i].score;
          break;
        }
        case MERGER_SCORE_LEFT: {
          record.score = it->second;
          break;
        }
        case MERGER_SCORE_RIGHT: {
          record.score = (*input_records_2_)[i].score;
          break;
        }
        case MERGER_SCORE_ZERO: {
          record.score = Float(0.0);
          break;
        }
      }
    } else {
      switch (score_operator_type_) {
        case MERGER_SCORE_PLUS: {
          record.score = missing_score_ + (*input_records_2_)[i].score;
          break;
        }
        case MERGER_SCORE_MINUS: {
          record.score = missing_score_ - (*input_records_2_)[i].score;
          break;
        }
        case MERGER_SCORE_MULTIPLICATION: {
          record.score = missing_score_ * (*input_records_2_)[i].score;
          break;
        }
        case MERGER_SCORE_LEFT: {
          record.score = missing_score_;
          break;
        }
        case MERGER_SCORE_RIGHT: {
          record.score = (*input_records_2_)[i].score;
          break;
        }
        case MERGER_SCORE_ZERO: {
          record.score = Float(0.0);
          break;
        }
      }
    }
    output_records_->push_back(record);
  }

  // Remove out-of-range records.
  if (offset_ > 0) {
    for (size_t i = offset_; i < output_records_->size(); ++i) {
      (*output_records_)[i - offset_] = (*output_records_)[i];
    }
    output_records_->resize(output_records_->size() - offset_);
  }
  if (limit_ < output_records_->size()) {
    output_records_->resize(limit_);
  }
  input_records_1_->clear();
  input_records_2_->clear();
}

}  // namespace merger

using namespace merger;

Merger::Merger(const MergerOptions &options)
    : MergerInterface(),
      input_records_1_(nullptr),
      input_records_2_(nullptr),
      output_records_(nullptr),
      logical_operator_type_(options.logical_operator_type),
      score_operator_type_(options.score_operator_type),
      missing_score_(options.missing_score),
      offset_(options.offset),
      limit_(options.limit) {}

void Merger::reset(Array<Record> *input_records_1,
                   Array<Record> *input_records_2,
                   Array<Record> *output_records) {
  input_records_1_ = input_records_1;
  input_records_2_ = input_records_2;
  output_records_ = output_records;
}

void Merger::progress() {
  // TODO: Incremental merging is not supported yet.
}

void Merger::merge(Array<Record> *input_records_1,
                   Array<Record> *input_records_2,
                   Array<Record> *output_records) {
  reset(input_records_1, input_records_2, output_records);
  finish();
}

Merger *Merger::create(const MergerOptions &options) try {
  switch (options.logical_operator_type) {
    case MERGER_LOGICAL_AND: {
      return new AndMerger(options);
    }
    case MERGER_LOGICAL_OR: {
      return new OrMerger(options);
    }
    case MERGER_LOGICAL_XOR: {
      return new XorMerger(options);
    }
    case MERGER_LOGICAL_MINUS: {
      return new MinusMerger(options);
    }
    case MERGER_LOGICAL_LEFT: {
      return new LeftMerger(options);
    }
    case MERGER_LOGICAL_RIGHT: {
      return new RightMerger(options);
    }
    default: {
      throw "Invalid operator type";  // TODO
    }
  }
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

}  // namespace impl
}  // namespace grnxx
