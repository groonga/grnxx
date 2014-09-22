#include "grnxx/merger.hpp"

#include <unordered_map>

namespace grnxx {

// -- AndMerger --

class AndMerger : public Merger {
 public:
  ~AndMerger() {}

  static unique_ptr<Merger> create(Error *error, const MergerOptions &options);

  bool reset(Error *error,
             Array<Record> *input_records_1,
             Array<Record> *input_records_2,
             Array<Record> *output_records);

  bool progress(Error *error);

  bool finish(Error *error);

 private:
  Array<Record> *input_records_1_;
  Array<Record> *input_records_2_;
  Array<Record> *output_records_;
  MergerOperatorType operator_type_;
  Int offset_;
  Int limit_;

  AndMerger(MergerOperatorType operator_type, Int offset, Int limit)
      : Merger(),
        input_records_1_(nullptr),
        input_records_2_(nullptr),
        output_records_(nullptr),
        operator_type_(operator_type),
        offset_(offset),
        limit_(limit) {}
};

unique_ptr<Merger> AndMerger::create(Error *error,
                                     const MergerOptions &options) {
  unique_ptr<Merger> merger(
      new (nothrow) AndMerger(options.operator_type,
                              options.offset,
                              options.limit));
  if (!merger) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return merger;
}

bool AndMerger::reset(Error *,
                      Array<Record> *input_records_1,
                      Array<Record> *input_records_2,
                      Array<Record> *output_records) {
  input_records_1_ = input_records_1;
  input_records_2_ = input_records_2;
  output_records_ = output_records;
  return true;
}

bool AndMerger::progress(Error *) {
  // TODO: Incremental merging is not supported yet.
  return true;
}

bool AndMerger::finish(Error *error) {
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
  std::unordered_map<Int, Float> filter;
  for (Int i = 0; i < filter_records->size(); ++i) try {
    filter[filter_records->get_row_id(i)] = filter_records->get_score(i);
  } catch (...) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }

  // Filter the stream (the larger input) with the hash table.
  const MergerOperatorType operator_type = operator_type_;
  const bool stream_is_1 = stream_records == input_records_1_;
  for (Int i = 0; i < stream_records->size(); ++i) {
    auto it = filter.find(stream_records->get_row_id(i));
    if (it != filter.end()) {
      Record record;
      record.row_id = it->first;
      switch (operator_type) {
        case PLUS_MERGER_OPERATOR: {
          record.score = stream_records->get_score(i) + it->second;
          break;
        }
        case MINUS_MERGER_OPERATOR: {
          if (stream_is_1) {
            record.score = stream_records->get_score(i) - it->second;
          } else {
            record.score = it->second - stream_records->get_score(i);
          }
        }
        case MULTIPLICATION_MERGER_OPERATOR: {
          record.score = stream_records->get_score(i) * it->second;
          break;
        }
        case LHS_MERGER_OPERATOR: {
          if (stream_is_1) {
            record.score = stream_records->get_score(i);
          } else {
            record.score = it->second;
          }
          break;
        }
        case RHS_MERGER_OPERATOR: {
          if (stream_is_1) {
            record.score = it->second;
          } else {
            record.score = stream_records->get_score(i);
          }
          break;
        }
        case ZERO_MERGER_OPERATOR: {
          record.score = 0.0;
          break;
        }
      }
      if (!output_records_->push_back(error, record)) {
        return false;
      }
    }
  }

  // Remove out-of-range records.
  if (offset_ > 0) {
    for (Int i = offset_; i < output_records_->size(); ++i) {
      output_records_->set(i - offset_, output_records_->get(i));
    }
    output_records_->resize(nullptr, output_records_->size() - offset_);
  }
  if (limit_ < output_records_->size()) {
    output_records_->resize(nullptr, limit_);
  }
  input_records_1_->clear();
  input_records_2_->clear();
  return true;
}

// -- Merger --

Merger::Merger() {}

Merger::~Merger() {}

unique_ptr<Merger> Merger::create(Error *error, const MergerOptions &options) {
  switch (options.type) {
    case AND_MERGER: {
      return AndMerger::create(error, options);
    }
    case OR_MERGER:
    case XOR_MERGER:
    case MINUS_MERGER:
    case LHS_MERGER:
    case RHS_MERGER: {
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
      return nullptr;
    }
  }
}

bool Merger::merge(Error *error,
                   Array<Record> *input_records_1,
                   Array<Record> *input_records_2,
                   Array<Record> *output_records) {
  if (!reset(error, input_records_1, input_records_2, output_records)) {
    return false;
  }
  return finish(error);
}

}  // namespace grnxx
