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
          break;
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

// -- OrMerger --

class OrMerger : public Merger {
 public:
  ~OrMerger() {}

  static unique_ptr<Merger> create(Error *error, const MergerOptions &options);

  bool reset(Error *error,
             Array<Record> *input_records_1,
             Array<Record> *input_records_2,
             Array<Record> *output_records);

  bool finish(Error *error);

 private:
  Array<Record> *input_records_1_;
  Array<Record> *input_records_2_;
  Array<Record> *output_records_;
  MergerOperatorType operator_type_;
  Int offset_;
  Int limit_;

  OrMerger(MergerOperatorType operator_type, Int offset, Int limit)
      : Merger(),
        input_records_1_(nullptr),
        input_records_2_(nullptr),
        output_records_(nullptr),
        operator_type_(operator_type),
        offset_(offset),
        limit_(limit) {}
};

unique_ptr<Merger> OrMerger::create(Error *error,
                                    const MergerOptions &options) {
  unique_ptr<Merger> merger(
      new (nothrow) OrMerger(options.operator_type,
                             options.offset,
                             options.limit));
  if (!merger) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return merger;
}

bool OrMerger::reset(Error *,
                     Array<Record> *input_records_1,
                     Array<Record> *input_records_2,
                     Array<Record> *output_records) {
  input_records_1_ = input_records_1;
  input_records_2_ = input_records_2;
  output_records_ = output_records;
  return true;
}

bool OrMerger::finish(Error *error) {
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
    Record record;
    record.row_id = stream_records->get_row_id(i);
    auto it = filter.find(stream_records->get_row_id(i));
    if (it == filter.end()) {
      switch (operator_type) {
        case PLUS_MERGER_OPERATOR: {
          record.score = stream_records->get_score(i);
          break;
        }
        case MINUS_MERGER_OPERATOR: {
          record.score = stream_records->get_score(i);
          if (!stream_is_1) {
            record.score = -record.score;
          }
          break;
        }
        case MULTIPLICATION_MERGER_OPERATOR: {
          // TODO: I'm not sure if stream_records->get_score(i) should be used?
          record.score = 0.0;
          break;
        }
        case LHS_MERGER_OPERATOR: {
          if (stream_is_1) {
            record.score = stream_records->get_score(i);
          } else {
            record.score = 0.0;
          }
          break;
        }
        case RHS_MERGER_OPERATOR: {
          if (stream_is_1) {
            record.score = 0.0;
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
    } else {
      switch (operator_type) {
        case PLUS_MERGER_OPERATOR: {
          record.score = it->second + stream_records->get_score(i);
          break;
        }
        case MINUS_MERGER_OPERATOR: {
          if (stream_is_1) {
            record.score = stream_records->get_score(i) - it->second;
          } else {
            record.score = it->second - stream_records->get_score(i);
          }
          break;
        }
        case MULTIPLICATION_MERGER_OPERATOR: {
          record.score = it->second * stream_records->get_score(i);
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
          if (!stream_is_1) {
            record.score = stream_records->get_score(i);
          } else {
            record.score = it->second;
          }
          break;
        }
        case ZERO_MERGER_OPERATOR: {
          record.score = 0.0;
          break;
        }
      }
      filter.erase(it);
    }
    if (!output_records_->push_back(error, record)) {
      return false;
    }
  }

  for (auto it : filter) {
    switch (operator_type) {
      case PLUS_MERGER_OPERATOR: {
        break;
      }
      case MINUS_MERGER_OPERATOR: {
        if (stream_is_1) {
          it.second = -it.second;
        }
        break;
      }
      case MULTIPLICATION_MERGER_OPERATOR: {
        // TODO: I'm not sure if it.second should be used?
        it.second = 0.0;
        break;
      }
      case LHS_MERGER_OPERATOR: {
        if (stream_is_1) {
          it.second = 0.0;
        }
        break;
      }
      case RHS_MERGER_OPERATOR: {
        if (!stream_is_1) {
          it.second = 0.0;
        }
        break;
      }
      case ZERO_MERGER_OPERATOR: {
        it.second = 0.0;
        break;
      }
    }
    if (!output_records_->push_back(error, Record(it.first, it.second))) {
      return false;
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

// -- XorMerger --

class XorMerger : public Merger {
 public:
  ~XorMerger() {}

  static unique_ptr<Merger> create(Error *error, const MergerOptions &options);

  bool reset(Error *error,
             Array<Record> *input_records_1,
             Array<Record> *input_records_2,
             Array<Record> *output_records);

  bool finish(Error *error);

 private:
  Array<Record> *input_records_1_;
  Array<Record> *input_records_2_;
  Array<Record> *output_records_;
  MergerOperatorType operator_type_;
  Int offset_;
  Int limit_;

  XorMerger(MergerOperatorType operator_type, Int offset, Int limit)
      : Merger(),
        input_records_1_(nullptr),
        input_records_2_(nullptr),
        output_records_(nullptr),
        operator_type_(operator_type),
        offset_(offset),
        limit_(limit) {}
};

unique_ptr<Merger> XorMerger::create(Error *error,
                                     const MergerOptions &options) {
  unique_ptr<Merger> merger(
      new (nothrow) XorMerger(options.operator_type,
                              options.offset,
                              options.limit));
  if (!merger) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return merger;
}

bool XorMerger::reset(Error *,
                      Array<Record> *input_records_1,
                      Array<Record> *input_records_2,
                      Array<Record> *output_records) {
  input_records_1_ = input_records_1;
  input_records_2_ = input_records_2;
  output_records_ = output_records;
  return true;
}

bool XorMerger::finish(Error *error) {
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
      filter.erase(it);
    } else {
      Record record;
      record.row_id = stream_records->get_row_id(i);
      switch (operator_type) {
        case PLUS_MERGER_OPERATOR: {
          record.score = stream_records->get_score(i);
          break;
        }
        case MINUS_MERGER_OPERATOR: {
          record.score = stream_records->get_score(i);
          if (!stream_is_1) {
            record.score = -record.score;
          }
          break;
        }
        case MULTIPLICATION_MERGER_OPERATOR: {
          // TODO: I'm not sure if stream_records->get_score(i) should be used?
          record.score = 0.0;
          break;
        }
        case LHS_MERGER_OPERATOR: {
          if (stream_is_1) {
            record.score = stream_records->get_score(i);
          } else {
            record.score = 0.0;
          }
          break;
        }
        case RHS_MERGER_OPERATOR: {
          if (stream_is_1) {
            record.score = 0.0;
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

  for (auto it : filter) {
    switch (operator_type) {
      case PLUS_MERGER_OPERATOR: {
        break;
      }
      case MINUS_MERGER_OPERATOR: {
        if (stream_is_1) {
          it.second = -it.second;
        }
        break;
      }
      case MULTIPLICATION_MERGER_OPERATOR: {
        // TODO: I'm not sure if it.second should be used?
        it.second = 0.0;
        break;
      }
      case LHS_MERGER_OPERATOR: {
        if (stream_is_1) {
          it.second = 0.0;
        }
        break;
      }
      case RHS_MERGER_OPERATOR: {
        if (!stream_is_1) {
          it.second = 0.0;
        }
        break;
      }
      case ZERO_MERGER_OPERATOR: {
        it.second = 0.0;
        break;
      }
    }
    if (!output_records_->push_back(error, Record(it.first, it.second))) {
      return false;
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

// -- MinusMerger --

class MinusMerger : public Merger {
 public:
  ~MinusMerger() {}

  static unique_ptr<Merger> create(Error *error, const MergerOptions &options);

  bool reset(Error *error,
             Array<Record> *input_records_1,
             Array<Record> *input_records_2,
             Array<Record> *output_records);

  bool finish(Error *error);

 private:
  Array<Record> *input_records_1_;
  Array<Record> *input_records_2_;
  Array<Record> *output_records_;
  MergerOperatorType operator_type_;
  Int offset_;
  Int limit_;

  MinusMerger(MergerOperatorType operator_type, Int offset, Int limit)
      : Merger(),
        input_records_1_(nullptr),
        input_records_2_(nullptr),
        output_records_(nullptr),
        operator_type_(operator_type),
        offset_(offset),
        limit_(limit) {}
};

unique_ptr<Merger> MinusMerger::create(Error *error,
                                       const MergerOptions &options) {
  unique_ptr<Merger> merger(
      new (nothrow) MinusMerger(options.operator_type,
                                options.offset,
                                options.limit));
  if (!merger) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return merger;
}

bool MinusMerger::reset(Error *,
                        Array<Record> *input_records_1,
                        Array<Record> *input_records_2,
                        Array<Record> *output_records) {
  input_records_1_ = input_records_1;
  input_records_2_ = input_records_2;
  output_records_ = output_records;
  return true;
}

bool MinusMerger::finish(Error *error) {
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
      filter.erase(it);
    }
  }

  for (auto it : filter) {
    Record record;
    record.row_id = it.first;
    switch (operator_type) {
      case PLUS_MERGER_OPERATOR: {
        break;
      }
      case MINUS_MERGER_OPERATOR: {
        record.score = stream_is_1 ? -it.second : it.second;
        break;
      }
      case MULTIPLICATION_MERGER_OPERATOR: {
        // TODO: I'm not sure if it.second should be used?
        record.score = 0.0;
        break;
      }
      case LHS_MERGER_OPERATOR: {
        record.score = stream_is_1 ? 0.0 : it.second;
        break;
      }
      case RHS_MERGER_OPERATOR: {
        record.score = stream_is_1 ? it.second : 0.0;
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

// -- LhsMerger --

class LhsMerger : public Merger {
 public:
  ~LhsMerger() {}

  static unique_ptr<Merger> create(Error *error, const MergerOptions &options);

  bool reset(Error *error,
             Array<Record> *input_records_1,
             Array<Record> *input_records_2,
             Array<Record> *output_records);

  bool finish(Error *error);

 private:
  Array<Record> *input_records_1_;
  Array<Record> *input_records_2_;
  Array<Record> *output_records_;
  MergerOperatorType operator_type_;
  Int offset_;
  Int limit_;

  LhsMerger(MergerOperatorType operator_type, Int offset, Int limit)
      : Merger(),
        input_records_1_(nullptr),
        input_records_2_(nullptr),
        output_records_(nullptr),
        operator_type_(operator_type),
        offset_(offset),
        limit_(limit) {}
};

unique_ptr<Merger> LhsMerger::create(Error *error,
                                     const MergerOptions &options) {
  unique_ptr<Merger> merger(
      new (nothrow) LhsMerger(options.operator_type,
                              options.offset,
                              options.limit));
  if (!merger) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return merger;
}

bool LhsMerger::reset(Error *,
                      Array<Record> *input_records_1,
                      Array<Record> *input_records_2,
                      Array<Record> *output_records) {
  input_records_1_ = input_records_1;
  input_records_2_ = input_records_2;
  output_records_ = output_records;
  return true;
}

bool LhsMerger::finish(Error *error) {
  // Create a hash table from the second input.
  std::unordered_map<Int, Float> filter;
  for (Int i = 0; i < input_records_2_->size(); ++i) {
    filter[input_records_2_->get_row_id(i)] = input_records_2_->get_score(i);
  }

  // Adjust score of the first input.
  const MergerOperatorType operator_type = operator_type_;
  for (Int i = 0; i < input_records_1_->size(); ++i) {
    Record record = input_records_1_->get(i);
    auto it = filter.find(record.row_id);
    if (it != filter.end()) {
      switch (operator_type) {
        case PLUS_MERGER_OPERATOR: {
          record.score += it->second;
          break;
        }
        case MINUS_MERGER_OPERATOR: {
          record.score -= it->second;
          break;
        }
        case MULTIPLICATION_MERGER_OPERATOR: {
          record.score *= it->second;
          break;
        }
        case LHS_MERGER_OPERATOR: {
          break;
        }
        case RHS_MERGER_OPERATOR: {
          record.score = it->second;
          break;
        }
        case ZERO_MERGER_OPERATOR: {
          record.score = 0.0;
          break;
        }
      }
    } else {
      switch (operator_type) {
        case PLUS_MERGER_OPERATOR:
        case MINUS_MERGER_OPERATOR: {
          break;
        }
        case MULTIPLICATION_MERGER_OPERATOR: {
          // TODO: I'm not sure if it->second should be used?
          record.score = 0.0;
          break;
        }
        case LHS_MERGER_OPERATOR: {
          break;
        }
        case RHS_MERGER_OPERATOR:
        case ZERO_MERGER_OPERATOR: {
          record.score = 0.0;
          break;
        }
      }
    }
    if (!output_records_->push_back(error, record)) {
      return false;
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

// -- RhsMerger --


class RhsMerger : public Merger {
 public:
  ~RhsMerger() {}

  static unique_ptr<Merger> create(Error *error, const MergerOptions &options);

  bool reset(Error *error,
             Array<Record> *input_records_1,
             Array<Record> *input_records_2,
             Array<Record> *output_records);

  bool finish(Error *error);

 private:
  Array<Record> *input_records_1_;
  Array<Record> *input_records_2_;
  Array<Record> *output_records_;
  MergerOperatorType operator_type_;
  Int offset_;
  Int limit_;

  RhsMerger(MergerOperatorType operator_type, Int offset, Int limit)
      : Merger(),
        input_records_1_(nullptr),
        input_records_2_(nullptr),
        output_records_(nullptr),
        operator_type_(operator_type),
        offset_(offset),
        limit_(limit) {}
};

unique_ptr<Merger> RhsMerger::create(Error *error,
                                     const MergerOptions &options) {
  unique_ptr<Merger> merger(
      new (nothrow) RhsMerger(options.operator_type,
                              options.offset,
                              options.limit));
  if (!merger) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return merger;
}

bool RhsMerger::reset(Error *,
                      Array<Record> *input_records_1,
                      Array<Record> *input_records_2,
                      Array<Record> *output_records) {
  input_records_1_ = input_records_1;
  input_records_2_ = input_records_2;
  output_records_ = output_records;
  return true;
}

bool RhsMerger::finish(Error *error) {
  // Create a hash table from the first input.
  std::unordered_map<Int, Float> filter;
  for (Int i = 0; i < input_records_1_->size(); ++i) {
    filter[input_records_1_->get_row_id(i)] = input_records_1_->get_score(i);
  }

  // Adjust score of the first input.
  const MergerOperatorType operator_type = operator_type_;
  for (Int i = 0; i < input_records_2_->size(); ++i) {
    Record record;
    record.row_id = input_records_2_->get_row_id(i);
    auto it = filter.find(record.row_id);
    if (it != filter.end()) {
      switch (operator_type) {
        case PLUS_MERGER_OPERATOR: {
          record.score = it->second + input_records_2_->get_score(i);
          break;
        }
        case MINUS_MERGER_OPERATOR: {
          record.score = it->second - input_records_2_->get_score(i);
          break;
        }
        case MULTIPLICATION_MERGER_OPERATOR: {
          record.score = it->second * input_records_2_->get_score(i);
          break;
        }
        case LHS_MERGER_OPERATOR: {
          record.score = it->second;
          break;
        }
        case RHS_MERGER_OPERATOR: {
          record.score = input_records_2_->get_score(i);
          break;
        }
        case ZERO_MERGER_OPERATOR: {
          record.score = 0.0;
          break;
        }
      }
    } else {
      switch (operator_type) {
        case PLUS_MERGER_OPERATOR: {
          record.score = input_records_2_->get_score(i);
          break;
        }
        case MINUS_MERGER_OPERATOR: {
          record.score = -input_records_2_->get_score(i);
          break;
        }
        case MULTIPLICATION_MERGER_OPERATOR: {
          // TODO: I'm not sure if input_records_2_->get_score(i) should be used?
          record.score = 0.0;
          break;
        }
        case LHS_MERGER_OPERATOR: {
          record.score = 0.0;
          break;
        }
        case RHS_MERGER_OPERATOR: {
          record.score = input_records_2_->get_score(i);
          break;
        }
        case ZERO_MERGER_OPERATOR: {
          record.score = 0.0;
          break;
        }
      }
    }
    if (!output_records_->push_back(error, record)) {
      return false;
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
    case OR_MERGER: {
      return OrMerger::create(error, options);
    }
    case XOR_MERGER: {
      return XorMerger::create(error, options);
    }
    case MINUS_MERGER: {
      return MinusMerger::create(error, options);
    }
    case LHS_MERGER: {
      return LhsMerger::create(error, options);
    }
    case RHS_MERGER: {
      return RhsMerger::create(error, options);
    }
  }
}

bool Merger::progress(Error *) {
  // TODO: Incremental merging is not supported yet.
  return true;
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
