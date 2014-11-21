#ifndef GRNXX_IMPL_MERGER_HPP
#define GRNXX_IMPL_MERGER_HPP

#include "grnxx/merger.hpp"

namespace grnxx {
namespace impl {

using MergerInterface = grnxx::Merger;

class Merger : public MergerInterface {
 public:
  // -- Public API (grnxx/merger.hpp) --

  explicit Merger(const MergerOptions &options);
  virtual ~Merger() = default;

  virtual void reset(Array<Record> *input_records_1,
                     Array<Record> *input_records_2,
                     Array<Record> *output_records);
  virtual void progress();
  virtual void finish() = 0;
  virtual void merge(Array<Record> *input_records_1,
                     Array<Record> *input_records_2,
                     Array<Record> *output_records);

  // -- Internal API --

  // Create an object for merging record sets.
  //
  // On success, returns the merger.
  // On failure, throws an exception.
  static Merger *create(const MergerOptions &options);

 protected:
  Array<Record> *input_records_1_;
  Array<Record> *input_records_2_;
  Array<Record> *output_records_;
  MergerLogicalOperatorType logical_operator_type_;
  MergerScoreOperatorType score_operator_type_;
  Float missing_score_;
  size_t offset_;
  size_t limit_;
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_MERGER_HPP
