#ifndef GRNXX_RECORD_HPP
#define GRNXX_RECORD_HPP

#include "grnxx/array.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

struct Record {
  Int row_id;
  Float score;

  Record() = default;
  Record(Int row_id, Float score) : row_id(row_id), score(score) {}
};

class RecordSubset {
 public:
  RecordSubset() = default;
  RecordSubset(const RecordSubset &) = default;

  RecordSubset &operator=(const RecordSubset &) = default;

  // Return the number of records.
  Int size() const {
    return size_;
  }

  // Return the subset.
  RecordSubset subset(Int offset = 0) const {
    return RecordSubset(records_ + offset, size_ - offset);
  }
  // Return the subset.
  RecordSubset subset(Int offset, Int size) const {
    return RecordSubset(records_ + offset, size);
  }

  // Return the "i"-th record.
  Record get(Int i) const {
    return records_[i];
  }
  // Return the row ID of the "i"-th record.
  Int get_row_id(Int i) const {
    return records_[i].row_id;
  }
  // Return the score of the "i"-th record.
  Float get_score(Int i) const {
    return records_[i].score;
  }

  // Set the "i"-th record.
  void set(Int i, Record record) {
    records_[i] = record;
  }
  // Set the row ID of the "i"-th record.
  void set_row_id(Int i, Int row_id) {
    records_[i].row_id = row_id;
  }
  // Set the score of the "i"-th record.
  void set_score(Int i, Float score) {
    records_[i].score = score;
  }

  // Swap the "i"-th record and the "j"-th record.
  void swap(Int i, Int j) {
    Record temporary_record = get(i);
    set(i, get(j));
    set(j, temporary_record);
  }

 private:
  Record *records_;
  Int size_;

  RecordSubset(Record *records, Int size) : records_(records), size_(size) {}

  friend class RecordSet;
};

class RecordSet {
 public:
  RecordSet() : records_() {}
  ~RecordSet() {}

  // Return the number of records.
  Int size() const {
    return static_cast<Int>(records_.size());
  }

  // Append a record.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool append(Error *error, const Record &record) {
    return records_.push_back(error, record);
  }

  // TODO: The following const_cast can be removed if RecordSet does not
  //       use Array<Record>.

  // Return a subset.
  RecordSubset subset(Int offset = 0) const {
    return RecordSubset(const_cast<Record *>(&records_[offset]),
                        records_.size() - offset);
  }
  // Return a subset.
  RecordSubset subset(Int offset, Int size) const {
    return RecordSubset(const_cast<Record *>(&records_[offset]),
                        records_.size());
  }

  // Return a subset.
  operator RecordSubset() const {
    return subset();
  }

  // If "i" is invalid, the result of the following functions is undefined.

  // Return the "i"-th record.
  Record get(Int i) const {
    return records_[i];
  }
  // Return the row ID of the "i"-th record.
  Int get_row_id(Int i) const {
    return records_[i].row_id;
  }
  // Return the score of the "i"-th record.
  Float get_score(Int i) const {
    return records_[i].score;
  }

  // Set the "i"-th record.
  void set(Int i, Record record) {
    records_[i] = record;
  }
  // Set the row ID of the "i"-th record.
  void set_row_id(Int i, Int row_id) {
    records_[i].row_id = row_id;
  }
  // Set the score of the "i"-th record.
  void set_score(Int i, Float score) {
    records_[i].score = score;
  }

  // Resize the set.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool resize(Error *error, Int size) {
    return records_.resize(error, size);
  }

  // Clear all the records.
  void clear() {
    records_.clear();
  }

 private:
  Array<Record> records_;
};

//class RecordSubset {
// public:
//  explicit RecordSubset(RecordSet *record_set)
//      : record_set_(record_set),
//        offset_(0),
//        size_(record_set_->size()) {}
//  RecordSubset(RecordSet *record_set, Int offset)
//      : record_set_(record_set),
//        offset_(offset),
//        size_(record_set_->size() - offset) {}
//  RecordSubset(RecordSet *record_set, Int offset, Int size)
//      : record_set_(record_set),
//        offset_(offset),
//        size_(size) {}
//  RecordSubset(const RecordSubset &subset) = default;
//  RecordSubset(const RecordSubset &subset, Int offset)
//      : record_set_(subset.record_set_),
//        offset_(subset.offset_ + offset),
//        size_(subset.size() - offset) {}
//  RecordSubset(const RecordSubset &subset, Int offset, Int size)
//      : record_set_(subset.record_set_),
//        offset_(subset.offset_ + offset),
//        size_(size) {}

//  // Return the number of records.
//  Int size() const {
//    return size_;
//  }

//  // Return the "i"-th record.
//  Record get(Int i) const {
//    return record_set_->get(offset_ + i);
//  }
//  // Return the row ID of the "i"-th record.
//  Int get_row_id(Int i) const {
//    return record_set_->get_row_id(offset_ + i);
//  }
//  // Return the score of the "i"-th record.
//  Float get_score(Int i) const {
//    return record_set_->get_score(offset_ + i);
//  }

//  // Set the "i"-th record.
//  void set(Int i, Record record) {
//    record_set_->set(offset_ + i, record);
//  }
//  // Set the row ID of the "i"-th record.
//  void set_row_id(Int i, Int row_id) {
//    record_set_->set_row_id(offset_ + i, row_id);
//  }
//  // Set the score of the "i"-th record.
//  void set_score(Int i, Float score) {
//    record_set_->set_score(offset_ + i, score);
//  }

//  // Swap the "i"-th record and the "j"-th record.
//  void swap(Int i, Int j) {
//    Record temporary_record = get(i);
//    set(i, get(j));
//    set(j, temporary_record);
//  }

// private:
//  RecordSet *record_set_;
//  Int offset_;
//  Int size_;
//};

}  // namespace grnxx

#endif  // GRNXX_RECORD_HPP
