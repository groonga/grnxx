#include "grnxx/timer.hpp"

#include <time.h>

namespace grnxx {
namespace {

// 現在の時刻を秒単位で返す．
double get_current_time() {
  // CLOCK_MONOTONIC_RAW は使える環境が限られるため，
  // とりあえずは CLOCK_MONOTONIC を使うことにする．
  struct timespec current_time;
  ::clock_gettime(CLOCK_MONOTONIC, &current_time);
  return current_time.tv_sec + (current_time.tv_nsec * 0.000000001);
}

}  // namespace

// タイマを初期化する．
Timer::Timer() : start_time_(get_current_time()) {}

// タイマが初期化されてからの経過時間を秒単位で返す．
double Timer::elapsed() const {
  return get_current_time() - start_time_;
}

// タイマを初期化する．
void Timer::reset() {
  start_time_ = get_current_time();
}

}  // namespace grnxx
