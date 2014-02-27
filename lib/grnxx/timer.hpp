#ifndef GRNXX_TIMER_HPP
#define GRNXX_TIMER_HPP

namespace grnxx {

// FIXME: 将来的にはマイクロ秒単位を標準にする．
class Timer {
 public:
  // タイマを初期化する．
  Timer();

  // タイマが初期化されてからの経過時間を秒単位で返す．
  double elapsed() const;

  // タイマを初期化する．
  void reset();

 private:
  double start_time_;
};

}  // namespace grnxx

#endif  // GRNXX_TIMER_HPP
