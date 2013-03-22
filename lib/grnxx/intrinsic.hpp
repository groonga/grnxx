/*
  Copyright (C) 2012  Brazil, Inc.

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public
  License as published by the Free Software Foundation; either
  version 2.1 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public
  License along with this library; if not, write to the Free Software
  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
*/
#ifndef GRNXX_INTRINSIC_HPP
#define GRNXX_INTRINSIC_HPP

#include "grnxx/basic.hpp"

#ifdef GRNXX_MSC
# include <intrin.h>
# pragma intrinsic(_BitScanReverse)
# pragma intrinsic(_BitScanForward)
# pragma intrinsic(_InterlockedExchangeAdd)
# pragma intrinsic(_InterlockedOr)
# pragma intrinsic(_InterlockedAnd)
# pragma intrinsic(_InterlockedXor)
# pragma intrinsic(_InterlockedCompareExchange)
# pragma intrinsic(_InterlockedCompareExchange64)
# ifdef GRNXX_MSC64
#  pragma intrinsic(_BitScanReverse64)
#  pragma intrinsic(_BitScanForward64)
#  pragma intrinsic(_InterlockedOr64)
#  pragma intrinsic(_InterlockedAnd64)
#  pragma intrinsic(_InterlockedXor64)
#  pragma intrinsic(_InterlockedExchangeAdd64)
# endif  // GRNXX_MSC64
#endif  // GRNXX_MSC

namespace grnxx {

// bit_scan_reverse() returns the position of the most significant 1 bit.
// For example, if value == 0x1010, bit_scan_reverse() returns 12.
// Note that if value == 0, the result is undefined.
template <typename Value>
inline uint8_t bit_scan_reverse(Value value);

// bit_scan_forward() returns the position of the most insignificant 1 bit.
// For example, if value == 0x1010, bit_scan_forward() returns 4.
// Note that if value == 0, the result is undefined.
template <typename Value>
inline uint8_t bit_scan_forward(Value value);

// atomic_compare_and_swap() atomically performs the following.
//  if (*value == expected) {
//    *value = desired;
//    return true;
//  } else {
//    return false;
//  }
template <typename Value>
inline bool atomic_compare_and_swap(Value expected, Value desired,
                                    volatile Value *value);

// atomic_fetch_and_add() atomically performs the following:
//  temp = *value;
//  *value += plus;
//  return temp;
template <typename Plus, typename Value>
inline Value atomic_fetch_and_add(Plus plus, volatile Value *value);

// atomic_fetch_or() atomically performs the following:
//  temp = *value;
//  *value |= plus;
//  return temp;
template <typename Value>
inline Value atomic_fetch_and_or(Value mask, volatile Value *value);

// atomic_fetch_and() atomically performs the following:
//  temp = *value;
//  *value &= plus;
//  return temp;
template <typename Value>
inline Value atomic_fetch_and_and(Value mask, volatile Value *value);

// atomic_fetch_xor() atomically performs the following:
//  temp = *value;
//  *value ^= plus;
//  return temp;
template <typename Value>
inline Value atomic_fetch_and_xor(Value mask, volatile Value *value);

// Implementation of bit_scan_reverse.

#if defined(GRNXX_MSC) || defined(GRNXX_HAS_GNUC_BUILTIN_CLZ)

template <size_t ValueSize>
class BitScanReverse {
 public:
  template <typename Value>
  uint8_t operator()(Value value) const {
# ifdef GRNXX_MSC
    unsigned long index;
    ::_BitScanReverse(&index, static_cast<unsigned long>(value));
    return static_cast<uint8_t>(index);
# else  // GRNXX_MSC
    return static_cast<uint8_t>(
        31 - ::__builtin_clz(static_cast<unsigned int>(value)));
# endif  // GRNXX_MSC
  }
};

template <>
class BitScanReverse<8> {
 public:
  template <typename Value>
  uint8_t operator()(Value value) const {
# ifdef GRNXX_MSC
#  ifdef GRNXX_MSC64
    unsigned long index;
    ::_BitScanReverse64(&index, static_cast<unsigned __int64>(value));
    return static_cast<uint8_t>(index);
#  else  // GRNXX_MSC64
    if ((value >> 32) != 0) {
      return static_cast<uint8_t>(BitScanReverse<4>((value >> 32) + 32));
    }
    return BitScanReverse<4>(value);
#  endif  // GRNXX_MSC64
# else  // GRNXX_MSC
    return static_cast<uint8_t>(
        63 - ::__builtin_clzll(static_cast<unsigned long long>(value)));
# endif  // GRNXX_MSC
  }
};

#endif  // defined(GRNXX_MSC) || defined(GRNXX_HAS_GNUC_BUILTIN_CLZ)

template <typename Value>
inline uint8_t bit_scan_reverse(Value value) {
  static_assert(std::is_integral<Value>::value &&
                std::is_unsigned<Value>::value,
                "bit_scan_reverse accepts only unsigned integer types");

#if defined(GRNXX_MSC) || defined(GRNXX_HAS_GNUC_BUILTIN_CLZ)
  return BitScanReverse<sizeof(Value)>()(value);
#else  // defined(GRNXX_MSC) || defined(GRNXX_HAS_GNUC_BUILTIN_CLZ)
  uint8_t result = 0;
  for (uint8_t shift = static_cast<uint8_t>(sizeof(Value) * 4);
       shift != 0; shift /= 2) {
    if ((value >> shift) != 0) {
      value >>= shift;
      result += shift;
    }
  }
  return result;
#endif  // defined(GRNXX_MSC) || defined(GRNXX_HAS_GNUC_BUILTIN_CLZ)
}

// Implementation of bit_scan_forward.

#if defined(GRNXX_MSC) || defined(GRNXX_HAS_GNUC_BUILTIN_CLZ)

template <size_t ValueSize>
class BitScanForward {
 public:
  template <typename Value>
  uint8_t operator()(Value value) const {
# ifdef GRNXX_MSC
    unsigned long index;
    ::_BitScanForward(&index, static_cast<unsigned long>(value));
    return static_cast<uint8_t>(index);
# else  // GRNXX_MSC
    return static_cast<uint8_t>(
        ::__builtin_ctz(static_cast<unsigned int>(value)));
# endif  // GRNXX_MSC
  }
};

template <>
class BitScanForward<8> {
 public:
  template <typename Value>
  uint8_t operator()(Value value) const {
# ifdef GRNXX_MSC
#  ifdef GRNXX_MSC64
    unsigned long index;
    ::_BitScanForward64(&index, static_cast<unsigned __int64>(value));
    return static_cast<uint8_t>(index);
#  else  // GRNXX_MSC64
    if ((value & 0xFFFFFFFFU) == 0) {
      return static_cast<uint8_t>(BitScanForward<4>((value >> 32) + 32));
    }
    return BitScanForward<4>(value);
#  endif  // GRNXX_MSC64
# else  // GRNXX_MSC
    return static_cast<uint8_t>(
        ::__builtin_ctzll(static_cast<unsigned long long>(value)));
# endif  // GRNXX_MSC
  }
};

#endif  // defined(GRNXX_MSC) || defined(GRNXX_HAS_GNUC_BUILTIN_CLZ)

template <typename Value>
inline uint8_t bit_scan_forward(Value value) {
  static_assert(std::is_integral<Value>::value &&
                std::is_unsigned<Value>::value,
                "bit_scan_forward accepts only unsigned integer types");

#if defined(GRNXX_MSC) || defined(GRNXX_HAS_GNUC_BUILTIN_CLZ)
  return BitScanForward<sizeof(Value)>()(value);
#else  // defined(GRNXX_MSC) || defined(GRNXX_HAS_GNUC_BUILTIN_CLZ)
  uint8_t result = static_cast<uint8_t>(sizeof(Value) * 8) - 1;
  for (uint8_t shift = static_cast<uint8_t>(sizeof(Value) * 4);
       shift != 0; shift /= 2) {
    if (static_cast<Value>(value << shift) != 0) {
      value <<= shift;
      result -= shift;
    }
  }
  return result;
#endif  // defined(GRNXX_MSC) || defined(GRNXX_HAS_GNUC_BUILTIN_CLZ)
}

// Helper classes.

template <size_t ValueSize> class IntrinsicType;

#ifdef GRNXX_MSC
template <> class IntrinsicType<1> { public: typedef char Value; };
template <> class IntrinsicType<2> { public: typedef short Value; };
template <> class IntrinsicType<4> { public: typedef long Value; };
template <> class IntrinsicType<8> { public: typedef __int64 Value; };
#else  // GRNXX_MSC
template <> class IntrinsicType<1> { public: typedef uint8_t Value; };
template <> class IntrinsicType<2> { public: typedef uint16_t Value; };
template <> class IntrinsicType<4> { public: typedef uint32_t Value; };
template <> class IntrinsicType<8> { public: typedef uint64_t Value; };
#endif  // GRNXX_MSC

template <typename Value>
class Intrinsic {
 public:
  typedef typename IntrinsicType<sizeof(Value)>::Value InternalValue;
  typedef volatile InternalValue *InternalPointer;
};

// Implementation of atomic_compare_and_swap.

#ifdef GRNXX_MSC

template <typename Value>
class AtomicCompareAndSwap;

template <>
class AtomicCompareAndSwap<long> {
 public:
  bool operator()(long expected, long desired,
                  volatile long *value) const {
    return ::_InterlockedCompareExchange(value, desired, expected) == expected;
  }
};

template <>
class AtomicCompareAndSwap<__int64> {
 public:
  bool operator()(__int64 expected, __int64 desired,
                  volatile __int64 *value) const {
    return ::_InterlockedCompareExchange64(value, desired, expected) == expected;
  }
};

template <typename Value>
class AtomicCompareAndSwap {
 public:
  typedef typename Intrinsic<Value>::InternalValue InternalValue;
  typedef typename Intrinsic<Value>::InternalPointer InternalPointer;

  bool operator()(Value expected, Value desired, volatile Value *value) const {
    return AtomicCompareAndSwap<InternalValue>()(
        reinterpret_cast<const InternalValue &>(expected),
        reinterpret_cast<const InternalValue &>(desired),
        reinterpret_cast<InternalPointer>(value));
  }
};

#elif defined(GRNXX_HAS_GNUC_BUILTIN_ATOMIC) ||\
      defined(GRNXX_HAS_GNUC_BUILTIN_SYNC)

// GNUC builtin functions for atomic operations accept integer types and
// pointer types. Other types require type casting.
template <typename Value, bool RequiresCast>
class InternalAtomicCompareAndSwap;

template <typename Value>
class InternalAtomicCompareAndSwap<Value, false> {
 public:
  bool operator()(Value expected, Value desired,
                  volatile Value *value) const {
#ifdef GRNXX_HAS_GNUC_BUILTIN_ATOMIC
    return ::__atomic_compare_exchange_n(
        value, &expected, desired, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
#else  // GRNXX_HAS_GNUC_BUILTIN_ATOMIC
    return ::__sync_bool_compare_and_swap(value, expected, desired);
#endif  // GRNXX_HAS_GNUC_BUILTIN_ATOMIC
  }
};

template <typename Value>
class InternalAtomicCompareAndSwap<Value, true> {
 public:
  typedef typename Intrinsic<Value>::InternalValue InternalValue;
  typedef typename Intrinsic<Value>::InternalPointer InternalPointer;

  bool operator()(Value expected, Value desired, volatile Value *value) const {
    return InternalAtomicCompareAndSwap<InternalValue, false>()(
        reinterpret_cast<const InternalValue &>(expected),
        reinterpret_cast<const InternalValue &>(desired),
        reinterpret_cast<InternalPointer>(value));
  }
};

template <typename Value>
class AtomicCompareAndSwap {
 public:
  typedef typename Intrinsic<Value>::InternalValue InternalValue;
  typedef typename Intrinsic<Value>::InternalPointer InternalPointer;

  bool operator()(Value expected, Value desired, volatile Value *value) const {
    return InternalAtomicCompareAndSwap<Value,
        !std::is_integral<Value>::value && !std::is_pointer<Value>::value>()(
            expected, desired, value);
  }
};

#else  // defined(GRNXX_HAS_GNUC_BUILTIN_ATOMIC) || ...

template <typename Value>
class AtomicCompareAndSwap {
 public:
  bool operator()(Value expected, Value desired, volatile Value *value) const {
    // Non-atomic implementation.
    if (*value == expected) {
      *value = desired;
      return true;
    } else {
      return false;
    }
  }
};

#endif  // defined(GRNXX_HAS_GNUC_BUILTIN_ATOMIC) || ...

template <typename Value>
inline bool atomic_compare_and_swap(Value expected, Value desired,
                                    volatile Value *value) {
  static_assert((sizeof(Value) == 4) || (sizeof(Value) == 8),
      "atomic_compare_and_swap is supported only for 4/8 bytes values.");

  return AtomicCompareAndSwap<Value>()(expected, desired, value);
}

// Implementation of atomic_fetch_and_add.

#ifdef GRNXX_MSC

template <typename Value>
class InternalAtomicFetchAndAdd;

template <>
class InternalAtomicFetchAndAdd<long> {
 public:
  long operator()(long plus, volatile long *value) const {
    return ::_InterlockedExchangeAdd(plus, value);
  }
};

template <>
class InternalAtomicFetchAndAdd<__int64> {
 public:
  __int64 operator()(__int64 plus, volatile __int64 *value) const {
#ifdef GRNXX_MSC64
    return ::_InterlockedExchangeAdd64(plus, value);
#else  // GRNXX_MSC64
    for ( ; ; ) {
      const __int64 expected = *value;
      const __int64 desired = expected + plus;
      if (atomic_compare_and_swap(expected, desired, value)) {
        return expected;
      }
    }
#endif  // GRNXX_MSC64
  }
};

template <typename Value>
class InternalAtomicFetchAndAdd {
 public:
  typedef typename Intrinsic<Value>::InternalValue InternalValue;
  typedef typename Intrinsic<Value>::InternalPointer InternalPointer;

  Value operator()(Value plus, volatile Value *value) const {
    return static_cast<Value>(InternalAtomicFetchAndAdd<InternalValue>()(
        static_cast<InternalValue>(plus),
        reinterpret_cast<InternalPointer>(value)));
  }
};

#elif defined(GRNXX_HAS_GNUC_BUILTIN_ATOMIC) ||\
      defined(GRNXX_HAS_GNUC_BUILTIN_SYNC)

template <typename Value>
class InternalAtomicFetchAndAdd {
 public:
  Value operator()(Value plus, volatile Value *value) const {
#ifdef GRNXX_HAS_GNUC_BUILTIN_ATOMIC
    return ::__atomic_fetch_add(value, plus, __ATOMIC_SEQ_CST);
#else  // GRNXX_HAS_GNUC_BUILTIN_ATOMIC
    return ::__sync_fetch_and_add(value, plus);
#endif  // GRNXX_HAS_GNUC_BUILTIN_ATOMIC
  }
};

#else  // defined(GRNXX_HAS_GNUC_BUILTIN_ATOMIC) || ...

template <typename Value>
class InternalAtomicFetchAndAdd {
 public:
  Value operator()(Value plus, volatile Value *value) const {
    for ( ; ; ) {
      const Value expected = *value;
      const Value desired = expected + plus;
      if (atomic_compare_and_swap(expected, desired, value)) {
        return expected;
      }
    }
  }
};

#endif

template <typename Plus, typename Value, bool PlusAndValueAreIntegers>
class AtomicFetchAndAdd;

template <typename Plus, typename Value>
class AtomicFetchAndAdd<Plus, Value, true> {
 public:
  Value operator()(Plus plus, volatile Value *value) const {
    return InternalAtomicFetchAndAdd<Value>()(static_cast<Value>(plus), value);
  }
};

template <typename Plus, typename Value>
class AtomicFetchAndAdd<Plus, Value, false> {
 public:
  Value operator()(Plus plus, volatile Value *value) const {
    for ( ; ; ) {
      const Value expected = *value;
      const Value desired = expected + plus;
      if (atomic_compare_and_swap(expected, desired, value)) {
        return expected;
      }
    }
  }
};

template <typename Plus, typename Value>
inline Value atomic_fetch_and_add(Plus plus, volatile Value *value) {
  static_assert((sizeof(Value) == 4) || (sizeof(Value) == 8),
      "atomic_fetch_and_add is supported only for 4/8 bytes values.");

  return AtomicFetchAndAdd<Plus, Value, std::is_integral<Plus>::value &&
                           std::is_integral<Value>::value>()(plus, value);
}

// Implementation of atomic_fetch_and_or.

#ifdef GRNXX_MSC

template <typename Value>
class AtomicFetchAndOr;

template <>
class AtomicFetchAndOr<char> {
 public:
  char operator()(char mask, volatile char *value) const {
    return ::_InterlockedOr8(mask, value);
  }
};

template <>
class AtomicFetchAndOr<short> {
 public:
  short operator()(short mask, volatile short *value) const {
    return ::_InterlockedOr16(mask, value);
  }
};

template <>
class AtomicFetchAndOr<long> {
 public:
  long operator()(long mask, volatile long *value) const {
    return ::_InterlockedOr(mask, value);
  }
};

template <>
class AtomicFetchAndOr<__int64> {
 public:
  __int64 operator()(__int64 mask, volatile __int64 *value) const {
#ifdef GRNXX_MSC64
    return ::_InterlockedOr64(mask, value);
#else  // GRNXX_MSC64
    for ( ; ; ) {
      const __int64 expected = *value;
      const __int64 desired = expected | mask;
      if (atomic_compare_and_swap(expected, desired, value)) {
        return expected;
      }
    }
#endif  // GRNXX_MSC64
  }
};

template <typename Value>
class AtomicFetchAndOr {
 public:
  typedef typename Intrinsic<Value>::InternalValue InternalValue;
  typedef typename Intrinsic<Value>::InternalPointer InternalPointer;

  Value operator()(Value plus, volatile Value *value) const {
    return static_cast<Value>(AtomicFetchAndOr<InternalValue>()(
        static_cast<InternalValue>(mask),
        reinterpret_cast<InternalPointer>(value)));
  }
};

#elif defined(GRNXX_HAS_GNUC_BUILTIN_ATOMIC) ||\
      defined(GRNXX_HAS_GNUC_BUILTIN_SYNC)

template <typename Value>
class AtomicFetchAndOr {
 public:
  Value operator()(Value mask, volatile Value *value) const {
#ifdef GRNXX_HAS_GNUC_BUILTIN_ATOMIC
    return ::__atomic_fetch_or(value, mask, __ATOMIC_SEQ_CST);
#else  // GRNXX_HAS_GNUC_BUILTIN_ATOMIC
    return ::__sync_fetch_and_or(value, mask);
#endif  // GRNXX_HAS_GNUC_BUILTIN_ATOMIC
  }
};

#else  // defined(GRNXX_HAS_GNUC_BUILTIN_ATOMIC) || ...

template <typename Value>
class AtomicFetchAndOr {
 public:
  Value operator()(Value mask, volatile Value *value) const {
    for ( ; ; ) {
      const Value expected = *value;
      const Value desired = expected | mask;
      if (atomic_compare_and_swap(expected, desired, value)) {
        return expected;
      }
    }
  }
};

#endif

template <typename Value>
inline Value atomic_fetch_and_or(Value mask, volatile Value *value) {
  static_assert(std::is_integral<Value>::value,
                "atomic_fetch_and_or accepts only integer types.");

  return AtomicFetchAndOr<Value>()(mask, value);
}

// Implementation of atomic_fetch_and_and.

#ifdef GRNXX_MSC

template <typename Value>
class AtomicFetchAndAnd;

template <>
class AtomicFetchAndAnd<char> {
 public:
  char operator()(char mask, volatile char *value) const {
    return ::_InterlockedAnd8(mask, value);
  }
};

template <>
class AtomicFetchAndAnd<short> {
 public:
  short operator()(short mask, volatile short *value) const {
    return ::_InterlockedAnd16(mask, value);
  }
};

template <>
class AtomicFetchAndAnd<long> {
 public:
  long operator()(long mask, volatile long *value) const {
    return ::_InterlockedAnd(mask, value);
  }
};

template <>
class AtomicFetchAndAnd<__int64> {
 public:
  __int64 operator()(__int64 mask, volatile __int64 *value) const {
#ifdef GRNXX_MSC64
    return ::_InterlockedAnd64(mask, value);
#else  // GRNXX_MSC64
    for ( ; ; ) {
      const __int64 expected = *value;
      const __int64 desired = expected & mask;
      if (atomic_compare_and_swap(expected, desired, value)) {
        return expected;
      }
    }
#endif  // GRNXX_MSC64
  }
};

template <typename Value>
class AtomicFetchAndAnd {
 public:
  typedef typename Intrinsic<Value>::InternalValue InternalValue;
  typedef typename Intrinsic<Value>::InternalPointer InternalPointer;

  Value operator()(Value plus, volatile Value *value) const {
    return static_cast<Value>(AtomicFetchAndAnd<InternalValue>()(
        static_cast<InternalValue>(mask),
        reinterpret_cast<InternalPointer>(value)));
  }
};

#elif defined(GRNXX_HAS_GNUC_BUILTIN_ATOMIC) ||\
      defined(GRNXX_HAS_GNUC_BUILTIN_SYNC)

template <typename Value>
class AtomicFetchAndAnd {
 public:
  Value operator()(Value mask, volatile Value *value) const {
#ifdef GRNXX_HAS_GNUC_BUILTIN_ATOMIC
    return ::__atomic_fetch_and(value, mask, __ATOMIC_SEQ_CST);
#else  // GRNXX_HAS_GNUC_BUILTIN_ATOMIC
    return ::__sync_fetch_and_and(value, mask);
#endif  // GRNXX_HAS_GNUC_BUILTIN_ATOMIC
  }
};

#else  // defined(GRNXX_HAS_GNUC_BUILTIN_ATOMIC) || ...

template <typename Value>
class AtomicFetchAndAnd {
 public:
  Value operator()(Value mask, volatile Value *value) const {
    for ( ; ; ) {
      const Value expected = *value;
      const Value desired = expected & mask;
      if (atomic_compare_and_swap(expected, desired, value)) {
        return expected;
      }
    }
  }
};

#endif

template <typename Value>
inline Value atomic_fetch_and_and(Value mask, volatile Value *value) {
  static_assert(std::is_integral<Value>::value,
                "atomic_fetch_and_and accepts only integer types.");

  return AtomicFetchAndAnd<Value>()(mask, value);
}

// Implementation of atomic_fetch_and_xor.

#ifdef GRNXX_MSC

template <typename Value>
class AtomicFetchAndXor;

template <>
class AtomicFetchAndXor<char> {
 public:
  char operator()(char mask, volatile char *value) const {
    return ::_InterlockedXor8(mask, value);
  }
};

template <>
class AtomicFetchAndXor<short> {
 public:
  short operator()(short mask, volatile short *value) const {
    return ::_InterlockedXor16(mask, value);
  }
};

template <>
class AtomicFetchAndXor<long> {
 public:
  long operator()(long mask, volatile long *value) const {
    return ::_InterlockedXor(mask, value);
  }
};

template <>
class AtomicFetchAndXor<__int64> {
 public:
  __int64 operator()(__int64 mask, volatile __int64 *value) const {
#ifdef GRNXX_MSC64
    return ::_InterlockedXor64(mask, value);
#else  // GRNXX_MSC64
    for ( ; ; ) {
      const __int64 expected = *value;
      const __int64 desired = expected ^ mask;
      if (atomic_compare_and_swap(expected, desired, value)) {
        return expected;
      }
    }
#endif  // GRNXX_MSC64
  }
};

template <typename Value>
class AtomicFetchAndXor {
 public:
  typedef typename Intrinsic<Value>::InternalValue InternalValue;
  typedef typename Intrinsic<Value>::InternalPointer InternalPointer;

  Value operator()(Value plus, volatile Value *value) const {
    return static_cast<Value>(AtomicFetchAndXor<InternalValue>()(
        static_cast<InternalValue>(mask),
        reinterpret_cast<InternalPointer>(value)));
  }
};

#elif defined(GRNXX_HAS_GNUC_BUILTIN_ATOMIC) ||\
      defined(GRNXX_HAS_GNUC_BUILTIN_SYNC)

template <typename Value>
class AtomicFetchAndXor {
 public:
  Value operator()(Value mask, volatile Value *value) const {
#ifdef GRNXX_HAS_GNUC_BUILTIN_ATOMIC
    return ::__atomic_fetch_xor(value, mask, __ATOMIC_SEQ_CST);
#else  // GRNXX_HAS_GNUC_BUILTIN_ATOMIC
    return ::__sync_fetch_and_xor(value, mask);
#endif  // GRNXX_HAS_GNUC_BUILTIN_ATOMIC
  }
};

#else  // defined(GRNXX_HAS_GNUC_BUILTIN_ATOMIC) || ...

template <typename Value>
class AtomicFetchAndXor {
 public:
  Value operator()(Value mask, volatile Value *value) const {
    for ( ; ; ) {
      const Value expected = *value;
      const Value desired = expected ^ mask;
      if (atomic_compare_and_swap(expected, desired, value)) {
        return expected;
      }
    }
  }
};

#endif

template <typename Value>
inline Value atomic_fetch_and_xor(Value mask, volatile Value *value) {
  static_assert(std::is_integral<Value>::value,
                "atomic_fetch_and_xor accepts only integer types.");

  return AtomicFetchAndXor<Value>()(mask, value);
}

}  // namespace grnxx

#endif  // INTRINSIC_HPP
