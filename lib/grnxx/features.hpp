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
#ifndef GRNXX_FEATURES_HPP
#define GRNXX_FEATURES_HPP

// Operating system features.

#ifdef _WIN32
# define GRNXX_WINDOWS
# ifdef _WIN64
#  define GRNXX_WIN64 _WIN64
# else  // _WIN64
#  define GRNXX_WIN32 _WIN32
# endif  // _WIN64
#endif  // _WIN32

#if defined(__APPLE__) && defined(__MACH__)
# define GRNXX_APPLE
#endif  // defined(__APPLE__) && defined(__MACH__)

#if defined(sun) || defined(__sun)
# define GRNXX_SOLARIS
#endif  // defined(sun) || defined(__sun)

// Compiler features.

#ifdef _MSC_VER
# define GRNXX_MSC
# define GRNXX_MSC_VERSION _MSC_VER
# ifdef GRNXX_WIN32
#  define GRNXX_MSC32
# else  // GRNXX_WIN32
#  define GRNXX_MSC64
# endif  // GRNXX_WIN32
#endif  // _MSC_VER

#ifdef __MINGW32__
# define GRNXX_MINGW
# ifdef __MINGW64__
#  define GRNXX_MINGW64
# else  // __MINGW64__
#  define GRNXX_MINGW32
# endif  // __MINGW64__
#endif  // __MINGW32__

#ifdef __clang__
# define GRNXX_CLANG
# define GRNXX_CLANG_MAJOR __clang_major__
# define GRNXX_CLANG_MINOR __clang_minor__
# define GRNXX_CLANG_PATCH_LEVEL __clang_patchlevel__
# define GRNXX_CLANG_VERSION __clang_version__
#endif  // __clang__

#ifdef __GNUC__
# define GRNXX_GNUC
# define GRNXX_GNUC_MAJOR __GNUC__
# ifdef __GNUC_MINOR__
#  define GRNXX_GNUC_MINOR __GNUC_MINOR__
# else  // __GNUC_MINOR__
#  define GRNXX_GNUC_MINOR 0
# endif  // __GNUC_MINOR__
# ifdef __GNUC_PATCHLEVEL__
#  define GRNXX_GNUC_PATCH_LEVEL __GNUC_PATCHLEVEL__
# else  // __GNUC_PATCHLEVEL__
#  define GRNXX_GNUC_PATCH_LEVEL 0
# endif  // __GNUC_PATCHLEVEL__
# define GRNXX_GNUC_MAKE_VERSION(major, minor, patch_level)\
    ((major * 10000) + (minor * 100) + patch_level)
# define GRNXX_GNUC_VERSION GRNXX_GNUC_MAKE_VERSION(\
    GRNXX_GNUC_MAJOR, GRNXX_GNUC_MINOR, GRNXX_GNUC_PATCH_LEVEL)
#endif  // defined(__GNUC__)

#ifdef GRNXX_CLANG
# ifdef __has_builtin
#  define GRNXX_CLANG_HAS_BUILTIN(builtin) __has_builtin(builtin)
# else
#  define GRNXX_CLANG_HAS_BUILTIN(builtin) false
# endif  // __has_builtin
# ifdef __has_feature
#  define GRNXX_CLANG_HAS_FEATURE(feature) __has_feature(feature)
# else
#  define GRNXX_CLANG_HAS_FEATURE(feature) false
# endif  // __has_feature
# ifdef __has_extension
#  define GRNXX_CLANG_HAS_EXTENSION(extension) __has_extension(extension)
# else
#  define GRNXX_CLANG_HAS_EXTENSION(extension) false
# endif  // __has_extension
# define GRNXX_CLANG_HAS(thing) (GRNXX_CLANG_HAS_BUILTIN(thing) |\
    GRNXX_CLANG_HAS_FEATURE(thing) | GRNXX_CLANG_HAS_EXTENSION(thing))
#endif  // GRNXX_CLANG

#ifdef GRNXX_CLANG
# if GRNXX_CLANG_HAS(__builtin_bswap16)
#  define GRNXX_HAS_GNUC_BUILTIN_BSWAP16
# endif  // GRNXX_CLANG_HAS(__builtin_bswap16)
# if GRNXX_CLANG_HAS(__builtin_bswap32)
#  define GRNXX_HAS_GNUC_BUILTIN_BSWAP32
# endif  // GRNXX_CLANG_HAS(__builtin_bswap32)
# if GRNXX_CLANG_HAS(__builtin_bswap64)
#  define GRNXX_HAS_GNUC_BUILTIN_BSWAP64
# endif  // GRNXX_CLANG_HAS(__builtin_bswap64)
# if GRNXX_CLANG_HAS(__builtin_clz)
#  define GRNXX_HAS_GNUC_BUILTIN_CLZ
# endif  // GRNXX_CLANG_HAS(__builtin_clz)
# if GRNXX_CLANG_HAS(__sync_bool_compare_and_swap)
#  define GRNXX_HAS_GNUC_BUILTIN_SYNC
# endif  // GRNXX_CLANG_HAS(__sync_bool_compare_and_swap)
# if GRNXX_CLANG_HAS(__atomic_compare_exchange_n)
#  define GRNXX_HAS_GNUC_BUILTIN_ATOMIC
# endif  // GRNXX_CLANG_HAS(__atomic_compare_exchange_n)
# if GRNXX_CLANG_HAS(c_atomic)
#  define GRNXX_HAS_CLANG_BUILTIN_ATOMIC
# endif  // GRNXX_CLANG_HAS(c_atomic)
#elif defined(GRNXX_GNUC)
# define GRNXX_HAS_GNUC_BUILTIN_CLZ
# if GRNXX_GNUC_VERSION >= GRNXX_GNUC_MAKE_VERSION(4, 2, 0)
#  define GRNXX_HAS_GNUC_BUILTIN_SYNC
#  define GRNXX_HAS_GNUC_BUILTIN_BSWAP32
#  define GRNXX_HAS_GNUC_BUILTIN_BSWAP64
# endif  // GRNXX_GNUC_VERSION >= GRNXX_GNUC_MAKE_VERSION(4, 2, 0)
# if GRNXX_GNUC_VERSION >= GRNXX_GNUC_MAKE_VERSION(4, 7, 0)
#  define GRNXX_HAS_GNUC_BUILTIN_ATOMIC
# endif  // GRNXX_GNUC_VERSION >= GRNXX_GNUC_MAKE_VERSION(4, 7, 0)
# if GRNXX_GNUC_VERSION >= GRNXX_GNUC_MAKE_VERSION(4, 8, 0)
#  define GRNXX_HAS_GNUC_BUILTIN_BSWAP16
# endif  // GRNXX_GNUC_VERSION >= GRNXX_GNUC_MAKE_VERSION(4, 8, 0)
#endif  // defined(GRNXX_GNUC)

// Source features.

#ifdef _POSIX_C_SOURCE
# define GRNXX_POSIX_C_SOURCE _POSIX_C_SOURCE
#else  // _POSIX_C_SOURCE
# define GRNXX_POSIX_C_SOURCE 0
#endif  // _POSIX_C_SOURCE

#ifdef _XOPEN_SOURCE
# define GRNXX_XOPEN_SOURCE _XOPEN_SOURCE
#else  // _XOPEN_SOURCE
# define GRNXX_XOPEN_SOURCE 0
#endif  // _XOPEN_SOURCE

#ifdef _XOPEN_SOURCE_EXTENDED
# define GRNXX_XOPEN_SOURCE_EXTENDED _XOPEN_SOURCE_EXTENDED
#else  // _XOPEN_SOURCE_EXTENDED
# define GRNXX_XOPEN_SOURCE_EXTENDED 0
#endif  // _XOPEN_SOURCE_EXTENDED

#ifdef _BSD_SOURCE
# define GRNXX_BSD_SOURCE _BSD_SOURCE
#else  // _BSD_SOURCE
# define GRNXX_BSD_SOURCE 0
#endif  // _BSD_SOURCE

#ifdef _SVID_SOURCE
# define GRNXX_SVID_SOURCE _SVID_SOURCE
#else  // _SVID_SOURCE
# define GRNXX_SVID_SOURCE 0
#endif  // _SVID_SOURCE

#ifdef _POSIX_SOURCE
# define GRNXX_POSIX_SOURCE _POSIX_SOURCE
#else  // _POSIX_SOURCE
# define GRNXX_POSIX_SOURCE 0
#endif  // _POSIX_SOURCE

// Available functions.

#if GRNXX_POSIX_C_SOURCE >= 199309L
# define GRNXX_HAS_NANOSLEEP
#endif  // GRNXX_POSIX_C_SOURCE >= 199309L

#if (GRNXX_POSIX_C_SOURCE >= 1) || GRNXX_XOPEN_SOURCE || GRNXX_BSD_SOURCE ||\
    GRNXX_SVID_SOURCE || GRNXX_POSIX_SOURCE
# define GRNXX_HAS_GMTIME_R
# define GRNXX_HAS_LOCALTIME_R
#endif  // (GRNXX_POSIX_C_SOURCE >= 1) || ...

#if (GRNXX_XOPEN_SOURCE >= 500) || (GRNXX_POSIX_C_SOURCE >= 200809L)
# define GRNXX_HAS_PREAD
# define GRNXX_HAS_PWRITE
#endif  // (GRNXX_XOPEN_SOURCE >= 500) || (GRNXX_POSIX_C_SOURCE >= 200809L)

#ifndef GRNXX_WINDOWS
# include <unistd.h>
# ifdef _POSIX_TIMERS
#  if _POSIX_TIMERS > 0
#   define GRNXX_HAS_POSIX_TIMER
#   define GRNXX_HAS_CLOCK_GETTIME
#  endif  // _POSIX_TIMERS > 0
# endif  // _POSIX_TIMERS
# ifdef _POSIX_PRIORITY_SCHEDULING
#  define GRNXX_HAS_SCHED_YIELD
# endif  // _POSIX_PRIORITY_SCHEDULING
#endif  // GRNXX_WINDOWS

// DLL features.

#ifdef GRNXX_WINDOWS
# ifdef DLL_EXPORT
#  define GRNXX_EXPORT __declspec(dllexport)
# else  // DLL_EXPORT
#  define GRNXX_EXPORT __declspec(dllimport)
# endif  // DLL_EXPORT
#else  // GRNXX_WINDOWS
# define GRNXX_EXPORT
#endif  // GRNXX_WINDOWS

#endif  // GRNXX_FEATURES_HPP
