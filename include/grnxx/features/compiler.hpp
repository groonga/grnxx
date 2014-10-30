#ifndef GRNXX_FEATURES_COMPILER_HPP
#define GRNXX_FEATURES_COMPILER_HPP

#ifdef _MSC_VER
# define GRNXX_MSC
# define GRNXX_MSC_VERSION _MSC_VER
#endif  // _MSC_VER

// NOTE: 64-bit MinGW defines both __MINGW32__ and __MINGW64__,
//       but the following uses the logical OR of them for clarification.
#if defined(__MINGW32__) || defined(__MINGW64__)
# define GRNXX_MINGW
# ifdef __MINGW64__
#  define GRNXX_MINGW_MAJOR __MINGW64_MAJOR_VERSION
#  define GRNXX_MINGW_MINOR __MINGW64_MINOR_VERSION
# else  // __MINGW64__
#  define GRNXX_MINGW_MAJOR __MINGW32_MAJOR_VERSION
#  define GRNXX_MINGW_MINOR __MINGW32_MINOR_VERSION
# endif  // __MINGW64__
# define GRNXX_MINGW_MAKE_VERSION(major, minor) ((major * 100) + minor)
# define GRNXX_MINGW_VERSION GRNXX_MINGW_MAKE_VERSION(\
    GRNXX_MINGW_MAJOR, GRNXX_MINGW_MINOR)
#endif  // defined(__MINGW32__) || defined(__MINGW64__)

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

#endif  // GRNXX_FEATURES_COMPILER_HPP
