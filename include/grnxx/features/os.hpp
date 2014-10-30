#ifndef GRNXX_FEATURES_OS_HPP
#define GRNXX_FEATURES_OS_HPP

#ifdef _WIN32
# define GRNXX_WIN
# ifdef _WIN64
#  define GRNXX_WIN64 _WIN64
# else  // _WIN64
#  define GRNXX_WIN32 _WIN32
# endif  // _WIN64
#endif  // _WIN32

#endif  // GRNXX_FEATURES_OS_HPP
