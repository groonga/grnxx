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
#include "grnxx/backtrace.hpp"

#include "../config.h"

#ifdef HAVE_BACKTRACE

#include <cxxabi.h>
#include <dlfcn.h>
#include <execinfo.h>

#ifdef HAVE_LIBBFD
# include <bfd.h>
# ifdef GRNXX_APPLE
#  include <mach-o/dyld.h>
# else  // GRNXX_APPLE
#  include <link.h>
# endif  // GRNXX_APPLE
#endif  // HAVE_LIBBFD

#include <cstdlib>
#include <cstring>
#include <sstream>

#include "grnxx/lock.hpp"
#include "grnxx/mutex.hpp"

namespace grnxx {
namespace {

class Resolver {
 public:
  static bool resolve(void *address, std::ostream *stream);

 private:
#ifdef HAVE_LIBBFD
  bfd *bfd_;
  asymbol **bfd_symbols_;

  static bool bfd_initialized_;
#endif  // HAVE_LIBBFD

  Resolver();
  ~Resolver();

#ifdef HAVE_LIBBFD
  bool resolve_(const char *image_name, bfd_vma address, std::ostream *stream);
#endif  // HAVE_LIBBFD

  Resolver(const Resolver &);
  Resolver &operator=(const Resolver &);
};

#ifdef HAVE_LIBBFD
struct Match {
  const char *image_name;
  uintptr_t address;
};

// FIXME: The default image name depends on the environment.
// http://stackoverflow.com/questions/1023306/finding-current-executables-path-without-proc-self-exe
const char DEFAULT_IMAGE_NAME[] = "/proc/self/exe";

# ifdef GRNXX_APPLE
// TODO: Not tested yet.
bool find_match(Match *match) {
  const uint32_t image_count = ::_dyld_image_count();
  for (uint32_t image_id = 0; image_id < image_count; ++image_id) {
    const struct mach_header *header = ::_dyld_get_image_header(image_id);
    const struct load_command *command;
    if (header->magic == MH_MAGIC_64) {
      command = reinterpret_cast<const struct load_command *>(
          reinterpret_cast<const struct mach_header_64 *>(header) + 1);
    } else {
      command = reinterpret_cast<const struct load_command *>(header + 1);
    }

    const intptr_t slide = ::_dyld_get_image_vmaddr_slide(image_id);
    for (uint32_t command_id = 0; command_id < header->ncmds; ++command_id) {
      switch (command->cmd) {
        case LC_SEGMENT: {
          const struct segment_command *segment =
              reinterpret_cast<const struct segment_command *>(command);
          if ((address >= (segment->vmaddr + slide)) &&
              (address < (segment->vmaddr + slide + segment->vmsize))) {
            match.address = segment->vmaddr - slide;
            match.image_name = ::_dyld_get_image_name(image_id);
            return true;
          }
          break;
        }
        case LC_SEGMENT_64: {
          const struct segment_command_64 *segment =
              reinterpret_cast<const struct segment_command_64 *>(command);
          if ((address >= (segment->vmaddr + slide)) &&
              (address < (segment->vmaddr + slide + segment->vmsize))) {
            match.address = segment->vmaddr - slide;
            match.image_name = ::_dyld_get_image_name(i);
            return true;
          }
          break;
        }
      }
      command = reinterpret_cast<const struct load_command *>(
          reinterpret_cast<const char *>(command) + cmd->cmdsize);
    }
  }
  return false;
}
# else  // GRNXX_APPLE
int find_match_callback(struct dl_phdr_info *info, size_t, void *user_data) {
  Match * const match = static_cast<Match *>(user_data);
  for (ElfW(Half) i = 0; i < info->dlpi_phnum; ++i) {
    if (info->dlpi_phdr[i].p_type == PT_LOAD) {
      ElfW(Addr) address = info->dlpi_phdr[i].p_vaddr + info->dlpi_addr;
      if ((match->address >= address) &&
          (match->address < (address + info->dlpi_phdr[i].p_memsz))) {
        if (!info->dlpi_name || (info->dlpi_name[0] == '\0')) {
          match->image_name = DEFAULT_IMAGE_NAME;
        } else {
          match->image_name = info->dlpi_name;
        }
        match->address -= (uintptr_t)info->dlpi_addr;
        return 1;
      }
    }
  }
  return 0;
}

bool find_match(Match *match) {
  return dl_iterate_phdr(find_match_callback, match) != 0;
}
# endif  // GRNXX_APPLE

bool Resolver::bfd_initialized_ = false;

bool Resolver::resolve(void *address, std::ostream *stream) {
  // Just to be safe, call ::bfd_init() only once.
  if (!bfd_initialized_) {
    ::bfd_init();
    bfd_initialized_ = true;
  }

  Match match;
  match.address = reinterpret_cast<uintptr_t>(address) - 1;
  if (find_match(&match)) {
    *stream << address;
    return Resolver().resolve_(match.image_name,
                               static_cast<bfd_vma>(match.address), stream);
  }
  return false;
}

class Detail {
 public:
  Detail(asymbol **symbols_, bfd_vma address_)
      : symbols(symbols_),
        address(address_),
        filename(nullptr),
        function(nullptr),
        line(0),
        found(false) {}

  asymbol **symbols;
  bfd_vma address;
  const char *filename;
  const char *function;
  unsigned int line;
  bool found;
};

void callback_for_each_section(bfd *bfd_, asection *section, void *user_data) {
  Detail * const detail = static_cast<Detail *>(user_data);
  if (detail->found) {
    return;
  }

  if ((bfd_get_section_flags(bfd_, section) & SEC_ALLOC) == 0) {
    return;
  }

  bfd_vma address = bfd_get_section_vma(bfd_, section);
  if (detail->address < address) {
    return;
  }

  bfd_size_type size = bfd_section_size(bfd_, section);
  if (detail->address >= (address + size)) {
    return;
  }

  if (bfd_find_nearest_line(bfd_, section, detail->symbols,
                            detail->address - address, &detail->filename,
                            &detail->function, &detail->line) != 0) {
    detail->found = true;
  }
}

Resolver::Resolver() : bfd_(nullptr), bfd_symbols_(nullptr) {}

Resolver::~Resolver() {
  if (bfd_symbols_) {
    std::free(bfd_symbols_);
  }
  if (bfd_) {
    ::bfd_close(bfd_);
  }
}

bool Resolver::resolve_(const char *image_name, bfd_vma address,
                        std::ostream *stream) {
  bfd_ = ::bfd_openr(image_name, nullptr);
  if (!bfd_) {
    return false;
  }

  if (::bfd_check_format(bfd_, ::bfd_archive)) {
    return false;
  }

  char **matches = nullptr;
  if (!::bfd_check_format_matches(bfd_, ::bfd_object, &matches)) {
    if (::bfd_get_error() == ::bfd_error_file_ambiguously_recognized) {
      std::free(matches);
    }
    return false;
  }

  if ((bfd_get_file_flags(bfd_) & HAS_SYMS) == 0) {
    return false;
  }

  unsigned int size = 0;
  long num_symbols = bfd_read_minisymbols(bfd_, false,
                                          (void **)&bfd_symbols_, &size);
  if (num_symbols == 0) {
    std::free(bfd_symbols_);
    bfd_symbols_ = nullptr;
    num_symbols = bfd_read_minisymbols(bfd_, true,
                                       (void**)&bfd_symbols_, &size);
  }
  if (num_symbols <= 0) {
    return false;
  }

  Detail detail(bfd_symbols_, address);
  ::bfd_map_over_sections(bfd_, callback_for_each_section, &detail);
  if (!detail.found) {
    return false;
  }

  *stream << ": ";
  if (!detail.function || (detail.function[0] == '\0')) {
    *stream << "???";
  } else {
    int status = 0;
    char *demangled_function =
        ::abi::__cxa_demangle(detail.function, 0, 0, &status);
    if (demangled_function) {
      *stream << demangled_function;
      std::free(demangled_function);
    } else {
      *stream << detail.function;
    }
  }

  *stream << " (";
  if (!detail.filename || (detail.filename[0] == '\0') || (detail.line == 0)) {
    *stream << "???:???";
  } else {
    *stream << ::basename(detail.filename) << ':' << detail.line;
  }

  if (std::strcmp(image_name, DEFAULT_IMAGE_NAME) != 0) {
    *stream << " in " << image_name;
  }
  *stream << ')';

//  bfd_find_inliner_info(bfd_, &detail.filename, &detail.function,
//                        &detail.line);

  return true;
}
#else  // HAVE_LIBBFD
bool Resolver::resolve(void *, std::ostream *) {
  return false;
}
#endif  // HAVE_LIBBFD

}  // namespace

bool Backtrace::backtrace(int skip_count, std::vector<void *> *addresses) try {
  static Mutex mutex(MUTEX_UNLOCKED);
  Lock lock(&mutex);

  if ((skip_count < BACKTRACE_MIN_SKIP_COUNT) ||
      (skip_count > BACKTRACE_MAX_SKIP_COUNT)) {
    return false;
  }
  if (!addresses) {
    return false;
  }
  ++skip_count;

  std::vector<void *> buf(BACKTRACE_MIN_BUF_SIZE);
  for ( ; ; ) {
    const int depth = ::backtrace(&buf[0], static_cast<int>(buf.size()));
    if (depth < static_cast<int>(buf.size())) {
      if (depth <= skip_count) {
        return false;
      }
      buf.resize(depth);
      break;
    }
    if (buf.size() >= BACKTRACE_MAX_BUF_SIZE) {
      break;
    }
    buf.resize(buf.size() * 2);
  }
  addresses->assign(buf.begin() + skip_count, buf.end());
  return true;
} catch (...) {
  return false;
}

bool Backtrace::resolve(void *address, std::string *entry) try {
  static Mutex mutex(MUTEX_UNLOCKED);
  Lock lock(&mutex);

  if (!address || !entry) {
    return false;
  }

  std::ostringstream stream;
  if (Resolver::resolve(address, &stream)) {
    entry->assign(stream.str());
    return true;
  }

  char **symbols = ::backtrace_symbols(&address, 1);
  if (!symbols) {
    return false;
  }
  char *first_symbol = symbols[0];
  std::free(symbols);
  if (!first_symbol) {
    return false;
  }
  entry->append(first_symbol);
  return true;
} catch (...) {
  return false;
}

bool Backtrace::pretty_backtrace(int skip_count,
                                 std::vector<std::string> *entries) try {
  if ((skip_count < BACKTRACE_MIN_SKIP_COUNT) ||
      (skip_count > BACKTRACE_MAX_SKIP_COUNT)) {
    return false;
  }
  if (!entries) {
    return false;
  }
  ++skip_count;

  std::vector<void *> addresses;
  if (!backtrace(skip_count, &addresses)) {
    return false;
  }

  entries->clear();
  for (std::size_t i = 0; i < addresses.size(); ++i) {
    std::string entry;
    if (resolve(addresses[i], &entry)) {
      entry.insert(0, (i == 0) ? "at " : "by ", 3);
      entries->push_back(entry);
    }
  }
  return true;
} catch (...) {
  return false;
}

}  // namespace grnxx

#else  // HAVE_BACKTRACE

namespace grnxx {

bool Backtrace::backtrace(int, std::vector<void *> *) {
  return false;
}

bool Backtrace::resolve(void *, std::string *) {
  return false;
}

bool Backtrace::pretty_backtrace(int, std::vector<std::string> *) {
  return false;
}

}  // namespace grnxx

#endif  // HAVE_BACKTRACE
