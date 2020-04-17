/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * with the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of CMU, TRIAD, Los Alamos National Laboratory, LANL, the
 *    U.S. Government, nor the names of its contributors may be used to endorse
 *    or promote products derived from this software without specific prior
 *    written permission.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#pragma once

#include "pdlfs-common/fstypes.h"

namespace pdlfs {
// User id information.
struct User {
  uint32_t uid;
  uint32_t gid;
};

// Filesystem interface at the server side.
class FilesystemIf {
 public:
  FilesystemIf() {}
  virtual ~FilesystemIf();
  virtual Status Mkfls(const User& who, const LookupStat& parent,
                       const Slice& namearr, uint32_t mode, uint32_t* n) = 0;
  virtual Status Mkfle(const User& who, const LookupStat& parent,
                       const Slice& name, uint32_t mode, Stat* stat) = 0;
  virtual Status Mkdir(const User& who, const LookupStat& parent,
                       const Slice& name, uint32_t mode, Stat* stat) = 0;
  virtual Status Lokup(const User& who, const LookupStat& parent,
                       const Slice& name, LookupStat* stat) = 0;
  virtual Status Lstat(const User& who, const LookupStat& parent,
                       const Slice& name, Stat* stat) = 0;
};

#if __cplusplus >= 201103L
#define OVERRIDE override
#else
#define OVERRIDE
#endif
class FilesystemWrapper : public FilesystemIf {
 public:
  FilesystemWrapper() {}
  virtual ~FilesystemWrapper();
  virtual Status Mkfls(const User& who, const LookupStat& parent,
                       const Slice& namearr, uint32_t mode,
                       uint32_t* n) OVERRIDE;
  virtual Status Mkfle(const User& who, const LookupStat& parent,
                       const Slice& name, uint32_t mode, Stat* stat) OVERRIDE;
  virtual Status Mkdir(const User& who, const LookupStat& parent,
                       const Slice& name, uint32_t mode, Stat* stat) OVERRIDE;
  virtual Status Lokup(const User& who, const LookupStat& parent,
                       const Slice& name, LookupStat* stat) OVERRIDE;
  virtual Status Lstat(const User& who, const LookupStat& parent,
                       const Slice& name, Stat* stat) OVERRIDE;
};
#undef OVERRIDE

}  // namespace pdlfs
