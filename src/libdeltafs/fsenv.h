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

#include "pdlfs-common/env.h"
#include "pdlfs-common/env_files.h"
#include "pdlfs-common/port.h"

#include <list>

namespace pdlfs {
struct FilesystemOptions;
#if __cplusplus >= 201103L
#define OVERRIDE override
#else
#define OVERRIDE
#endif
class FilesystemEnv : public EnvWrapper {
 public:
  explicit FilesystemEnv(const FilesystemOptions& options);
  virtual ~FilesystemEnv();

  virtual Status NewSequentialFile(const char* f, SequentialFile** r) OVERRIDE;
  virtual Status NewRandomAccessFile(const char* f,
                                     RandomAccessFile** r) OVERRIDE;
  virtual Status NewWritableFile(const char* f, WritableFile** r) OVERRIDE;

  // Total number of db table files opened for read. A db may be configured to
  // cache table file handles in memory reducing physical file opens.
  size_t TotalTableFilesOpenedForRead();
  // Total amount of data read from db table files. This includes both data read
  // for background db compaction and data read for foreground db queries. A db
  // may be configured to cache data in memory reducing physical data reads.
  uint64_t TotalDbBytesRead();
  // Total number of db table files opened for write.
  size_t TotalTableFilesOpenedForWrite();
  // Total amount of data written to db table files. Data written to db
  // write-ahead log files, manifest files, or info log files are not counted.
  uint64_t TotalDbBytesWritten();

 private:
  std::list<SequentialFileStats*> sequentialfile_repo_;
  std::list<RandomAccessFileStats*> randomaccessfile_repo_;
  std::list<WritableFileStats*> writablefile_repo_;
  port::Mutex mu_;
};
#undef OVERRIDE
}  // namespace pdlfs
