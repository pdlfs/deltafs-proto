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
#include "pdlfs-common/port.h"
#include "pdlfs-common/status.h"

#include <list>
#include <stddef.h>
#include <string>
#if __cplusplus >= 201103L
#define OVERRIDE override
#else
#define OVERRIDE
#endif

namespace pdlfs {

class Cache;
class DB;
class FilterPolicy;
class RandomAccessFileStats;
class Stat;

struct DirId;
struct FilesystemDbStats;

struct FilesystemReadonlyDbOptions {
  FilesystemReadonlyDbOptions();
  // Read options from env.
  void ReadFromEnv();
  // Max number of table files we open.
  // Setting to 0 disables caching effectively.
  // Default: 0
  size_t table_cache_size;
  // Bloom filter bits per key.
  // Use 0 to disable filters altogether.
  // Default: 10
  size_t filter_bits_per_key;
  // Block cache size.
  // Setting to 0 disables caching effectively.
  // Default: 0
  size_t block_cache_size;
  // Collect performance stats for db table files.
  // Default: false
  bool enable_io_monitoring;
  // Detach db directory on db closing.
  // Default: false
  bool detach_dir_on_close;
  // Log to stderr.
  // Default: false
  bool use_default_logger;
};

class FilesystemReadonlyDbEnvWrapper : public EnvWrapper {
 public:
  FilesystemReadonlyDbEnvWrapper(const FilesystemReadonlyDbOptions& options,
                                 Env* base);
  virtual ~FilesystemReadonlyDbEnvWrapper();
  virtual Status NewRandomAccessFile(const char* f,
                                     RandomAccessFile** r) OVERRIDE;
  void SetDbLoc(const std::string& dbloc);
  // Total number of random read operations performed on db table files.
  uint64_t TotalRndTblReads();
  // Total amount of data read randomly from db table files. This could include
  // both data read for background compaction (when compaction input
  // pre-fetching is turned off) and data read for foreground db queries. A db
  // may also be configured to cache data in memory reducing physical random
  // data reads.
  uint64_t TotalRndTblBytesRead();

 private:
  std::list<RandomAccessFileStats*> randomaccessfile_repo_;
  std::string dbprefix_;
  FilesystemReadonlyDbOptions options_;
  port::Mutex mu_;
};

class FilesystemReadonlyDb {
 public:
  FilesystemReadonlyDb(const FilesystemReadonlyDbOptions& options, Env* base);
  FilesystemReadonlyDbEnvWrapper* GetDbEnv() { return env_wrapper_; }
  DB* TEST_GetDbRep() { return db_; }
  Status Get(const DirId& id, const Slice& fname, Stat* stat,
             FilesystemDbStats* stats);
  Status Open(const std::string& dbloc);
  ~FilesystemReadonlyDb();

 private:
  struct Tx;
  struct MetadataDb;
  MetadataDb* mdb_;
  void operator=(const FilesystemReadonlyDb&);
  FilesystemReadonlyDb(const FilesystemReadonlyDb& other);
  FilesystemReadonlyDbOptions options_;
  FilesystemReadonlyDbEnvWrapper* env_wrapper_;
  const FilterPolicy* filter_policy_;
  Cache* table_cache_;
  Cache* block_cache_;
  DB* db_;
};

}  // namespace pdlfs
#undef OVERRIDE
