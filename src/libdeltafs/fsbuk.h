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
#include "pdlfs-common/fsdbx.h"
#if __cplusplus >= 201103L
#define OVERRIDE override
#else
#define OVERRIDE
#endif
namespace pdlfs {

class FilterPolicy;
class Cache;

struct BukDbOptions {
  BukDbOptions();
  // Read options from env.
  void ReadFromEnv();
  // Write buffer size for db write ahead log files.
  // Set 0 to disable.
  // Default: 4KB
  uint64_t write_ahead_log_buffer;
  // Write buffer size for db manifest files.
  // Set 0 to disable.
  // Default: 4KB
  uint64_t manifest_buffer;
  // Write buffer size for db table files.
  // Set 0 to disable.
  // Default: 256KB
  uint64_t table_buffer;
  // Max size for a MemTable.
  // Default: 8MB
  size_t memtable_size;
  // Size for a table block.
  // Default: 4KB
  size_t block_size;
  // Bloom filter bits per key.
  // Use 0 to disable filters altogether.
  // Default: 10
  size_t filter_bits_per_key;
  // Number of keys between restart points for delta encoding of keys.
  // Default: 16
  int block_restart_interval;
  // Detach db directory on db closing, flushing directory contents and umount
  // the directory.
  // Default: false
  bool detach_dir_on_close;
  // Disable write ahead logging.
  // Default: false
  bool disable_write_ahead_logging;
  // Enable snappy compression.
  // Default: false
  bool compression;
};

struct BukDbStats {
  BukDbStats();
  // Total amount of key bytes pushed to db.
  uint64_t putkeybytes;
  // Total amount of val bytes pushed to db.
  uint64_t putbytes;
  // Total number of put operations.
  uint64_t puts;
};

class BukDbEnvWrapper : public EnvWrapper {
 public:
  BukDbEnvWrapper(const BukDbOptions& options, Env* base);
  virtual ~BukDbEnvWrapper();
  virtual Status NewWritableFile(const char* f, WritableFile** r) OVERRIDE;
  void SetDbLoc(const std::string& dbloc);

 private:
  const BukDbOptions options_;
  std::string dbprefix_;
};

// Client-side filesystem bulk db context.
class BukDb {
 public:
  BukDb(const BukDbOptions& options, Env* base);
  ~BukDb();

  DB* TEST_GetDbRep() { return db_; }
  Status Open(const std::string& dbloc);
  Status Put(const DirId& id, const Slice& fname, const Stat& stat,
             BukDbStats* stats);
  Status Flush();

 private:
  struct Tx;
  void operator=(const BukDb& other);
  BukDb(const BukDb&);
  typedef MXDB<DB, Slice, Status, kNameInKey> MDB;
  MDB* mdb_;
  BukDbOptions options_;
  BukDbEnvWrapper* env_wrapper_;
  const FilterPolicy* filter_policy_;
  Cache* table_cache_;
  Cache* block_cache_;
  DB* db_;
};

}  // namespace pdlfs
#undef OVERRIDE
