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

#include "pdlfs-common/fsdbx.h"

namespace pdlfs {

class FilesystemDbEnvWrapper;
class FilterPolicy;
class Cache;

struct FilesystemDbOptions {
  FilesystemDbOptions();
  // Read options from env.
  void ReadFromEnv();
  // Write buffer size for db write ahead log files. Set 0 to disable.
  uint64_t write_ahead_log_buffer;
  // Write buffer size for db manifest files. Set 0 to disable.
  uint64_t manifest_buffer;
  // Write buffer size for db table files. Set 0 to disable.
  uint64_t table_buffer;
  // Max size for a MemTable.
  size_t memtable_size;
  // Planned size for each on-disk table file.
  size_t table_size;
  // Size for a table block.
  size_t block_size;
  // Max number of table files we open.
  // Use 0 will disable caching effectively.
  size_t table_cache_size;
  // Bloom filter bits per key. Use 0 to disable filters altogether.
  size_t filter_bits_per_key;
  // Block cache size.
  // Use 0 will disable caching effectively.
  size_t block_cache_size;
  // Number of keys between restart points for delta encoding of keys.
  int block_restart_interval;
  // The size ratio between two levels.
  int level_factor;
  // Planned number of files for level 1.
  int l1_compaction_trigger;
  // Number of files in level-0 until compaction starts.
  int l0_compaction_trigger;
  // Number of files in level-0 until writes are slowed down.
  int l0_soft_limit;
  // Number of files in level-0 until writes are entirely stalled.
  int l0_hard_limit;
  // Collect performance stats for db table files.
  bool enable_io_monitoring;
  // Log to stderr.
  bool use_default_logger;
  // Disable write ahead logging.
  bool disable_write_ahead_logging;
  // Disable background table compaction.
  bool disable_compaction;
  // Enable snappy compression.
  bool compression;
};

struct FilesystemDbStats {
  FilesystemDbStats();
  void Merge(const FilesystemDbStats& other);
  // Total amount of key bytes pushed to db.
  uint64_t putkeybytes;
  // Total amount of val bytes pushed to db.
  uint64_t putbytes;
  // Total number of put operations.
  uint64_t puts;
  // Total number of key bytes read out of db.
  uint64_t getkeybytes;
  // Total number of val bytes read out of db.
  uint64_t getbytes;
  // Total number of get operations.
  uint64_t gets;
};

class FilesystemDb {
 public:
  FilesystemDb(const FilesystemDbOptions& options, Env* base);
  ~FilesystemDb();

  std::string GetDbLevel0Events();
  std::string GetDbStats();
  FilesystemDbEnvWrapper* GetDbEnv() { return dbenv_; }
  static Status DestroyDb(const std::string& dbloc, Env* env);
  Status Open(const std::string& dbloc);
  Status Get(const DirId& id, const Slice& fname, Stat* stat,
             FilesystemDbStats* stats);
  Status Put(const DirId& id, const Slice& fname, const Stat& stat,
             FilesystemDbStats* stats);
  Status Delete(const DirId& id, const Slice& fname);
  Status DrainCompaction();
  Status Flush(bool force_flush_l0);

 private:
  struct Tx;
  void operator=(const FilesystemDb& fsdb);
  FilesystemDb(const FilesystemDb&);
  typedef MXDB<DB, Slice, Status, kNameInKey> MDB;
  MDB* mdb_;
  FilesystemDbOptions options_;
  FilesystemDbEnvWrapper* dbenv_;
  const FilterPolicy* filter_;
  Cache* table_cache_;
  Cache* block_cache_;
  DB* db_;
};

}  // namespace pdlfs
