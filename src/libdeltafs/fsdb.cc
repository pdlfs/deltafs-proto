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
#include "fsdb.h"

#include "fsenv.h"

#include "pdlfs-common/leveldb/db.h"
#include "pdlfs-common/leveldb/filter_policy.h"
#include "pdlfs-common/leveldb/options.h"
#include "pdlfs-common/leveldb/snapshot.h"
#include "pdlfs-common/leveldb/write_batch.h"

#include "pdlfs-common/cache.h"

namespace pdlfs {

FilesystemDbOptions::FilesystemDbOptions()
    : write_buffer_size(4u << 20u),
      table_file_size(2u << 20u),
      block_size(64u << 10u),
      table_cache_size(1024),
      filter_bits_per_key(14),
      block_cache_size(32u << 20u),
      block_restart_interval(16),
      level_factor(10),
      l1_compaction_trigger(5),
      l0_compaction_trigger(4),
      l0_soft_limit(8),
      l0_hard_limit(12),
      enable_io_monitoring(false),
      use_default_logger(false),
      disable_compaction(false),
      compression(false) {}

FilesystemDbStats::FilesystemDbStats()
    : putkeybytes(0),
      putbytes(0),
      puts(0),
      getkeybytes(0),
      getbytes(0),
      gets(0) {}

Status FilesystemDb::Open(const std::string& dbloc) {
  DBOptions dbopts;
  dbopts.error_if_exists = dbopts.create_if_missing = true;
  dbopts.disable_compaction = options_.disable_compaction;
  dbopts.disable_seek_compaction = true;
  dbopts.skip_lock_file = true;
  dbopts.table_cache = table_cache_;
  dbopts.block_cache = block_cache_;
  dbopts.filter_policy = filter_;
  dbopts.write_buffer_size = options_.write_buffer_size;
  dbopts.table_file_size = options_.table_file_size;
  dbopts.block_size = options_.block_size;
  dbopts.block_restart_interval = options_.block_restart_interval;
  dbopts.level_factor = options_.level_factor;
  dbopts.l1_compaction_trigger = options_.l1_compaction_trigger;
  dbopts.l0_compaction_trigger = options_.l0_compaction_trigger;
  dbopts.l0_soft_limit = options_.l0_soft_limit;
  dbopts.l0_hard_limit = options_.l0_hard_limit;
  dbopts.info_log = options_.use_default_logger ? Logger::Default() : NULL;
  dbopts.compression =
      options_.compression ? kSnappyCompression : kNoCompression;
  dbenv_->SetDbLoc(dbloc);
  dbopts.env = dbenv_;
  Status status = DB::Open(dbopts, dbloc, &db_);
  if (status.ok()) {
    mdb_ = new MDB(db_);
  }
  return status;
}

struct FilesystemDb::Tx {
  const Snapshot* snap;
  WriteBatch bat;
};

FilesystemDb::FilesystemDb(const FilesystemDbOptions& options)
    : mdb_(NULL),
      options_(options),
      dbenv_(new FilesystemDbEnvWrapper(options)),
      filter_(NewBloomFilterPolicy(options_.filter_bits_per_key)),
      table_cache_(NewLRUCache(options_.table_cache_size)),
      block_cache_(NewLRUCache(options_.block_cache_size)),
      db_(NULL) {}

FilesystemDb::~FilesystemDb() {
  delete db_;
  delete block_cache_;
  delete table_cache_;
  delete filter_;
  delete dbenv_;
  delete mdb_;
}

Status FilesystemDb::DrainCompaction() { return db_->DrainCompactions(); }

Status FilesystemDb::Flush() { return db_->FlushMemTable(FlushOptions()); }

Status FilesystemDb::Put(  ///
    const DirId& id, const Slice& fname, const Stat& stat,
    FilesystemDbStats* const stats) {
  WriteOptions options;
  Tx* const tx = NULL;
  return mdb_->PUT<Key>(id, fname, stat, fname, &options, tx, stats);
}

Status FilesystemDb::Get(  ///
    const DirId& id, const Slice& fname, Stat* const stat,
    FilesystemDbStats* const stats) {
  ReadOptions options;
  Tx* const tx = NULL;
  return mdb_->GET<Key>(id, fname, stat, NULL, &options, tx, stats);
}

Status FilesystemDb::Delete(const DirId& id, const Slice& fname) {
  WriteOptions options;
  Tx* const tx = NULL;
  return mdb_->DELETE<Key>(id, fname, &options, tx);
}

}  // namespace pdlfs
