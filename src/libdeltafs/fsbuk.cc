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
#include "fsbuk.h"

#include "pdlfs-common/leveldb/db.h"
#include "pdlfs-common/leveldb/filenames.h"
#include "pdlfs-common/leveldb/filter_policy.h"
#include "pdlfs-common/leveldb/options.h"
#include "pdlfs-common/leveldb/write_batch.h"

#include "pdlfs-common/cache.h"
#include "pdlfs-common/strutil.h"

namespace pdlfs {

BukDbEnvWrapper::BukDbEnvWrapper(const BukDbOptions& options, Env* base)
    : EnvWrapper(base != NULL ? base : Env::Default()), options_(options) {}

BukDbStats::BukDbStats() : putkeybytes(0), putbytes(0), puts(0) {}

BukDbOptions::BukDbOptions()
    : write_ahead_log_buffer(4 << 10),
      manifest_buffer(4 << 10),
      table_buffer(256 << 10),
      memtable_size(8 << 20),
      block_size(4 << 10),
      filter_bits_per_key(10),
      block_restart_interval(16),
      detach_dir_on_close(false),
      disable_write_ahead_logging(false),
      compression(false) {}

BukDbEnvWrapper::~BukDbEnvWrapper() {}

void BukDbEnvWrapper::SetDbLoc(const std::string& dbloc) {
  dbprefix_ = dbloc + "/";
}

struct BukDb::Tx {
  WriteBatch bat;
};

BukDb::BukDb(const BukDbOptions& options, Env* base)
    : mdb_(NULL),
      options_(options),
      env_wrapper_(new BukDbEnvWrapper(options, base)),
      filter_policy_(options_.filter_bits_per_key != 0
                         ? NewBloomFilterPolicy(options_.filter_bits_per_key)
                         : NULL),
      table_cache_(NewLRUCache(0)),
      block_cache_(NewLRUCache(0)),
      db_(NULL) {}

BukDb::~BukDb() {
  delete db_;
  delete block_cache_;
  delete table_cache_;
  delete filter_policy_;
  delete env_wrapper_;
  delete mdb_;
}

namespace {
template <typename T>
void ReadIntegerOptionFromEnv(const char* key, T* const dst) {
  const char* env = getenv(key);
  if (!env || !env[0]) return;
  uint64_t tmp;
  if (ParsePrettyNumber(env, &tmp)) {
    *dst = static_cast<T>(tmp);
  }
}

void ReadBoolFromEnv(const char* key, bool* dst) {
  const char* env = getenv(key);
  if (!env || !env[0]) return;
  bool tmp;
  if (ParsePrettyBool(env, &tmp)) {
    *dst = tmp;
  }
}
}  // namespace

// Read options from system env. All env keys start with "DELTAFS_Bk_".
void BukDbOptions::ReadFromEnv() {
  ReadIntegerOptionFromEnv("DELTAFS_Bk_write_ahead_log_buffer",
                           &write_ahead_log_buffer);
  ReadIntegerOptionFromEnv("DELTAFS_Bk_manifest_buffer", &manifest_buffer);
  ReadIntegerOptionFromEnv("DELTAFS_Bk_table_buffer", &table_buffer);
  ReadIntegerOptionFromEnv("DELTAFS_Bk_memtable_size", &memtable_size);
  ReadIntegerOptionFromEnv("DELTAFS_Bk_block_size", &block_size);
  ReadIntegerOptionFromEnv("DELTAFS_Bk_filter_bits_per_key",
                           &filter_bits_per_key);
  ReadIntegerOptionFromEnv("DELTAFS_Bk_block_restart_interval",
                           &block_restart_interval);
  ReadBoolFromEnv("DELTAFS_Bk_disable_write_ahead_logging",
                  &disable_write_ahead_logging);
  ReadBoolFromEnv("DELTAFS_Bk_compression", &compression);
}

Status BukDb::Open(const std::string& dbloc) {
  DBOptions dbopts;
  dbopts.create_if_missing = true;
  dbopts.table_builder_skip_verification = true;
  dbopts.sync_log_on_close = true;
  dbopts.detach_dir_on_close = options_.detach_dir_on_close;
  dbopts.disable_write_ahead_log = options_.disable_write_ahead_logging;
  dbopts.disable_compaction = true;
  dbopts.disable_seek_compaction = true;
  dbopts.rotating_manifest = true;
  dbopts.skip_lock_file = true;
  dbopts.table_cache = table_cache_;
  dbopts.block_cache = block_cache_;
  dbopts.filter_policy = filter_policy_;
  dbopts.write_buffer_size = options_.memtable_size;
  dbopts.block_size = options_.block_size;
  dbopts.block_restart_interval = options_.block_restart_interval;
  dbopts.max_mem_compact_level = 0;
  dbopts.info_log = Logger::Default();
  dbopts.compression =
      options_.compression ? kSnappyCompression : kNoCompression;
  dbopts.error_if_exists = true;
  env_wrapper_->SetDbLoc(dbloc);
  dbopts.env = env_wrapper_;
  Status status = DB::Open(dbopts, dbloc, &db_);
  if (status.ok()) {
    mdb_ = new MDB(db_);
  }
  return status;
}

Status BukDb::Put(  ///
    const DirId& id, const Slice& fname, const Stat& stat,
    BukDbStats* const stats) {
  WriteOptions options;
  Tx* const tx(NULL);
  return mdb_->PUT<Key>(id, fname, stat, fname, &options, tx, stats);
}

Status BukDb::Flush() {
  FlushOptions opts;
  opts.force_flush_l0 = false;
  return db_->FlushMemTable(opts);
}

Status BukDb::DestroyDb(const std::string& dbloc, Env* env) {
  if (env) {
    // XXX: The following code forces the db dir to be mounted in case where the
    // underlying env is an object store instead of a POSIX filesystem.
    // Created dir will eventually be deleted by the subsequent DestroyDB() call
    // so no harm will be done.
    //
    // When env is NULL, this step is unnecessary because Env::Default() will be
    // used which does not require a pre-mount.
    env->CreateDir(dbloc.c_str());
  }
  DBOptions dbopts;
  dbopts.skip_lock_file = true;
  dbopts.env = env;
  return DestroyDB(dbloc, dbopts);
}

namespace {
// REQUIRES: prefix is given as dbhome + "/"
inline bool TryResolveFileType(  ///
    const std::string& prefix, const char* filename, FileType* type) {
  uint64_t ignored_filenum;
  if (strncmp(filename, prefix.c_str(), prefix.size()) == 0) {
    if (ParseFileName(filename + prefix.size(), &ignored_filenum, type)) {
      return true;
    }
  }
  return false;
}

}  // namespace

Status BukDbEnvWrapper::NewWritableFile(const char* f, WritableFile** r) {
  WritableFile* file;
  FileType type;
  Status s = target()->NewWritableFile(f, &file);
  if (!s.ok()) {
    *r = NULL;
  } else {
    if (TryResolveFileType(dbprefix_, f, &type)) {
      uint64_t bufsize = 0;
      switch (type) {
        case kLogFile:
          bufsize = options_.write_ahead_log_buffer;
          break;
        case kDescriptorFile:
          bufsize = options_.manifest_buffer;
          break;
        case kTableFile:
          bufsize = options_.table_buffer;
          break;
        default:
          break;
      }
      if (bufsize) {
        file = new MinMaxBufferedWritableFile(file, bufsize, (bufsize << 1));
      }
    }
    *r = file;
  }
  return s;
}

}  // namespace pdlfs
