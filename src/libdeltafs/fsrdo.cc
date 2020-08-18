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
#include "fsrdo.h"

#include "pdlfs-common/leveldb/filenames.h"
#include "pdlfs-common/leveldb/filter_policy.h"
#include "pdlfs-common/leveldb/options.h"
#include "pdlfs-common/leveldb/readonly.h"

#include "pdlfs-common/cache.h"
#include "pdlfs-common/env.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/strutil.h"

namespace pdlfs {

FilesystemReadonlyDbOptions::FilesystemReadonlyDbOptions()
    : filter_bits_per_key(10),
      enable_io_monitoring(false),
      detach_dir_on_close(false),
      use_default_logger(false) {}

FilesystemReadonlyDbEnvWrapper::FilesystemReadonlyDbEnvWrapper(
    const FilesystemReadonlyDbOptions& options, Env* base)
    : EnvWrapper(base), options_(options) {}

namespace {
template <typename T>
inline void CleanUpRepo(std::list<T*>* v) {
  typename std::list<T*>::iterator it = v->begin();
  for (; it != v->end(); ++it) {
    delete *it;
  }
  v->clear();
}

}  // namespace

FilesystemReadonlyDbEnvWrapper::~FilesystemReadonlyDbEnvWrapper() {
  CleanUpRepo(&randomaccessfile_repo_);
}

void FilesystemReadonlyDbEnvWrapper::SetDbLoc(const std::string& dbloc) {
  dbprefix_ = dbloc + "/";
}

namespace {
void ReadBoolFromEnv(const char* key, bool* dst) {
  const char* env = getenv(key);
  if (!env || !env[0]) return;
  bool tmp;
  if (ParsePrettyBool(env, &tmp)) {
    *dst = tmp;
  }
}
}  // namespace

// Read options from system env. All env keys start with "DELTAFS_Rr_".
void FilesystemReadonlyDbOptions::ReadFromEnv() {
  ReadBoolFromEnv("DELTAFS_Rr_use_default_logger", &use_default_logger);
}

Status FilesystemReadonlyDb::Open(const std::string& dbloc) {
  DBOptions dbopts;
  dbopts.create_if_missing = false;
  dbopts.detach_dir_on_close = options_.detach_dir_on_close;
  dbopts.table_cache = table_cache_;
  dbopts.block_cache = block_cache_;
  dbopts.filter_policy = filter_policy_;
  dbopts.info_log = options_.use_default_logger ? Logger::Default() : NULL;
  dbopts.env = env_wrapper_;
  return ReadonlyDB::Open(dbopts, dbloc, &db_);
}

FilesystemReadonlyDb::FilesystemReadonlyDb(
    const FilesystemReadonlyDbOptions& options, Env* base)
    : options_(options),
      env_wrapper_(new FilesystemReadonlyDbEnvWrapper(options, base)),
      filter_policy_(options_.filter_bits_per_key != 0
                         ? NewBloomFilterPolicy(options_.filter_bits_per_key)
                         : NULL),
      table_cache_(NewLRUCache(0)),
      block_cache_(NewLRUCache(0)),
      db_(NULL) {}

FilesystemReadonlyDb::~FilesystemReadonlyDb() {
  delete db_;
  delete env_wrapper_;
  delete block_cache_;
  delete table_cache_;
  delete filter_policy_;
}

namespace {
// REQUIRES: dbprefix is given as dbhome + "/"
bool TryResolveFileType(const std::string& dbprefix, const char* filename,
                        FileType* type) {
  uint64_t filenum;
  if (strncmp(filename, dbprefix.c_str(), dbprefix.size()) == 0) {
    if (ParseFileName(filename + dbprefix.size(), &filenum, type)) {
      return true;
    }
  }
  return false;
}

}  // namespace

Status FilesystemReadonlyDbEnvWrapper::NewRandomAccessFile(
    const char* f, RandomAccessFile** r) {
  RandomAccessFile* file;
  FileType type;
  Status s = target()->NewRandomAccessFile(f, &file);
  if (!s.ok()) {
    *r = NULL;
  } else if (  ///
      options_.enable_io_monitoring &&
      TryResolveFileType(dbprefix_, f, &type) && type == kTableFile) {
    MutexLock ml(&mu_);
    RandomAccessFileStats* stats = new RandomAccessFileStats;
    *r = new MonitoredRandomAccessFile(stats, file);
    randomaccessfile_repo_.push_back(stats);
  } else {
    *r = file;
  }
  return s;
}

}  // namespace pdlfs
