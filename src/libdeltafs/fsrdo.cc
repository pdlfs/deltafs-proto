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

#include "pdlfs-common/leveldb/db.h"
#include "pdlfs-common/leveldb/filter_policy.h"
#include "pdlfs-common/leveldb/options.h"

#include "pdlfs-common/cache.h"
#include "pdlfs-common/env.h"
#include "pdlfs-common/strutil.h"

namespace pdlfs {

ReadonlyDbOptions::ReadonlyDbOptions()
    : filter_bits_per_key(10),
      detach_dir_on_close(false),
      use_default_logger(false) {}

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
void ReadonlyDbOptions::ReadFromEnv() {
  ReadBoolFromEnv("DELTAFS_Rr_use_default_logger", &use_default_logger);
}

Status ReadonlyDb::Open(const std::string& dbloc) {
  DBOptions dbopts;
  dbopts.create_if_missing = false;
  dbopts.detach_dir_on_close = options_.detach_dir_on_close;
  dbopts.table_cache = table_cache_;
  dbopts.block_cache = block_cache_;
  dbopts.filter_policy = filter_policy_;
  dbopts.info_log = options_.use_default_logger ? Logger::Default() : NULL;
  dbopts.env = env_;
  return DB::Open(dbopts, dbloc, &db_);
}

ReadonlyDb::ReadonlyDb(const ReadonlyDbOptions& options, Env* base)
    : options_(options),
      env_(base),
      filter_policy_(options_.filter_bits_per_key != 0
                         ? NewBloomFilterPolicy(options_.filter_bits_per_key)
                         : NULL),
      table_cache_(NewLRUCache(0)),
      block_cache_(NewLRUCache(0)),
      db_(NULL) {}

ReadonlyDb::~ReadonlyDb() {
  delete db_;
  delete block_cache_;
  delete table_cache_;
  delete filter_policy_;
}

}  // namespace pdlfs
