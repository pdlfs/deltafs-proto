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

#include "pdlfs-common/status.h"

#include <stddef.h>

namespace pdlfs {

class Cache;
class DB;
class Env;
class FilterPolicy;

struct ReadonlyDbOptions {
  ReadonlyDbOptions();
  // Read options from env.
  void ReadFromEnv();
  // Bloom filter bits per key.
  // Use 0 to disable filters altogether.
  // Default: 10
  size_t filter_bits_per_key;
  // Detach db directory on db closing.
  // Default: false
  bool detach_dir_on_close;
  // Log to stderr.
  // Default: false
  bool use_default_logger;
};

class ReadonlyDb {
 public:
  ReadonlyDb(const ReadonlyDbOptions& options, Env* base);
  DB* TEST_GetDbRep() { return db_; }
  Status Open(const std::string& dbloc);
  ~ReadonlyDb();

 private:
  void operator=(const ReadonlyDb& other);
  ReadonlyDb(const ReadonlyDb&);
  ReadonlyDbOptions options_;
  Env* env_;
  const FilterPolicy* filter_policy_;
  Cache* table_cache_;
  Cache* block_cache_;
  DB* db_;
};

}  // namespace pdlfs
