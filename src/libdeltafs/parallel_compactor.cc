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
#include "pdlfs-common/leveldb/db.h"
#include "pdlfs-common/leveldb/iterator.h"
#include "pdlfs-common/leveldb/options.h"
#include "pdlfs-common/leveldb/readonly.h"

#include "pdlfs-common/env.h"
#include "pdlfs-common/strutil.h"

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

namespace pdlfs {
namespace {
DBOptions FLAGS_src_dbopts;

// Compaction input.
const char* FLAGS_src_prefix = NULL;

// Total number of ranks.
int FLAGS_comm_size = 1;

// My rank number.
int FLAGS_rank = 0;

class Mapper {
 private:
  Env* env_;  // Not owned by us
  DB* db_;

  void Open() {
    DBOptions dbopts = FLAGS_src_dbopts;
    dbopts.env = env_;
    char dbid[100];
    snprintf(dbid, sizeof(dbid), "/r%d", FLAGS_rank);
    std::string dbpath = FLAGS_src_prefix;
    dbpath += dbid;
    Status s = ReadonlyDB::Open(dbopts, dbpath, &db_);
    if (!s.ok()) {
      fprintf(stderr, "%d: Cannot open db: %s\n", FLAGS_rank,
              s.ToString().c_str());
      MPI_Abort(MPI_COMM_WORLD, 1);
    }
  }

 public:
  explicit Mapper(Env* env) : env_(env), db_(NULL) {
    if (env_ == NULL) {
      env_ = Env::Default();
    }
  }
  ~Mapper() { delete db_; }

  void Run() {
    Open();
    ReadOptions read_options;
    read_options.fill_cache = false;
    Iterator* const iter = db_->NewIterator(read_options);
    iter->SeekToFirst();
    while (iter->Valid()) {
      fprintf(stderr, "%s\n", EscapeString(iter->key()).c_str());
      iter->Next();
    }
    delete iter;
  }
};

}  // namespace
}  // namespace pdlfs

namespace {
void BM_Main(int* const argc, char*** const argv) {
  std::string default_src_prefix;
  if (pdlfs::FLAGS_src_prefix == NULL) {
    default_src_prefix = "/tmp/deltafs_bm";
    pdlfs::FLAGS_src_prefix = default_src_prefix.c_str();
  }

  pdlfs::Mapper mapper(NULL);
  mapper.Run();
}
}  // namespace

int main(int argc, char* argv[]) {
  int rv = MPI_Init(&argc, &argv);
  if (rv != 0) {
    fprintf(stderr, "Cannot init mpi\n");
    exit(1);
  }
  MPI_Comm_size(MPI_COMM_WORLD, &pdlfs::FLAGS_comm_size);
  MPI_Comm_rank(MPI_COMM_WORLD, &pdlfs::FLAGS_rank);
  BM_Main(&argc, &argv);
  MPI_Finalize();
  return 0;
}
