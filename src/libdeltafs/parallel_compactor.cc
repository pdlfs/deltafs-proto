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
#include "pdlfs-common/port.h"
#include "pdlfs-common/strutil.h"

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#if defined(PDLFS_RADOS)
#include "pdlfs-common/rados/rados_connmgr.h"
#endif
#if defined(PDLFS_OS_LINUX)
#include <ctype.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <time.h>
#endif

namespace pdlfs {
namespace {
// Options for the db at the input end.
DBOptions FLAGS_src_dbopts;

// Compaction input.
const char* FLAGS_src_prefix = NULL;

// True iff rados env should be used.
bool FLAGS_env_use_rados = false;

// True iff rados async io (AIO) should be disabled.
bool FLAGS_rados_force_syncio = false;

#if defined(PDLFS_RADOS)
// User name for ceph rados connection.
const char* FLAGS_rados_cli_name = "client.admin";

// Rados cluster name.
const char* FLAGS_rados_cluster_name = "ceph";

// Rados storage pool name.
const char* FLAGS_rados_pool = "test";

// Rados cluster configuration file.
const char* FLAGS_rados_conf = "/tmp/ceph.conf";
#endif

// Total number of ranks.
int FLAGS_comm_size = 1;

// My rank number.
int FLAGS_rank = 0;

class Compactor {
 private:
  DB* srcdb_;
#if defined(PDLFS_RADOS)
  rados::RadosConnMgr* mgr_;
  Env* myenv_;
#endif

  static void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    fprintf(stdout, "WARNING: C++ optimization disabled\n");
#endif
#ifndef NDEBUG
    fprintf(stdout, "WARNING: C++ assertions are on\n");
#endif

    // See if snappy is working by attempting to compress a compressible string
    const char text[] = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
    std::string compressed;
    if (!port::Snappy_Compress(text, sizeof(text), &compressed)) {
      fprintf(stdout, "WARNING: Snappy compression is not enabled\n");
    } else if (compressed.size() >= sizeof(text)) {
      fprintf(stdout, "WARNING: Snappy compression is not effective\n");
    }
  }

#if defined(PDLFS_OS_LINUX)
  static Slice TrimSpace(Slice s) {
    size_t start = 0;
    while (start < s.size() && isspace(s[start])) {
      start++;
    }
    size_t limit = s.size();
    while (limit > start && isspace(s[limit - 1])) {
      limit--;
    }

    Slice r = s;
    r.remove_suffix(s.size() - limit);
    r.remove_prefix(start);
    return r;
  }
#endif

  static void PrintEnvironment() {
#if defined(PDLFS_OS_LINUX)
    time_t now = time(NULL);
    fprintf(stdout, "Date:       %s", ctime(&now));  // ctime() adds newline

    FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
    if (cpuinfo != NULL) {
      char line[1000];
      int num_cpus = 0;
      std::string cpu_type;
      std::string cache_size;
      while (fgets(line, sizeof(line), cpuinfo) != NULL) {
        const char* sep = strchr(line, ':');
        if (sep == NULL) {
          continue;
        }
        Slice key = TrimSpace(Slice(line, sep - 1 - line));
        Slice val = TrimSpace(Slice(sep + 1));
        if (key == "model name") {
          ++num_cpus;
          cpu_type = val.ToString();
        } else if (key == "cache size") {
          cache_size = val.ToString();
        }
      }
      fclose(cpuinfo);
      fprintf(stdout, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
      fprintf(stdout, "CPUCache:   %s\n", cache_size.c_str());
    }
#endif
  }

#if defined(PDLFS_RADOS)
  static void PrintRadosSettings() {
    fprintf(stdout, "Disable async io:   %d\n", FLAGS_rados_force_syncio);
    fprintf(stdout, "Cluster name:       %s\n", FLAGS_rados_cluster_name);
    fprintf(stdout, "Cli name:           %s\n", FLAGS_rados_cli_name);
    fprintf(stdout, "Storage pool name:  %s\n", FLAGS_rados_pool);
    fprintf(stdout, "Conf: %s\n", FLAGS_rados_conf);
  }
#endif

  static void PrintHeader() {
    PrintWarnings();
    PrintEnvironment();
#if defined(PDLFS_RADOS)
    fprintf(stdout, "Use rados:          %d\n", FLAGS_env_use_rados);
    if (FLAGS_env_use_rados) PrintRadosSettings();
#endif
    fprintf(stdout, "------------------------------------------------\n");
  }

  Env* OpenEnv() {
    if (FLAGS_env_use_rados) {
#if defined(PDLFS_RADOS)
      if (myenv_) {
        return myenv_;
      }
      FLAGS_src_dbopts.detach_dir_on_close = true;
      using namespace rados;
      RadosOptions options;
      options.force_syncio = FLAGS_rados_force_syncio;
      RadosConn* conn;
      Osd* osd;
      mgr_ = new RadosConnMgr(RadosConnMgrOptions());
      Status s = mgr_->OpenConn(  ///
          FLAGS_rados_cluster_name, FLAGS_rados_cli_name, FLAGS_rados_conf,
          RadosConnOptions(), &conn);
      if (!s.ok()) {
        fprintf(stderr, "%d: Cannot connect to rados: %s\n", FLAGS_rank,
                s.ToString().c_str());
        MPI_Finalize();
        exit(1);
      }
      s = mgr_->OpenOsd(conn, FLAGS_rados_pool, options, &osd);
      if (!s.ok()) {
        fprintf(stderr, "%d: Cannot open rados object pool: %s\n", FLAGS_rank,
                s.ToString().c_str());
        MPI_Finalize();
        exit(1);
      }
      myenv_ = mgr_->OpenEnv(osd, true, RadosEnvOptions());
      mgr_->Release(conn);
      return myenv_;
#else
      if (FLAGS_rank == 0) {
        fprintf(stderr, "Rados not installed\n");
      }
      MPI_Finalize();
      exit(1);
#endif
    } else {
      return Env::Default();
    }
  }

  void Open() {
    Env* const env = OpenEnv();
    DBOptions dbopts = FLAGS_src_dbopts;
    dbopts.env = env;
    char dbid[100];
    snprintf(dbid, sizeof(dbid), "/r%d", FLAGS_rank);
    std::string dbpath = FLAGS_src_prefix;
    dbpath += dbid;
    Status s = ReadonlyDB::Open(dbopts, dbpath, &srcdb_);
    if (!s.ok()) {
      fprintf(stderr, "%d: Cannot open db: %s\n", FLAGS_rank,
              s.ToString().c_str());
      MPI_Abort(MPI_COMM_WORLD, 1);
    }
  }

 public:
  Compactor() : srcdb_(NULL) {
#if defined(PDLFS_RADOS)
    mgr_ = NULL;
    myenv_ = NULL;
#endif
  }

  ~Compactor() {
    delete srcdb_;
#if defined(PDLFS_RADOS)
    delete myenv_;
    delete mgr_;
#endif
  }

  void Run() {
    Open();
    ReadOptions read_options;
    read_options.fill_cache = false;
    Iterator* const iter = srcdb_->NewIterator(read_options);
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

  pdlfs::Compactor compactor;
  compactor.Run();
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
