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
#include "fs.h"
#include "fsdb.h"
#include "fssvr.h"

#include "pdlfs-common/leveldb/db.h"
#include "pdlfs-common/leveldb/options.h"

#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/port.h"

#include <ctype.h>
#include <mpi.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

namespace pdlfs {
namespace {
FilesystemDbOptions FLAGS_dbopts;

// Total number of ranks.
int FLAGS_comm_size = 1;

// My rank number.
int FLAGS_rank = 0;

// Listening port for rank 0.
int FLAGS_port = 10086;

// Number of listening ports per rank.
int FLAGS_ports_per_rank = 1;

// Skip all fs checks.
bool FLAGS_skip_fs_checks = false;

// If true, do not destroy the existing database.
bool FLAGS_use_existing_db = false;

// Use the db at the following prefix.
const char* FLAGS_db = NULL;

class Server {
 private:
  port::Mutex mu_;
  port::AtomicPointer shutting_down_;
  port::CondVar cv_;
  FilesystemDb* fsdb_;
  Filesystem* fs_;
  FilesystemServer** svrs_;
  int n_;

  static void PrintHeader() {
    PrintEnvironment();
    PrintWarnings();
    fprintf(stdout, "Num ports per rank: %d\n", FLAGS_ports_per_rank);
    fprintf(stdout, "Port:               %d, starting from\n", FLAGS_port);
    fprintf(stdout, "Use existing db:    %d\n", FLAGS_use_existing_db);
    fprintf(stdout, "Db: %s/<rank>\n", FLAGS_db);
    fprintf(stdout, "------------------------------------------------\n");
  }

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
    fprintf(stderr, "Date:       %s", ctime(&now));  // ctime() adds newline

    FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
    if (cpuinfo != nullptr) {
      char line[1000];
      int num_cpus = 0;
      std::string cpu_type;
      std::string cache_size;
      while (fgets(line, sizeof(line), cpuinfo) != nullptr) {
        const char* sep = strchr(line, ':');
        if (sep == nullptr) {
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
      fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
      fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
#endif
  }

  FilesystemIf* OpenFilesystem() {
    fsdb_ = new FilesystemDb(FLAGS_dbopts, Env::Default());
    char dbsuffix[100];
    snprintf(dbsuffix, sizeof(dbsuffix), "/%d", FLAGS_rank);
    std::string dbpath = FLAGS_db;
    dbpath += dbsuffix;
    Status s = fsdb_->Open(dbpath);
    if (!s.ok()) {
      fprintf(stderr, "%d: cannot open db: %s\n", FLAGS_rank,
              s.ToString().c_str());
      MPI_Finalize();
      exit(1);
    }

    FilesystemOptions opts;
    opts.skip_partition_checks = opts.skip_perm_checks =
        opts.skip_lease_due_checks = opts.skip_name_collision_checks =
            FLAGS_skip_fs_checks;
    fs_ = new Filesystem(opts);
    fs_->SetDb(fsdb_);
    return fs_;
  }

  FilesystemServer* OpenPort(int port, FilesystemIf* const fs) {
    FilesystemServerOptions srvopts;
    srvopts.num_rpc_threads = 1;
    char uri[10];
    snprintf(uri, sizeof(uri), ":%d", port);
    srvopts.uri = uri;
    FilesystemServer* const rpcsrv = new FilesystemServer(srvopts);
    rpcsrv->SetFs(fs);
    Status s = rpcsrv->OpenServer();
    if (!s.ok()) {
      fprintf(stderr, "%d: cannot open rpc: %s\n", FLAGS_rank,
              s.ToString().c_str());
      MPI_Finalize();
      exit(1);
    }
    return rpcsrv;
  }

 public:
  Server()
      : shutting_down_(NULL),
        cv_(&mu_),
        fsdb_(NULL),
        fs_(NULL),
        svrs_(NULL),
        n_(0) {
    if (!FLAGS_use_existing_db) {
      DestroyDB(FLAGS_db, DBOptions());
    }
  }

  ~Server() {
    for (int i = 0; i < n_; i++) {
      delete svrs_[i];
    }
    delete[] svrs_;
    delete fs_;
    delete fsdb_;
  }

  void Interrupt() {
    MutexLock ml(&mu_);
    shutting_down_.Release_Store(this);
    cv_.Signal();
  }

  void Run() {
    if (FLAGS_rank == 0) {
      PrintHeader();
    }
    FilesystemIf* const fs = OpenFilesystem();
    svrs_ = new FilesystemServer*[FLAGS_ports_per_rank];
    int base_port = FLAGS_port + FLAGS_rank * FLAGS_ports_per_rank;
    for (int i = 0; i < FLAGS_ports_per_rank; i++) {
      svrs_[i] = OpenPort(base_port + i, fs);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    if (FLAGS_rank == 0) {
      puts("Running...");
    }
    MutexLock ml(&mu_);
    while (!shutting_down_.Acquire_Load()) {
      cv_.Wait();
    }
    MPI_Barrier(MPI_COMM_WORLD);
    if (FLAGS_rank == 0) {
      puts("Bye!");
    }
  }
};

}  // namespace
}  // namespace pdlfs

namespace {
pdlfs::Server* g_srvr = NULL;

void HandleSig(const int sig) {
  fprintf(stdout, "\n");
  if (sig == SIGINT || sig == SIGTERM) {
    if (g_srvr) {
      g_srvr->Interrupt();
    }
  }
}

void Doit(int* const argc, char*** const argv) {
  pdlfs::FLAGS_dbopts.ReadFromEnv();

  for (int i = 2; i < *argc; i++) {
    int n;
    char junk;
    if (sscanf((*argv)[i], "--port=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_port = n;
    } else if (sscanf((*argv)[i], "--ports_per_rank=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_ports_per_rank = n;
    } else if (sscanf((*argv)[i], "--skip_fs_checks=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_skip_fs_checks = n;
    } else if (sscanf((*argv)[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_use_existing_db = n;
    } else if (strncmp((*argv)[i], "--db=", 5) == 0) {
      pdlfs::FLAGS_db = (*argv)[i] + 5;
    } else {
      if (pdlfs::FLAGS_rank == 0) {
        fprintf(stderr, "Invalid flag: '%s'\n", (*argv)[i]);
      }
      MPI_Finalize();
      exit(1);
    }
  }

  std::string default_db_prefix;
  // Choose a location for the test database if none given with --db=<path>
  if (!pdlfs::FLAGS_db) {
    default_db_prefix = "/tmp/deltafs_srvr";
    pdlfs::FLAGS_db = default_db_prefix.c_str();
  }

  pdlfs::Server srvr;
  g_srvr = &srvr;
  signal(SIGINT, &HandleSig);
  srvr.Run();
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
  Doit(&argc, &argv);
  MPI_Finalize();
  return 0;
}
