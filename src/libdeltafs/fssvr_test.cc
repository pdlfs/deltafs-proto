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
#include "fssvr.h"

#include "fs.h"
#include "fsdb.h"

#include "pdlfs-common/leveldb/db.h"
#include "pdlfs-common/leveldb/options.h"

#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/testharness.h"

#include <ctype.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#if __cplusplus >= 201103L
#define OVERRIDE override
#else
#define OVERRIDE
#endif
namespace pdlfs {
class FilesystemServerTest {
 public:
  FilesystemServerTest()  ///
      : srv_(new FilesystemServer(options_)) {}
  virtual ~FilesystemServerTest() {  ///
    delete srv_;
  }

  FilesystemServerOptions options_;
  FilesystemServer* srv_;
};

Status TEST_Handler(FilesystemIf*, rpc::If::Message& in,
                    rpc::If::Message& out) {
  EncodeFixed32(&out.buf[0], DecodeFixed32(&in.contents[4]));
  out.contents = Slice(&out.buf[0], 4);
  return Status::OK();
}

TEST(FilesystemServerTest, StartAndStop) {
  ASSERT_OK(srv_->OpenServer());
  ASSERT_OK(srv_->Close());
}

TEST(FilesystemServerTest, OpRoute) {
  ASSERT_OK(srv_->OpenServer());
  srv_->TEST_Remap(0, TEST_Handler);
  rpc::If* cli = srv_->TEST_CreateCli("127.0.0.1" + options_.uri);
  rpc::If::Message in, out;
  EncodeFixed32(&in.buf[0], 0);
  EncodeFixed32(&in.buf[4], 12345);
  in.contents = Slice(&in.buf[0], 8);
  ASSERT_OK(cli->Call(in, out));
  ASSERT_EQ(out.contents.size(), 4);
  ASSERT_EQ(DecodeFixed32(&out.contents[0]), 12345);
  ASSERT_OK(srv_->Close());
}

namespace {  // RPC performance bench (the srvr part of it)...
// Number of rpc processing threads to launch.
int FLAGS_threads = 1;

// Use a real fs server backed by a db.
bool FLAGS_with_db = false;

// If true, do not destroy the existing database.
bool FLAGS_use_existing_db = false;

// Use the db at the following name.
const char* FLAGS_db = NULL;

// Start the server at the following address.
const char* FLAGS_srv_uri = NULL;

class Benchmark : public FilesystemWrapper {
 private:
  port::Mutex mu_;
  bool shutting_down_;
  port::CondVar cv_;
  FilesystemServer* fsrpcsrv_;
  Filesystem* fs_;
  FilesystemDb* db_;

  static void PrintHeader() {
    PrintEnvironment();
    PrintWarnings();
    fprintf(stdout, "Threads:            %d\n", FLAGS_threads);
    fprintf(stdout, "Uri:                %s\n", FLAGS_srv_uri);
    fprintf(stdout, "Real fs w/ db:      %d\n", FLAGS_with_db);
    fprintf(stdout, "Use existing db:    %d\n", FLAGS_use_existing_db);
    fprintf(stdout, "Db: %s\n", FLAGS_db);
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

  virtual Status Mkfle(  ///
      const User& who, const LookupStat& parent, const Slice& name,
      uint32_t mode, Stat* stat) OVERRIDE {
    return Status::OK();
  }

  FilesystemIf* OpenFilesystem() {
    FilesystemDbOptions dbopts;
    dbopts.enable_io_monitoring = false;
    db_ = new FilesystemDb(dbopts, Env::Default());
    Status s = db_->Open(FLAGS_db);
    if (!s.ok()) {
      fprintf(stderr, "Cannot open db: %s\n", s.ToString().c_str());
      exit(1);
    }

    FilesystemOptions opts;
    opts.skip_partition_checks = opts.skip_perm_checks =
        opts.skip_lease_due_checks = opts.skip_name_collision_checks = false;
    fs_ = new Filesystem(opts);
    fs_->SetDb(db_);
    return fs_;
  }

  void Open() {
    FilesystemServerOptions srvopts;
    srvopts.num_rpc_threads = FLAGS_threads;
    srvopts.uri = FLAGS_srv_uri;
    fsrpcsrv_ = new FilesystemServer(srvopts);
    FilesystemIf* const fs = !FLAGS_with_db ? this : OpenFilesystem();
    fsrpcsrv_->SetFs(fs);
    Status s = fsrpcsrv_->OpenServer();
    if (!s.ok()) {
      fprintf(stderr, "Cannot open rpc: %s\n", s.ToString().c_str());
      exit(1);
    }
  }

 public:
  Benchmark()
      : shutting_down_(false),
        cv_(&mu_),
        fsrpcsrv_(NULL),
        fs_(NULL),
        db_(NULL) {
    if (FLAGS_with_db && !FLAGS_use_existing_db) {
      DestroyDB(FLAGS_db, DBOptions());
    }
  }

  ~Benchmark() {
    delete fsrpcsrv_;
    delete fs_;
    delete db_;
  }

  void Interrupt() {
    MutexLock ml(&mu_);
    shutting_down_ = true;
    cv_.Signal();
  }

  void Run() {
    PrintHeader();
    Open();
    puts("Running...");
    MutexLock ml(&mu_);
    while (!shutting_down_) {
      cv_.Wait();
    }
    puts("Bye!");
  }
};
}  // namespace
}  // namespace pdlfs
#undef OVERRIDE

namespace {
pdlfs::Benchmark* g_bench = NULL;

void HandleSig(const int sig) {
  fprintf(stdout, "\n");
  if (sig == SIGINT || sig == SIGTERM) {
    if (g_bench) {
      g_bench->Interrupt();
    }
  }
}

void BM_Main(int* const argc, char*** const argv) {
  std::string default_db_path;
  std::string default_uri;

  for (int i = 2; i < *argc; i++) {
    int n;
    char junk;
    if (sscanf((*argv)[i], "--with_db=%d%c", &n, &junk) == 1 &&
        (n == 0 || n == 1)) {
      pdlfs::FLAGS_with_db = n;
    } else if (sscanf((*argv)[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_use_existing_db = n;
    } else if (sscanf((*argv)[i], "--threads=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_threads = n;
    } else if (strncmp((*argv)[i], "--uri=", 6) == 0) {
      pdlfs::FLAGS_srv_uri = (*argv)[i] + 6;
    } else if (strncmp((*argv)[i], "--db=", 5) == 0) {
      pdlfs::FLAGS_db = (*argv)[i] + 5;
    } else {
      fprintf(stderr, "Invalid flag: \"%s\"\n", (*argv)[i]);
      exit(1);
    }
  }

  // Choose a location for the test database if none given with --db=<path>
  if (!pdlfs::FLAGS_db) {
    default_db_path = pdlfs::test::TmpDir() + "/fsrpcsrv_bench";
    pdlfs::FLAGS_db = default_db_path.c_str();
  }
  if (!pdlfs::FLAGS_srv_uri) {
    default_uri = ":10086";
    pdlfs::FLAGS_srv_uri = default_uri.c_str();
  }

  pdlfs::Benchmark benchmark;
  g_bench = &benchmark;
  signal(SIGINT, &HandleSig);
  benchmark.Run();
}
}  // namespace

int main(int argc, char* argv[]) {
  pdlfs::Slice token;
  if (argc > 1) {
    token = pdlfs::Slice(argv[1]);
  }
  if (!token.starts_with("--bench")) {
    return pdlfs::test::RunAllTests(&argc, &argv);
  } else {
    BM_Main(&argc, &argv);
    return 0;
  }
}
