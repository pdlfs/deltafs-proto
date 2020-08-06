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
#include "fscli.h"

#include "base64enc.h"
#include "fs.h"
#include "fscom.h"
#include "fsdb.h"

#include "pdlfs-common/leveldb/db.h"
#include "pdlfs-common/leveldb/options.h"

#include "pdlfs-common/histogram.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/testharness.h"

#include <algorithm>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <vector>
#if defined(PDLFS_OS_LINUX)
#include <ctype.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <time.h>
#endif

namespace pdlfs {

class FilesystemCliTest {
 public:
  typedef FilesystemCli::BAT BATCH;
  typedef FilesystemCli::AT AT;
  FilesystemCliTest()
      : fsdb_(NULL),
        fs_(NULL),
        fscli_(NULL),
        myctx_(301),
        fsloc_(test::TmpDir() + "/fscli_test") {
    DestroyDB(fsloc_, DBOptions());
    me_.gid = me_.uid = 1;
    myctx_.who = me_;
  }

  Status OpenFilesystemCli() {
    fsdb_ = new FilesystemDb(fsdbopts_, Env::GetUnBufferedIoEnv());
    Status s = fsdb_->Open(fsloc_);
    if (s.ok()) {
      fscli_ = new FilesystemCli(fscliopts_);
      fs_ = new Filesystem(fsopts_);
      fscli_->SetLocalFs(fs_);
      fs_->SetDb(fsdb_);
    }
    return s;
  }

  ~FilesystemCliTest() {
    delete fscli_;
    delete fs_;
    delete fsdb_;
  }

  Status Atdir(const char* path, AT** result, const AT* at = NULL) {
    return fscli_->Atdir(&myctx_, at, path, result);
  }

  Status Creat(const char* path, const AT* at = NULL) {
    return fscli_->Mkfle(&myctx_, at, path, 0660, &tmp_);
  }

  Status Mkdir(const char* path, const AT* at = NULL) {
    return fscli_->Mkdir(&myctx_, at, path, 0770, &tmp_);
  }

  Status Exist(const char* path, const AT* at = NULL) {
    return fscli_->Lstat(&myctx_, at, path, &tmp_);
  }

  Status BatchStart(const char* path, BATCH** result, const AT* at = NULL) {
    return fscli_->BatchInit(&myctx_, at, path, result);
  }

  Status BatchInsert(const char* name, BATCH* batch) {
    return fscli_->BatchInsert(batch, name);
  }

  Status BatchCommit(BATCH* batch) {  ///
    return fscli_->BatchCommit(batch);
  }

  Status BatchEnd(BATCH* batch) {  ///
    return fscli_->BatchEnd(batch);
  }

  Stat tmp_;
  FilesystemDbOptions fsdbopts_;
  FilesystemDb* fsdb_;
  FilesystemOptions fsopts_;
  Filesystem* fs_;
  FilesystemCliOptions fscliopts_;
  FilesystemCli* fscli_;
  FilesystemCliCtx myctx_;
  std::string fsloc_;
  User me_;
};

TEST(FilesystemCliTest, OpenAndClose) {
  ASSERT_OK(OpenFilesystemCli());
  ASSERT_OK(fscli_->TEST_ProbeDir(DirId(0)));
  ASSERT_EQ(fscli_->TEST_TotalDirsInMemory(), 0);
  ASSERT_OK(fscli_->TEST_ProbePartition(DirId(0), 0));
  ASSERT_EQ(fscli_->TEST_TotalPartitionsInMemory(), 1);
  ASSERT_EQ(fscli_->TEST_TotalDirsInMemory(), 1);
}

TEST(FilesystemCliTest, Files) {
  ASSERT_OK(OpenFilesystemCli());
  ASSERT_OK(Creat("/1"));
  ASSERT_CONFLICT(Creat("/1"));
  ASSERT_OK(Exist("/1"));
  ASSERT_OK(Exist("//1"));
  ASSERT_OK(Exist("///1"));
  ASSERT_ERR(Exist("/1/"));
  ASSERT_ERR(Exist("//1//"));
  ASSERT_NOTFOUND(Exist("/2"));
  ASSERT_OK(Creat("/2"));
}

TEST(FilesystemCliTest, Dirs) {
  ASSERT_OK(OpenFilesystemCli());
  ASSERT_OK(Exist("/"));
  ASSERT_OK(Exist("//"));
  ASSERT_OK(Exist("///"));
  ASSERT_OK(Mkdir("/1"));
  ASSERT_CONFLICT(Mkdir("/1"));
  ASSERT_CONFLICT(Creat("/1"));
  ASSERT_OK(Exist("/1"));
  ASSERT_OK(Exist("/1/"));
  ASSERT_OK(Exist("//1"));
  ASSERT_OK(Exist("//1//"));
  ASSERT_OK(Exist("///1"));
  ASSERT_OK(Exist("///1///"));
  ASSERT_NOTFOUND(Exist("/2"));
  ASSERT_OK(Mkdir("/2"));
}

TEST(FilesystemCliTest, Subdirs) {
  ASSERT_OK(OpenFilesystemCli());
  ASSERT_OK(Mkdir("/1"));
  ASSERT_OK(Mkdir("/1/a"));
  ASSERT_CONFLICT(Mkdir("/1/a"));
  ASSERT_CONFLICT(Creat("/1/a"));
  ASSERT_OK(Exist("/1/a"));
  ASSERT_OK(Exist("/1/a/"));
  ASSERT_OK(Exist("//1//a"));
  ASSERT_OK(Exist("//1//a//"));
  ASSERT_OK(Exist("///1///a"));
  ASSERT_OK(Exist("///1///a///"));
  ASSERT_NOTFOUND(Exist("/1/b"));
  ASSERT_OK(Mkdir("/1/b"));
}

TEST(FilesystemCliTest, Resolv) {
  ASSERT_OK(OpenFilesystemCli());
  ASSERT_OK(Mkdir("/1"));
  ASSERT_OK(Mkdir("/1/2"));
  ASSERT_OK(Mkdir("/1/2/3"));
  ASSERT_OK(Mkdir("/1/2/3/4"));
  ASSERT_OK(Mkdir("/1/2/3/4/5"));
  ASSERT_OK(Creat("/1/2/3/4/5/6"));
  ASSERT_OK(Exist("/1"));
  ASSERT_OK(Exist("/1/2"));
  ASSERT_OK(Exist("/1/2/3"));
  ASSERT_OK(Exist("/1/2/3/4"));
  ASSERT_OK(Exist("/1/2/3/4/5"));
  ASSERT_ERR(Exist("/1/2/3/4/5/6/"));
  ASSERT_ERR(Exist("/2/3"));
  ASSERT_ERR(Exist("/1/2/4/5"));
  ASSERT_ERR(Exist("/1/2/3/5"));
  ASSERT_ERR(Creat("/1/2/3/4/5/6/7"));
}

TEST(FilesystemCliTest, Atdir) {
  ASSERT_OK(OpenFilesystemCli());
  AT *d0, *d1, *d2, *d3, *d4, *d5;
  ASSERT_OK(Atdir("//", &d0));
  ASSERT_OK(Mkdir("/1", d0));
  ASSERT_OK(Exist("/1", d0));
  ASSERT_OK(Atdir("/1", &d1, d0));
  ASSERT_OK(Mkdir("/2", d1));
  ASSERT_OK(Exist("/2", d1));
  ASSERT_OK(Atdir("/2", &d2, d1));
  ASSERT_OK(Mkdir("/3", d2));
  ASSERT_OK(Exist("/3", d2));
  ASSERT_OK(Atdir("/3", &d3, d2));
  ASSERT_OK(Mkdir("/4", d3));
  ASSERT_OK(Exist("/4", d3));
  ASSERT_OK(Atdir("/4", &d4, d3));
  ASSERT_OK(Mkdir("/5", d4));
  ASSERT_OK(Exist("/5", d4));
  ASSERT_OK(Atdir("/5", &d5, d4));
  ASSERT_OK(Creat("/a", d5));
  ASSERT_OK(Creat("/b", d5));
  ASSERT_OK(Creat("/c", d5));
  ASSERT_OK(Creat("/d", d5));
  ASSERT_OK(Creat("/e", d5));
  ASSERT_OK(Creat("/f", d5));
  ASSERT_OK(Exist("/1/2/3/4/5/a", d0));
  ASSERT_OK(Exist("/2/3/4/5/b", d1));
  ASSERT_OK(Exist("/3/4/5/c", d2));
  ASSERT_OK(Exist("/4/5/d", d3));
  ASSERT_OK(Exist("/5/e", d4));
  ASSERT_OK(Exist("/f", d5));
  fscli_->Destroy(d5);
  fscli_->Destroy(d4);
  fscli_->Destroy(d3);
  fscli_->Destroy(d2);
  fscli_->Destroy(d1);
}

TEST(FilesystemCliTest, BatchCtx) {
  ASSERT_OK(OpenFilesystemCli());
  BATCH *bat, *bat1, *bat2;
  ASSERT_OK(Mkdir("/a"));
  ASSERT_OK(BatchStart("/a", &bat));
  ASSERT_EQ(fscli_->TEST_TotalLeasesAtPartition(DirId(0), 0), 1);
  // Directory locked for batch creates
  ASSERT_ERR(Mkdir("/a/1"));
  ASSERT_ERR(Exist("/a/2"));
  ASSERT_ERR(Creat("/a/3"));
  ASSERT_OK(BatchEnd(bat));
  ASSERT_EQ(fscli_->TEST_TotalLeasesAtPartition(DirId(0), 0), 0);
  ASSERT_OK(Mkdir("/b"));
  ASSERT_OK(BatchStart("/b", &bat1));
  ASSERT_OK(BatchStart("/b", &bat2));
  ASSERT_OK(BatchEnd(bat1));
  ASSERT_EQ(fscli_->TEST_TotalLeasesAtPartition(DirId(0), 0), 1);
  ASSERT_OK(BatchEnd(bat2));
  ASSERT_EQ(fscli_->TEST_TotalLeasesAtPartition(DirId(0), 0), 0);
  ASSERT_NOTFOUND(BatchStart("/c", &bat));  // Target directory not found
  ASSERT_OK(Mkdir("/c"));
  ASSERT_OK(Creat("/c/1"));
  // Directory locked for regular operations
  ASSERT_ERR(BatchStart("/c", &bat));
}

TEST(FilesystemCliTest, BatchCreats) {
  ASSERT_OK(OpenFilesystemCli());
  BATCH* bat;
  ASSERT_OK(Mkdir("/a"));
  ASSERT_OK(BatchStart("/a", &bat));
  ASSERT_OK(BatchInsert("1", bat));
  ASSERT_ERR(Exist("/a/1"));
  ASSERT_OK(BatchInsert("2", bat));
  ASSERT_ERR(Exist("/a/2"));
  ASSERT_OK(BatchInsert("3", bat));
  ASSERT_ERR(Exist("/a/3"));
  ASSERT_OK(BatchCommit(bat));
  ASSERT_ERR(BatchInsert("4", bat));
  ASSERT_OK(BatchCommit(bat));
  ASSERT_OK(BatchEnd(bat));
  ASSERT_OK(Exist("/a/1"));
  ASSERT_OK(Exist("/a/2"));
  ASSERT_OK(Exist("/a/3"));
}

namespace {  // Filesystem rpc performance bench (the client part of it)...
// Number of threads to run.
int FLAGS_threads = 1;

// Number of rpc operations to perform per thread.
int FLAGS_num = 8;

// Print histogram of timings.
bool FLAGS_histogram = false;

// Generate file names in random order.
bool FLAGS_random_order = false;

// User id for the bench.
int FLAGS_uid = 1;

// Group id.
int FLAGS_gid = 1;

// Comma-separated server locations.
const char* FLAGS_svr_uris = NULL;

// Performance stats.
class Stats {
 private:
#if defined(PDLFS_OS_LINUX)
  struct rusage start_rusage_;
  struct rusage rusage_;
#endif
  double start_;
  double finish_;
  double seconds_;
  int done_;
  int next_report_;
  double last_op_finish_;
  Histogram hist_;

#if defined(PDLFS_OS_LINUX)
  static void MergeTimeval(struct timeval* tv, const struct timeval* other) {
    tv->tv_sec += other->tv_sec;
    tv->tv_usec += other->tv_usec;
  }

  static void MergeU(struct rusage* ru, const struct rusage* other) {
    MergeTimeval(&ru->ru_utime, &other->ru_utime);
    MergeTimeval(&ru->ru_stime, &other->ru_stime);
  }

  static uint64_t TimevalToMicros(const struct timeval* tv) {
    uint64_t t;
    t = static_cast<uint64_t>(tv->tv_sec) * 1000000;
    t += tv->tv_usec;
    return t;
  }
#endif

 public:
  Stats() { Start(); }

  void Start() {
    next_report_ = 100;
    last_op_finish_ = start_;
    hist_.Clear();
    done_ = 0;
    seconds_ = 0;
    start_ = CurrentMicros();
    finish_ = start_;
#if defined(PDLFS_OS_LINUX)
    getrusage(RUSAGE_THREAD, &start_rusage_);
#endif
  }

  void Merge(const Stats& other) {
#if defined(PDLFS_OS_LINUX)
    MergeU(&start_rusage_, &other.start_rusage_);
    MergeU(&rusage_, &other.rusage_);
#endif
    hist_.Merge(other.hist_);
    done_ += other.done_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;
  }

  void Stop() {
#if defined(PDLFS_OS_LINUX)
    getrusage(RUSAGE_THREAD, &rusage_);
#endif
    finish_ = CurrentMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void FinishedSingleOp(int total, int tid) {
    if (FLAGS_histogram) {
      double now = CurrentMicros();
      double micros = now - last_op_finish_;
      hist_.Add(micros);
      if (micros > 20000) {
        fprintf(stderr, "Long op: %.1f micros%30s\r", micros, "");
        fflush(stderr);
      }
      last_op_finish_ = now;
    }

    done_++;
    if (tid == 0 && done_ >= next_report_) {
      if (next_report_ < 1000)
        next_report_ += 100;
      else if (next_report_ < 5000)
        next_report_ += 500;
      else if (next_report_ < 10000)
        next_report_ += 1000;
      else if (next_report_ < 50000)
        next_report_ += 5000;
      else if (next_report_ < 100000)
        next_report_ += 10000;
      else if (next_report_ < 500000)
        next_report_ += 50000;
      else
        next_report_ += 100000;
      fprintf(stderr, "... finished %d ops (%.0f%%)%30s\r", done_,
              100.0 * done_ / total, "");
      fflush(stderr);
    }
  }

  void Report(const Slice& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    if (done_ < 1) done_ = 1;

    char rate[100];
    // Rate is computed on actual elapsed time, not the sum of per-thread
    // elapsed times.
    double elapsed = (finish_ - start_) * 1e-6;
    snprintf(rate, sizeof(rate), "%9.3f Kop/s, %9d ops",
             done_ / 1000.0 / elapsed, done_);
    // Per-op latency is computed on the sum of per-thread elapsed times, not
    // the actual elapsed time.
    fprintf(stdout, "==%-12s : %9.3f micros/op, %s\n", name.ToString().c_str(),
            seconds_ * 1e6 / done_, rate);
#if defined(PDLFS_OS_LINUX)
    fprintf(stdout, "Time(usr/sys/wall): %.3f/%.3f/%.3f\n",
            (TimevalToMicros(&rusage_.ru_utime) -
             TimevalToMicros(&start_rusage_.ru_utime)) *
                1e-6,
            (TimevalToMicros(&rusage_.ru_stime) -
             TimevalToMicros(&start_rusage_.ru_stime)) *
                1e-6,
            (finish_ - start_) * 1e-6);
#endif
    if (FLAGS_histogram) {
      fprintf(stdout, "Microseconds per op:\n%s\n", hist_.ToString().c_str());
    }
    fflush(stdout);
  }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
  port::Mutex mu;
  port::CondVar cv;
  int total;  // Total number of threads
  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done
  int num_initialized;
  int num_done;
  bool start;
  LookupStat parent_lstat;
  User me;

  SharedState(int total)
      : cv(&mu), total(total), num_initialized(0), num_done(0), start(false) {
    parent_lstat.SetDnodeNo(0);
    parent_lstat.SetInodeNo(0);
    parent_lstat.SetDirMode(0770 | S_IFDIR);
    parent_lstat.SetUserId(FLAGS_uid);
    parent_lstat.SetGroupId(FLAGS_gid);
    parent_lstat.SetZerothServer(0);
    parent_lstat.SetLeaseDue(-1);
    parent_lstat.AssertAllSet();
    me.uid = FLAGS_uid;
    me.gid = FLAGS_gid;
  }
};

// A wrapper over our own random object.
struct STLRand {
  STLRand(int seed) : rnd(seed) {}
  int operator()(int i) { return rnd.Next() % i; }
  Random rnd;
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  int tid;  // 0..n-1 when running in n threads
  std::vector<uint32_t> fids;
  SharedState* shared;
  Stats stats;

  ThreadState(int tid) : tid(tid), shared(NULL) {
    fids.reserve(FLAGS_num);
    for (int i = 0; i < FLAGS_num; i++) {
      fids.push_back(i);
    }
    if (FLAGS_random_order) {
      std::random_shuffle(fids.begin(), fids.end(), STLRand(1000 * tid));
    }
  }
};

class Benchmark {
 private:
  std::vector<std::string> svr_uris_;
  RPC* rpc_;

  static void PrintHeader() {
    PrintEnvironment();
    PrintWarnings();
    fprintf(stdout, "Threads:            %d\n", FLAGS_threads);
    fprintf(stdout, "Num sends:          %d per thread\n", FLAGS_num);
    fprintf(stdout, "Random key order:   %d\n", FLAGS_random_order);
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

  struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;
    ThreadState* thread;
  };

  static void ThreadBody(void* v) {
    ThreadArg* const arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
    {
      MutexLock l(&shared->mu);
      shared->num_initialized++;
      if (shared->num_initialized >= shared->total) {
        shared->cv.SignalAll();
      }
      while (!shared->start) {
        shared->cv.Wait();
      }
    }

    thread->stats.Start();
    arg->bm->SendAndReceive(thread);
    thread->stats.Stop();

    {
      MutexLock l(&shared->mu);
      shared->num_done++;
      if (shared->num_done >= shared->total) {
        shared->cv.SignalAll();
      }
    }
  }

  void RunBenchmark(int n) {
    SharedState shared(n);

    ThreadArg* const arg = new ThreadArg[n];
    for (int i = 0; i < n; i++) {
      arg[i].bm = this;
      arg[i].shared = &shared;
      arg[i].thread = new ThreadState(i);
      arg[i].thread->shared = &shared;
      Env::Default()->StartThread(ThreadBody, &arg[i]);
    }

    {
      MutexLock ml(&shared.mu);
      while (shared.num_initialized < n) {
        shared.cv.Wait();
      }

      shared.start = true;
      shared.cv.SignalAll();
      while (shared.num_done < n) {
        shared.cv.Wait();
      }
    }

    for (int i = 1; i < n; i++) {
      arg[0].thread->stats.Merge(arg[i].thread->stats);
    }
    arg[0].thread->stats.Report("send/recv");
    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }
    delete[] arg;
  }

  void SendAndReceive(ThreadState* const thread) {
    SharedState* shared = thread->shared;
    Stats* stats = &thread->stats;
    const uint64_t tid = uint64_t(thread->tid) << 32;
    Random rnd(1000 + thread->tid);
    MkfleOptions options;
    options.parent = &shared->parent_lstat;
    options.mode = 0660;
    options.me = shared->me;
    Stat stat;
    MkfleRet ret;
    ret.stat = &stat;
    const int n = svr_uris_.size();
    if (n == 0) {
      return;
    }
    rpc::If** const clis = new rpc::If*[n];
    for (int i = 0; i < n; i++) {
      clis[i] = rpc_->OpenStubFor(svr_uris_[i]);
    }
    char tmp[30];
    for (int i = 0; i < FLAGS_num; i++) {
      options.name = Base64Enc(tmp, tid | thread->fids[i]);
      Status s =
          rpc::MkfleCli(clis[(n == 1) ? 0 : rnd.Next() % n])(options, &ret);
      if (!s.ok()) {
        fprintf(stderr, "Cannot send/recv: %s\n", s.ToString().c_str());
        exit(1);
      }
      stats->FinishedSingleOp(FLAGS_num, thread->tid);
    }
    for (int i = 0; i < n; i++) {
      delete clis[i];
    }
    delete[] clis;
  }

 public:
  Benchmark() : rpc_(NULL) {}

  ~Benchmark() {
    if (rpc_) {
      rpc_->Stop();
    }
    delete rpc_;
  }

  void Run() {
    PrintHeader();
    RPCOptions opts;
    opts.uri = "udp://-1:-1";
    opts.mode = rpc::kClientOnly;
    rpc_ = RPC::Open(opts);
    Status s = rpc_->status();
    if (!s.ok()) {
      fprintf(stderr, "Cannot open rpc: %s\n", s.ToString().c_str());
      exit(1);
    }
    const char* p = FLAGS_svr_uris;
    while (p != NULL) {
      const char* sep = strchr(p, ',');
      Slice uri;
      if (sep == NULL) {
        uri = p;
        p = NULL;
      } else {
        uri = Slice(p, sep - p);
        p = sep + 1;
      }
      svr_uris_.push_back(uri.ToString());
      fprintf(stdout, "Add svr uri: '%s'\n", svr_uris_.back().c_str());
    }
    fflush(stdout);

    RunBenchmark(FLAGS_threads);
  }
};
}  // namespace
}  // namespace pdlfs

namespace {
void BM_Main(const int* const argc, char*** const argv) {
  pdlfs::FLAGS_random_order = true;
  std::string default_uri;

  for (int i = 2; i < *argc; i++) {
    int n;
    char junk;
    if (strncmp((*argv)[i], "--uris=", 7) == 0) {
      pdlfs::FLAGS_svr_uris = (*argv)[i] + 7;
    } else if (sscanf((*argv)[i], "--random_order=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_random_order = n;
    } else if (sscanf((*argv)[i], "--histogram=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_histogram = n;
    } else if (sscanf((*argv)[i], "--threads=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_threads = n;
    } else if (sscanf((*argv)[i], "--num=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_num = n;
    } else {
      fprintf(stderr, "Invalid flag: '%s'\n", (*argv)[i]);
      exit(1);
    }
  }

  if (!pdlfs::FLAGS_svr_uris) {
    default_uri = "udp://127.0.0.1:10086";
    pdlfs::FLAGS_svr_uris = default_uri.c_str();
  }

  pdlfs::Benchmark benchmark;
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
