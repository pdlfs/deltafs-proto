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

#include "env_wrapper.h"
#include "fs.h"

#include "pdlfs-common/leveldb/db.h"
#include "pdlfs-common/leveldb/options.h"

#include "pdlfs-common/histogram.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/testharness.h"

#include <algorithm>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <vector>

namespace pdlfs {

class FilesystemDbTest {
 public:
  FilesystemDbTest() : dbloc_(test::TmpDir() + "/fsdb_test") {
    DestroyDB(dbloc_, DBOptions());
    db_ = NULL;
  }

  Status OpenDb() {
    db_ = new FilesystemDb(options_);
    return db_->Open(dbloc_);
  }

  ~FilesystemDbTest() {  ///
    delete db_;
  }

  std::string dbloc_;
  FilesystemDbOptions options_;
  FilesystemDb* db_;
};

TEST(FilesystemDbTest, OpenAndClose) {  ///
  ASSERT_OK(OpenDb());
}

namespace {  // Db benchmark
// Comma-separated list of operations to run in the specified order.
const char* FLAGS_benchmarks =
    "fillrandom,"
    "compact,"
    "readrandom,"
    "readrandom,";

// Number of concurrent threads to run.
int FLAGS_threads = 1;

// Number of KV pairs to insert per thread.
int FLAGS_num = 8;

// Number of KV pairs to read from db.
int FLAGS_reads = -1;

// Fire ops through the Filesystem interface atop FilesystemDb.
bool FLAGS_withfs = false;

// Simply print operations to be sent to db.
bool FLAGS_dryrun = false;

// Print histogram of op timings.
bool FLAGS_histogram = false;

// Number of bytes to use as a cache of uncompressed data.
// Initialized to default by main().
int FLAGS_cache_size = -1;

// Maximum number of files to keep open at the same time.
// Initialized to default by main().
int FLAGS_max_open_files = -1;

// Bloom filter bits per key. Negative means use default settings.
// Initialized to default by main().
int FLAGS_bloom_bits = -1;

// Number of keys between restart points for delta encoding of keys.
int FLAGS_block_restart_interval = -1;

// Enable snappy compression.
bool FLAGS_snappy = false;

// All files are inserted into a single parent directory.
bool FLAGS_shared_dir = false;

// Disable all background compaction.
bool FLAGS_disable_compaction = false;

// If true, do not destroy the existing database.
bool FLAGS_use_existing_db = false;

// Use the db at the following name.
const char* FLAGS_db = NULL;

// Transform a 64-bit (8-byte) integer into a 12-byte filename.
const char base64_table[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-=";
Slice Base64Encoding(char* const dst, uint64_t input) {
  const unsigned char* in = reinterpret_cast<unsigned char*>(&input);
  char* p = dst;
  *p++ = base64_table[in[0] >> 2];
  *p++ = base64_table[((in[0] & 0x03) << 4) | (in[1] >> 4)];
  *p++ = base64_table[((in[1] & 0x0f) << 2) | (in[2] >> 6)];
  *p++ = base64_table[in[2] & 0x3f];

  *p++ = base64_table[in[3] >> 2];
  *p++ = base64_table[((in[3] & 0x03) << 4) | (in[4] >> 4)];
  *p++ = base64_table[((in[4] & 0x0f) << 2) | (in[5] >> 6)];
  *p++ = base64_table[in[5] & 0x3f];

  *p++ = base64_table[in[6] >> 2];
  *p++ = base64_table[((in[6] & 0x03) << 4) | (in[7] >> 4)];
  *p++ = base64_table[(in[7] & 0x0f) << 2];
  *p++ = '+';
  assert(p - dst == 12);
  return Slice(dst, p - dst);
}

#if defined(PDLFS_OS_LINUX)
Slice TrimSpace(Slice s) {
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
void AppendWithSpace(std::string* str, Slice msg) {
  if (msg.empty()) return;
  if (!str->empty()) {
    str->push_back(' ');
  }
  str->append(msg.data(), msg.size());
}

// Performance stats.
class Stats {
 private:
  double start_;
  double finish_;
  double seconds_;
  int done_;
  int next_report_;
  int64_t bytes_;
  double last_op_finish_;
  Histogram hist_;
  std::string message_;

 public:
  Stats() { Start(); }

  void Start() {
    next_report_ = 100;
    last_op_finish_ = start_;
    hist_.Clear();
    done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = CurrentMicros();
    finish_ = start_;
    message_.clear();
  }

  void Merge(const Stats& other) {
    hist_.Merge(other.hist_);
    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Stop() {
    finish_ = CurrentMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void AddMessage(Slice msg) { AppendWithSpace(&message_, msg); }

  void FinishedSingleOp(int total) {
    if (FLAGS_histogram) {
      double now = CurrentMicros();
      double micros = now - last_op_finish_;
      hist_.Add(micros);
      if (micros > 20000) {
        fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
        fflush(stderr);
      }
      last_op_finish_ = now;
    }

    done_++;
    if (done_ >= next_report_) {
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

  void AddBytes(int64_t n) { bytes_ += n; }

  void Report(const Slice& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    if (done_ < 1) done_ = 1;

    std::string extra;
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      double elapsed = (finish_ - start_) * 1e-6;
      char rate[100];
      snprintf(rate, sizeof(rate), "%6.1f MB/s, %.0f bytes",
               (bytes_ / 1048576.0) / elapsed, double(bytes_));
      extra = rate;
    }
    AppendWithSpace(&extra, message_);

    fprintf(stdout, "%-12s : %16.3f micros/op, %12.0f ops;%s%s\n",
            name.ToString().c_str(), seconds_ * 1e6 / done_, double(done_),
            (extra.empty() ? "" : " "), extra.c_str());
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

  explicit SharedState(int total)
      : cv(&mu), total(total), num_initialized(0), num_done(0), start(false) {}
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
  SharedState* shared;
  Stats stats;
  std::vector<uint32_t> fids;
  LookupStat parent_lstat;
  DirId parent_dir;
  Stat stat;

  ThreadState(int tid, int base_seed, bool random_order)
      : tid(tid), shared(NULL), parent_dir(0, 0) {
    fids.reserve(FLAGS_num);
    for (int i = 0; i < FLAGS_num; i++) {
      fids.push_back(i);
    }
    if (random_order) {
      std::random_shuffle(fids.begin(), fids.end(), STLRand(base_seed + tid));
    }
    if (!FLAGS_shared_dir) {
      parent_dir = DirId(0, tid + 1);
    }
    parent_lstat.SetDnodeNo(parent_dir.dno);
    parent_lstat.SetInodeNo(parent_dir.ino);
    parent_lstat.SetDirMode(0777);
    parent_lstat.SetZerothServer(0);
    parent_lstat.SetUserId(1);
    parent_lstat.SetGroupId(1);
    parent_lstat.SetLeaseDue(-1);
    parent_lstat.AssertAllSet();
    stat.SetDnodeNo(0);
    stat.SetInodeNo(0);  // To be be overridden later
    stat.SetFileMode(0660);
    stat.SetFileSize(0);
    stat.SetUserId(1);
    stat.SetGroupId(1);
    stat.SetZerothServer(-1);
    stat.SetChangeTime(0);
    stat.SetModifyTime(0);
    stat.AssertAllSet();
  }
};

}  // namespace

class Benchmark {
 private:
  FilesystemDb* db_;
  // If FLAGS_withfs is true, all read and write operations will be invoked
  // through fs_ instead of db_
  Filesystem* fs_;
  User me_;

  void PrintHeader() {
    PrintEnvironment();
    PrintWarnings();
    fprintf(stdout, "Threads:            %d\n", FLAGS_threads);
    fprintf(stdout, "Entries:            %d per thread\n", FLAGS_num);
    fprintf(stdout, "Block cache size:   %d MB\n", FLAGS_cache_size >> 20);
    fprintf(stdout, "Bloom bits:         %d\n", FLAGS_bloom_bits);
    fprintf(stdout, "Max open tables:    %d\n", FLAGS_max_open_files);
    fprintf(stdout, "Lsm compaction off: %d\n", FLAGS_disable_compaction);
    fprintf(stdout, "Shared dir:         %d\n", FLAGS_shared_dir);
    fprintf(stdout, "Snappy:             %d\n", FLAGS_snappy);
    fprintf(stdout, "Use fs api:         %d\n", FLAGS_withfs);
    fprintf(stdout, "Dry run:            %d\n", FLAGS_dryrun);
    fprintf(stdout, "Use existing db:    %d\n", FLAGS_use_existing_db);
    fprintf(stdout, "Db: %s\n", FLAGS_db);
    fprintf(stdout, "------------------------------------------------\n");
  }

  void PrintWarnings() {
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

  void PrintEnvironment() {
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
    void (Benchmark::*method)(ThreadState*);
    SharedState* shared;
    ThreadState* thread;
  };

  static void ThreadBody(void* v) {
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
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
    (arg->bm->*(arg->method))(thread);
    thread->stats.Stop();

    {
      MutexLock l(&shared->mu);
      shared->num_done++;
      if (shared->num_done >= shared->total) {
        shared->cv.SignalAll();
      }
    }
  }

  void RunBenchmark(int n, int m, Slice name,
                    void (Benchmark::*method)(ThreadState*)) {
    SharedState shared(n);

    ThreadArg* arg = new ThreadArg[n];
    for (int i = 0; i < n; i++) {
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      arg[i].thread = new ThreadState(
          i, m * 1000, name.ToString().find("random") != std::string::npos);
      arg[i].thread->shared = &shared;
      Env::Default()->StartThread(ThreadBody, &arg[i]);
    }

    shared.mu.Lock();
    while (shared.num_initialized < n) {
      shared.cv.Wait();
    }

    shared.start = true;
    shared.cv.SignalAll();
    while (shared.num_done < n) {
      shared.cv.Wait();
    }
    shared.mu.Unlock();

    for (int i = 1; i < n; i++) {
      arg[0].thread->stats.Merge(arg[i].thread->stats);
    }
    arg[0].thread->stats.Report(name);
    fprintf(stdout, " - total bytes written: %llu\n",
            static_cast<unsigned long long>(
                db_->GetDbEnv()->TotalDbBytesWritten()));
    fprintf(
        stdout, " - total bytes read: %llu\n",
        static_cast<unsigned long long>(db_->GetDbEnv()->TotalDbBytesRead()));
    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }
    delete[] arg;
  }

  void Write(ThreadState* thread) {
    const uint64_t tid = uint64_t(thread->tid) << 32;
    FilesystemDbStats stats;
    FilesystemDir* dir;
    if (FLAGS_withfs) {
      dir = fs_->TEST_ProbeDir(thread->parent_dir);
    }
    char tmp[20];
    Stat buf;
    for (int i = 0; i < FLAGS_num; i++) {
      const uint64_t fid = tid | thread->fids[i];
      Slice fname = Base64Encoding(tmp, fid);
      thread->stat.SetInodeNo(fid);
      if (FLAGS_dryrun) {
        fprintf(stdout, "put dir[%ld,%ld]/%s: fid=%ld\n",
                long(thread->parent_dir.dno), long(thread->parent_dir.ino),
                fname.ToString().c_str(), long(fid));
      } else {
        Status s;
        if (FLAGS_withfs) {
          s = fs_->Mkfle(me_, thread->parent_lstat, fname, 0660, &buf);
        } else {
          s = db_->Put(thread->parent_dir, fname, thread->stat, &stats);
        }
        if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
      }
      thread->stats.FinishedSingleOp(FLAGS_num);
    }
    if (FLAGS_withfs) {
      if (!FLAGS_shared_dir || thread->tid == 0)
        stats.Merge(fs_->TEST_FetchDbStats(dir));
      fs_->TEST_Release(dir);
    }
    int64_t bytes = stats.putkeybytes + stats.putbytes;
    thread->stats.AddBytes(bytes);
  }

  void Compact(ThreadState* thread) {
    Status s = db_->Flush();
    if (!s.ok()) {
      fprintf(stderr, "flush error: %s\n", s.ToString().c_str());
      exit(1);
    }
    s = db_->DrainCompaction();
    if (!s.ok()) {
      fprintf(stderr, "drain compaction error: %s\n", s.ToString().c_str());
      exit(1);
    }
  }

  void Read(ThreadState* thread) {
    const uint64_t tid = uint64_t(thread->tid) << 32;
    FilesystemDbStats stats;
    FilesystemDir* dir;
    if (FLAGS_withfs) {
      dir = fs_->TEST_ProbeDir(thread->parent_dir);
    }
    char tmp[20];
    Stat buf;
    int found = 0;
    for (int i = 0; i < FLAGS_reads; i++) {
      const uint64_t fid = tid | thread->fids[i];
      Slice fname = Base64Encoding(tmp, fid);
      if (FLAGS_dryrun) {
        fprintf(stdout, "get dir[%ld,%ld]/%s\n", long(thread->parent_dir.dno),
                long(thread->parent_dir.ino), fname.ToString().c_str());
      } else {
        Status s;
        if (FLAGS_withfs) {
          s = fs_->Lstat(me_, thread->parent_lstat, fname, &buf);
        } else {
          s = db_->Get(thread->parent_dir, fname, &buf, &stats);
        }
        if (s.ok()) {
          found++;
        } else if (!s.IsNotFound()) {
          fprintf(stderr, "get error: %s\n", s.ToString().c_str());
          exit(1);
        }
      }
      thread->stats.FinishedSingleOp(FLAGS_reads);
    }
    if (FLAGS_withfs) {
      if (!FLAGS_shared_dir || thread->tid == 0)
        stats.Merge(fs_->TEST_FetchDbStats(dir));
      fs_->TEST_Release(dir);
    }
    int64_t bytes = stats.getkeybytes + stats.getbytes;
    thread->stats.AddBytes(bytes);
    if (thread->tid == 0) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(thread 0: %d of %d found)", found,
               FLAGS_reads);
      thread->stats.AddMessage(msg);
    }
  }

  void Open() {
    FilesystemDbOptions dbopts;
    dbopts.compression = FLAGS_snappy;
    dbopts.disable_compaction = FLAGS_disable_compaction;
    dbopts.table_cache_size = FLAGS_max_open_files;
    dbopts.block_restart_interval = FLAGS_block_restart_interval;
    dbopts.block_cache_size = FLAGS_cache_size;
    dbopts.filter_bits_per_key = FLAGS_bloom_bits;
    dbopts.enable_io_monitoring = true;
    db_ = new FilesystemDb(dbopts);
    Status s = db_->Open(FLAGS_db);
    if (!s.ok()) {
      fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      exit(1);
    }

    FilesystemOptions opts;
    fs_ = new Filesystem(opts);
    fs_->SetDb(db_);
  }

 public:
  Benchmark() : db_(NULL), fs_(NULL) {
    me_.gid = me_.uid = 1;
    if (!FLAGS_use_existing_db) {
      DestroyDB(FLAGS_db, DBOptions());
    }
  }

  ~Benchmark() {  ///
    delete fs_;
    delete db_;
  }

  void Run() {
    PrintHeader();
    const char* benchmarks = FLAGS_benchmarks;
    int m = 0;
    while (benchmarks != NULL) {
      const char* sep = strchr(benchmarks, ',');
      Slice name;
      if (sep == NULL) {
        name = benchmarks;
        benchmarks = NULL;
      } else {
        name = Slice(benchmarks, sep - benchmarks);
        benchmarks = sep + 1;
      }

      void (Benchmark::*method)(ThreadState*) = NULL;
      bool fresh_db = false;

      if (name == Slice("fillrandom")) {
        fresh_db = true;
        method = &Benchmark::Write;
      } else if (name == Slice("compact")) {
        method = &Benchmark::Compact;
      } else if (name == Slice("readrandom")) {
        method = &Benchmark::Read;
      } else {
        if (!name.empty()) {  // No error message for empty name
          fprintf(stderr, "unknown benchmark '%s'\n", name.ToString().c_str());
        }
      }

      if (fresh_db) {
        if (FLAGS_use_existing_db) {
          fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n",
                  name.ToString().c_str());
          method = NULL;
        } else {
          delete fs_;
          fs_ = NULL;
          delete db_;
          db_ = NULL;
          DestroyDB(FLAGS_db, DBOptions());
          Open();
        }
      } else if (db_ == NULL) {
        Open();
      }

      if (method != NULL) {
        RunBenchmark(FLAGS_threads, m++, name, method);
      }
    }
  }
};
}  // namespace pdlfs

static void BM_Usage() {
  fprintf(stderr, "Use --bench to run db benchmark.\n");
}

static void BM_Main(int* argc, char*** argv) {
  pdlfs::FLAGS_bloom_bits = pdlfs::FilesystemDbOptions().filter_bits_per_key;
  pdlfs::FLAGS_max_open_files = pdlfs::FilesystemDbOptions().table_cache_size;
  pdlfs::FLAGS_block_restart_interval =
      pdlfs::FilesystemDbOptions().block_restart_interval;
  pdlfs::FLAGS_cache_size = pdlfs::FilesystemDbOptions().block_cache_size;
  std::string default_db_path;

  for (int i = 2; i < *argc; i++) {
    int n;
    char junk;
    if (pdlfs::Slice((*argv)[i]).starts_with("--benchmarks=")) {
      pdlfs::FLAGS_benchmarks = (*argv)[i] + strlen("--benchmarks=");
    } else if (sscanf((*argv)[i], "--histogram=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_histogram = n;
    } else if (sscanf((*argv)[i], "--withfs=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_withfs = n;
    } else if (sscanf((*argv)[i], "--dryrun=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_dryrun = n;
    } else if (sscanf((*argv)[i], "--snappy=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_snappy = n;
    } else if (sscanf((*argv)[i], "--no_compact=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_disable_compaction = n;
    } else if (sscanf((*argv)[i], "--shared_dir=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_shared_dir = n;
    } else if (sscanf((*argv)[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_use_existing_db = n;
    } else if (sscanf((*argv)[i], "--num=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_num = n;
    } else if (sscanf((*argv)[i], "--reads=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_reads = n;
    } else if (sscanf((*argv)[i], "--threads=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_threads = n;
    } else if (sscanf((*argv)[i], "--max_open_files=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_max_open_files = n;
    } else if (sscanf((*argv)[i], "--block_restart_interval=%d%c", &n, &junk) ==
               1) {
      pdlfs::FLAGS_block_restart_interval = n;
    } else if (sscanf((*argv)[i], "--block_cache_size=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_cache_size = n;
    } else if (sscanf((*argv)[i], "--bloom_bits=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_bloom_bits = n;
    } else if (strncmp((*argv)[i], "--db=", 5) == 0) {
      pdlfs::FLAGS_db = (*argv)[i] + 5;
    } else {
      fprintf(stderr, "Invalid flag: \"%s\"\n", (*argv)[i]);
      BM_Usage();
      exit(1);
    }
  }

  if (pdlfs::FLAGS_reads == -1) {
    pdlfs::FLAGS_reads = pdlfs::FLAGS_num;
  }

  // Choose a location for the test database if none given with --db=<path>
  if (pdlfs::FLAGS_db == NULL) {
    default_db_path = pdlfs::test::TmpDir() + "/fsdb_bench";
    pdlfs::FLAGS_db = default_db_path.c_str();
  }

  pdlfs::Benchmark benchmark;
  benchmark.Run();
}

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
