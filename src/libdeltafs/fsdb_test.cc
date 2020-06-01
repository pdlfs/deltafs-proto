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
#include "fscli.h"

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
#include <sys/stat.h>
#include <time.h>
#include <vector>
#if defined(PDLFS_RADOS)
#include "pdlfs-common/rados/rados_connmgr.h"
#endif
#if defined(PDLFS_OS_LINUX)
#include <sys/resource.h>
#include <sys/time.h>
#endif

namespace pdlfs {

class FilesystemDbTest {
 public:
  FilesystemDbTest() : dbloc_(test::TmpDir() + "/fsdb_test") {
    DestroyDB(dbloc_, DBOptions());
    db_ = NULL;
  }

  Status OpenDb() {
    db_ = new FilesystemDb(options_, Env::GetUnBufferedIoEnv());
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

namespace {
// Transform a 64-bit (8-byte) integer into a 12-byte filename.
const char base64_table[] =
    "+-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
Slice Base64Encoding(char* const dst, uint64_t input) {
  input = htobe64(input);
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
}  // namespace

TEST(FilesystemDbTest, Base64) {
  char tmp[20];
  std::string prev;
  for (uint64_t i = 0; i < 1024 * 1024; i += 1024) {
    Slice r = Base64Encoding(tmp, i);
    ASSERT_TRUE(r.compare(prev) > 0);
    prev = r.ToString();
  }
}

namespace {  // Db benchmark
FilesystemDbOptions FLAGS_dboptions;

// Parameters for opening ceph
#if defined(PDLFS_RADOS)
const char* FLAGS_rados_cli_name = "client.admin";
const char* FLAGS_rados_cluster_name = "ceph";
const char* FLAGS_rados_pool = "test";
const char* FLAGS_rados_conf = "/tmp/ceph.conf";
#endif

// Testing env.
enum TestEnv { kRados, kUnbufferedIo, kDefault };
TestEnv FLAGS_env = kDefault;

// Testing mode.
enum TestMode { kFsFullCliApi, kFsCliApi, kFsApi, kDb };
TestMode FLAGS_mode = kDb;

// Comma-separated list of benchmarks to run in the specified order.
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

// Print histogram of op timings.
bool FLAGS_histogram = false;

// User id for the bench.
int FLAGS_uid = 1;

// Group id.
int FLAGS_gid = 1;

// Skip various fs checks.
bool FLAGS_fs_skip_checks = false;

// All files are inserted into a single parent directory.
bool FLAGS_shared_dir = false;

// If true, do not destroy the existing database.
bool FLAGS_use_existing_db = false;

// Use the db at the following name.
const char* FLAGS_db = NULL;

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
  int64_t bytes_;
  double last_op_finish_;
  Histogram hist_;
  std::string message_;

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

  static void AppendWithSpace(std::string* str, Slice msg) {
    if (msg.empty()) return;
    if (!str->empty()) {
      str->push_back(' ');
    }
    str->append(msg.data(), msg.size());
  }

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
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Stop() {
#if defined(PDLFS_OS_LINUX)
    getrusage(RUSAGE_THREAD, &rusage_);
#endif
    finish_ = CurrentMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void AddMessage(Slice msg) { AppendWithSpace(&message_, msg); }

  void FinishedSingleOp(int total, int tid) {
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

    // Per-op latency is computed on the sum of per-thread elapsed times, not
    // the actual elapsed time.
    fprintf(stdout, "==%-12s : %16.3f micros/op, %12.0f ops;%s%s\n",
            name.ToString().c_str(), seconds_ * 1e6 / done_, double(done_),
            (extra.empty() ? "" : " "), extra.c_str());
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
  bool prepare_write;
  std::vector<uint32_t> fids;
  DirId parent_dir;
  std::string pathname;
  std::string::size_type prefix_length;
  LookupStat parent_lstat;
  Stat parent_stat;
  Stat stat;

  ThreadState(int tid, int base_seed, bool random_order, bool prepare_write)
      : tid(tid),
        shared(NULL),
        prepare_write(prepare_write),
        parent_dir(0, FLAGS_shared_dir ? 1 : tid + 1) {
    fids.reserve(FLAGS_num);
    for (int i = 0; i < FLAGS_num; i++) {
      fids.push_back(i + 1);
    }
    if (random_order) {
      std::random_shuffle(fids.begin(), fids.end(), STLRand(base_seed + tid));
    }
    stat.SetDnodeNo(0);
    stat.SetInodeNo(0);  // To be overridden later
    stat.SetFileMode(0660 | S_IFREG);
    stat.SetFileSize(0);
    stat.SetUserId(FLAGS_uid);
    stat.SetGroupId(FLAGS_gid);
    stat.SetZerothServer(-1);
    stat.SetChangeTime(0);
    stat.SetModifyTime(0);
    stat.AssertAllSet();
    parent_stat.SetDnodeNo(parent_dir.dno);
    parent_stat.SetInodeNo(parent_dir.ino);
    parent_stat.SetFileMode(0770 | S_IFDIR);
    parent_stat.SetFileSize(0);
    parent_stat.SetUserId(FLAGS_uid);
    parent_stat.SetGroupId(FLAGS_gid);
    parent_stat.SetZerothServer(0);
    parent_stat.SetChangeTime(0);
    parent_stat.SetModifyTime(0);
    parent_stat.AssertAllSet();
    char tmp[30];
    pathname.reserve(100);
    pathname += "/";
    pathname += Base64Encoding(tmp, parent_dir.ino).ToString();
    pathname += "/";
    prefix_length = pathname.size();
    parent_lstat.CopyFrom(parent_stat);
    parent_lstat.SetLeaseDue(-1);
    parent_lstat.AssertAllSet();
  }
};

class Benchmark {
 private:
  FilesystemDb* db_;
  // According to FLAGS_mode, all read and write operations may be invoked
  // through fscli_ or fs_ instead of db_
  FilesystemCli* fscli_;
  Filesystem* fs_;
#if defined(PDLFS_RADOS)
  rados::RadosConnMgr* mgr_;
  Env* myenv_;
#endif
  User me_;

#if defined(PDLFS_RADOS)
  static void PrintRadosInfo() {
    fprintf(stdout, "RADOS:\n");
    fprintf(stdout, "Cluster name:       %s\n", FLAGS_rados_cluster_name);
    fprintf(stdout, "Cli name:           %s\n", FLAGS_rados_cli_name);
    fprintf(stdout, "Storage pool name:  %s\n", FLAGS_rados_pool);
    fprintf(stdout, "Conf: %s\n", FLAGS_rados_conf);
  }
#endif

  static void PrintHeader() {
    PrintEnvironment();
    PrintWarnings();
    fprintf(stdout, "Threads:            %d\n", FLAGS_threads);
    fprintf(stdout, "Num (rd/wr):        %d/%d per thread\n", FLAGS_reads,
            FLAGS_num);
    fprintf(stdout, "Skip fs checks:     %d\n", FLAGS_fs_skip_checks);
    fprintf(stdout, "Shared dir:         %d\n", FLAGS_shared_dir);
    fprintf(stdout, "Snappy:             %d\n", FLAGS_dboptions.compression);
    fprintf(stdout, "Block cache size:   %d MB\n",
            int(FLAGS_dboptions.block_cache_size >> 20));
    fprintf(stdout, "Block size:         %d KB\n",
            int(FLAGS_dboptions.block_size >> 10));
    fprintf(stdout, "Bloom bits:         %d\n",
            int(FLAGS_dboptions.filter_bits_per_key));
    fprintf(stdout, "Max open tables:    %d\n",
            int(FLAGS_dboptions.table_cache_size));
    fprintf(stdout, "Io monitoring:      %d\n",
            FLAGS_dboptions.enable_io_monitoring);
    fprintf(stdout, "WAL off:            %d\n",
            FLAGS_dboptions.disable_write_ahead_logging);
    fprintf(stdout, "WAL write buffer:   %d KB\n",
            int(FLAGS_dboptions.write_ahead_log_buffer >> 10));
    fprintf(stdout, "Lsm compaction off: %d\n",
            FLAGS_dboptions.disable_compaction);
    fprintf(stdout, "MEMTABLE size:      %d MB\n",
            int(FLAGS_dboptions.memtable_size >> 20));
    fprintf(stdout, "SST size:           %d MB\n",
            int(FLAGS_dboptions.table_size >> 20));
    fprintf(stdout, "SST write buffer:   %d KB\n",
            int(FLAGS_dboptions.table_buffer >> 10));
    fprintf(stdout, "Level factor:       %d\n", FLAGS_dboptions.level_factor);
    fprintf(stdout, "L1 trigger:         %d\n",
            FLAGS_dboptions.l1_compaction_trigger);
    fprintf(stdout, "Api:\n");
    fprintf(stdout, "Use fs cli full api:%d\n", FLAGS_mode == kFsFullCliApi);
    fprintf(stdout, "Use fs cli api:     %d\n", FLAGS_mode == kFsCliApi);
    fprintf(stdout, "Use fs api:         %d\n", FLAGS_mode == kFsApi);
    fprintf(stdout, "Env:\n");
    fprintf(stdout, "Use default:        %d\n", FLAGS_env == kDefault);
    fprintf(stdout, "Use unbuffered io:  %d\n", FLAGS_env == kUnbufferedIo);
#if defined(PDLFS_RADOS)
    fprintf(stdout, "Use rados:          %d\n", FLAGS_env == kRados);
    if (FLAGS_env == kRados) {
      PrintRadosInfo();
    }
#endif
    fprintf(stdout, "Db:\n");
    fprintf(stdout, "Use existing db:    %d\n", FLAGS_use_existing_db);
    fprintf(stdout, "Dbhome: %s\n", FLAGS_db);
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
    void (Benchmark::*method)(ThreadState*);
    SharedState* shared;
    ThreadState* thread;
  };

  static void ThreadBody(void* v) {
    ThreadArg* const arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
    if (thread->prepare_write) {
      arg->bm->PrepareWrite(thread);
    }
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

    ThreadArg* const arg = new ThreadArg[n];
    for (int i = 0; i < n; i++) {
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      arg[i].thread = new ThreadState(
          i, m * 1000, name.ToString().find("random") != std::string::npos,
          name.ToString().find("fill") != std::string::npos);
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
    if (FLAGS_dboptions.enable_io_monitoring) {
      fprintf(stdout, "Total bytes written: %llu ",
              static_cast<unsigned long long>(
                  db_->GetDbEnv()->TotalDbBytesWritten()));
      fprintf(stdout, "(Avg write size: %.1fK)\n",
              1.0 * db_->GetDbEnv()->TotalDbBytesWritten() / 1024.0 /
                  db_->GetDbEnv()->TotalDbWriteOps());
      fprintf(
          stdout, "Total bytes read: %llu ",
          static_cast<unsigned long long>(db_->GetDbEnv()->TotalDbBytesRead()));
      fprintf(stdout, "(Avg read size: %.1fK)\n",
              1.0 * db_->GetDbEnv()->TotalDbBytesRead() / 1024.0 /
                  db_->GetDbEnv()->TotalDbReadOps());
    }
    fprintf(stdout, " - Db stats: >>>\n%s\n", db_->GetDbStats().c_str());
    fprintf(stdout, " - L0 stats: >>>\n%s\n", db_->GetDbLevel0Events().c_str());
    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }
    delete[] arg;
  }

  void PrepareWrite(ThreadState* thread) {
    FilesystemDbStats stats;
    if (FLAGS_mode == kFsFullCliApi) {
      if (!FLAGS_shared_dir || thread->tid == 0) {
        Slice fname = thread->pathname;
        fname.remove_prefix(1);
        fname.remove_suffix(1);
        Status s = db_->Put(DirId(0, 0), fname, thread->parent_stat, &stats);
        if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
      }
    }
  }

  template <TestMode m>
  void DoWrite(ThreadState* thread) {
    const uint64_t tid = uint64_t(thread->tid) << 32;
    FilesystemDbStats stats;
    char tmp[20];
    for (int i = 0; i < FLAGS_num; i++) {
      const uint64_t fid = tid | thread->fids[i];
      Slice fname = Base64Encoding(tmp, fid);
      thread->stat.SetInodeNo(fid);
      Status s;
      switch (m) {
        case kFsFullCliApi: {
          std::string* const p = &thread->pathname;
          p->resize(thread->prefix_length);
          p->append(fname.data(), fname.size());
          s = fscli_->TEST_Mkfle(me_, p->c_str(), thread->stat, &stats);
          break;
        }
        case kFsCliApi:
          s = fscli_->TEST_Mkfle(me_, thread->parent_lstat, fname, thread->stat,
                                 &stats);
          break;
        case kFsApi:
          s = fs_->TEST_Mkfle(me_, thread->parent_lstat, fname, thread->stat,
                              &stats);
          break;
        default:
          s = db_->Put(thread->parent_dir, fname, thread->stat, &stats);
          break;
      }
      if (!s.ok()) {
        fprintf(stderr, "Db put error: %s\n", s.ToString().c_str());
        exit(1);
      }
      thread->stats.FinishedSingleOp(FLAGS_num, thread->tid);
    }
    int64_t bytes = stats.putkeybytes + stats.putbytes;
    thread->stats.AddBytes(bytes);
  }

  void Write(ThreadState* const thread) {
    switch (FLAGS_mode) {
      case kFsFullCliApi:
        DoWrite<kFsFullCliApi>(thread);
        break;
      case kFsCliApi:
        DoWrite<kFsCliApi>(thread);
        break;
      case kFsApi:
        DoWrite<kFsApi>(thread);
        break;
      default:
        DoWrite<kDb>(thread);
        break;
    }
  }

  void Compact(ThreadState* thread) {
    Status s = db_->Flush(true);
    if (!s.ok()) {
      fprintf(stderr, "Db flush error: %s\n", s.ToString().c_str());
      exit(1);
    }
    if (FLAGS_dboptions.disable_compaction) {
      return;
    }
    s = db_->DrainCompaction();
    if (!s.ok()) {
      fprintf(stderr, "Db drain compaction error: %s\n", s.ToString().c_str());
      exit(1);
    }
  }

  template <TestMode m>
  void DoRead(ThreadState* thread) {
    const uint64_t tid = uint64_t(thread->tid) << 32;
    FilesystemDbStats stats;
    char tmp[20];
    Stat buf;
    int found = 0;
    for (int i = 0; i < FLAGS_reads; i++) {
      const uint64_t fid = tid | thread->fids[i];
      Slice fname = Base64Encoding(tmp, fid);
      Status s;
      switch (m) {
        case kFsFullCliApi: {
          std::string* const p = &thread->pathname;
          p->resize(thread->prefix_length);
          p->append(fname.data(), fname.size());
          s = fscli_->TEST_Lstat(me_, p->c_str(), &buf, &stats);
          break;
        }
        case kFsCliApi:
          s = fscli_->TEST_Lstat(me_, thread->parent_lstat, fname, &buf,
                                 &stats);
          break;
        case kFsApi:
          s = fs_->TEST_Lstat(me_, thread->parent_lstat, fname, &buf, &stats);
          break;
        default:
          s = db_->Get(thread->parent_dir, fname, &buf, &stats);
          break;
      }
      if (s.ok()) {
        found++;
      } else if (!s.IsNotFound()) {
        fprintf(stderr, "Db get error: %s\n", s.ToString().c_str());
        exit(1);
      }
      thread->stats.FinishedSingleOp(FLAGS_reads, thread->tid);
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

  void Read(ThreadState* const thread) {
    switch (FLAGS_mode) {
      case kFsFullCliApi:
        DoRead<kFsFullCliApi>(thread);
        break;
      case kFsCliApi:
        DoRead<kFsCliApi>(thread);
        break;
      case kFsApi:
        DoRead<kFsApi>(thread);
        break;
      default:
        DoRead<kDb>(thread);
        break;
    }
  }

  Env* OpenEnv() {
    Env* env;
    switch (FLAGS_env) {
      case kRados: {
#if defined(PDLFS_RADOS)
        using namespace rados;
        mgr_ = new RadosConnMgr(RadosConnMgrOptions());
        RadosConn* conn;
        Osd* osd;
        ASSERT_OK(mgr_->OpenConn(  ///
            FLAGS_rados_cluster_name, FLAGS_rados_cli_name, FLAGS_rados_conf,
            RadosConnOptions(), &conn));
        ASSERT_OK(mgr_->OpenOsd(conn, FLAGS_rados_pool, RadosOptions(), &osd));
        myenv_ = mgr_->OpenEnv(osd, true, RadosEnvOptions());
        env = myenv_;
        mgr_->Release(conn);
        break;
#else
        fprintf(stderr, "Rados not installed\n");
        exit(1);
#endif
      }
      case kUnbufferedIo:
        env = Env::GetUnBufferedIoEnv();
        break;
      default:
        env = Env::Default();
        break;
    }
    return env;
  }

  void Open() {
    FilesystemDbOptions dbopts = FLAGS_dboptions;
    Env* env = OpenEnv();
    db_ = new FilesystemDb(dbopts, env);
    Status s = db_->Open(FLAGS_db);
    if (!s.ok()) {
      fprintf(stderr, "Cannot open db: %s\n", s.ToString().c_str());
      exit(1);
    }

    FilesystemOptions opts;
    opts.skip_partition_checks = opts.skip_perm_checks =
        opts.skip_lease_due_checks = opts.skip_name_collision_checks =
            FLAGS_fs_skip_checks;
    fs_ = new Filesystem(opts);
    fs_->SetDb(db_);

    FilesystemCliOptions cliopts;
    fscli_ = new FilesystemCli(cliopts);
    fscli_->SetLocalFs(fs_);
  }

 public:
  Benchmark() : db_(NULL), fscli_(NULL), fs_(NULL) {
#if defined(PDLFS_RADOS)
    myenv_ = NULL;
    mgr_ = NULL;
#endif
    me_.uid = FLAGS_uid;
    me_.gid = FLAGS_gid;
    if (!FLAGS_use_existing_db) {
      DestroyDB(FLAGS_db, DBOptions());
    }
  }

  ~Benchmark() {  ///
    delete fscli_;
    delete fs_;
    delete db_;
#if defined(PDLFS_RADOS)
    delete myenv_;
    delete mgr_;
#endif
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

      if (name.starts_with("fill")) {
        fresh_db = true;
        method = &Benchmark::Write;
      } else if (name == Slice("compact")) {
        method = &Benchmark::Compact;
      } else if (name.starts_with("read")) {
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
          delete fscli_;
          fscli_ = NULL;
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

}  // namespace
}  // namespace pdlfs

namespace {
void BM_Main(int* const argc, char*** const argv) {
  pdlfs::FLAGS_dboptions.use_default_logger = true;
  pdlfs::FLAGS_dboptions.ReadFromEnv();

  for (int i = 2; i < *argc; i++) {
    int n;
    char junk;
    if (pdlfs::Slice((*argv)[i]).starts_with("--benchmarks=")) {
      pdlfs::FLAGS_benchmarks = (*argv)[i] + strlen("--benchmarks=");
    } else if (sscanf((*argv)[i], "--histogram=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_histogram = n;
    } else if (sscanf((*argv)[i], "--skip_fs_checks=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_fs_skip_checks = n;
    } else if (sscanf((*argv)[i], "--with_fscli_full=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      if (n == 1) {
        pdlfs::FLAGS_mode = pdlfs::kFsFullCliApi;
      }
    } else if (sscanf((*argv)[i], "--with_fscli=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      if (n == 1) {
        pdlfs::FLAGS_mode = pdlfs::kFsCliApi;
      }
    } else if (sscanf((*argv)[i], "--with_fs=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      if (n == 1) {
        pdlfs::FLAGS_mode = pdlfs::kFsApi;
      }
    } else if (sscanf((*argv)[i], "--env_use_unbuffered_io=%d%c", &n, &junk) ==
                   1 &&
               (n == 0 || n == 1)) {
      if (n == 1) {
        pdlfs::FLAGS_env = pdlfs::kUnbufferedIo;
      }
    } else if (sscanf((*argv)[i], "--env_use_rados=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      if (n == 1) {
        pdlfs::FLAGS_env = pdlfs::kRados;
      }
    } else if (sscanf((*argv)[i], "--snappy=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_dboptions.compression = n;
    } else if (sscanf((*argv)[i], "--enable_io_monitoring=%d%c", &n, &junk) ==
                   1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_dboptions.enable_io_monitoring = n;
    } else if (sscanf((*argv)[i], "--disable_write_ahead_logging=%d%c", &n,
                      &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_dboptions.disable_write_ahead_logging = n;
    } else if (sscanf((*argv)[i], "--disable_compaction=%d%c", &n, &junk) ==
                   1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_dboptions.disable_compaction = n;
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
      pdlfs::FLAGS_dboptions.table_cache_size = n;
    } else if (sscanf((*argv)[i], "--table_file_size=%dM%c", &n, &junk) == 1) {
      pdlfs::FLAGS_dboptions.table_size = n << 20;
      pdlfs::FLAGS_dboptions.memtable_size = (n << 20) << 1;
    } else if (sscanf((*argv)[i], "--table_write_buffer=%dK%c", &n, &junk) ==
               1) {
      pdlfs::FLAGS_dboptions.table_buffer = n << 10;
    } else if (sscanf((*argv)[i], "--block_size=%dK%c", &n, &junk) == 1) {
      pdlfs::FLAGS_dboptions.block_size = n << 10;
    } else if (sscanf((*argv)[i], "--l1_compaction_trigger=%d%c", &n, &junk) ==
               1) {
      pdlfs::FLAGS_dboptions.l1_compaction_trigger = n;
    } else if (sscanf((*argv)[i], "--level_factor=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_dboptions.level_factor = n;
    } else if (sscanf((*argv)[i], "--block_restart_interval=%d%c", &n, &junk) ==
               1) {
      pdlfs::FLAGS_dboptions.block_restart_interval = n;
    } else if (sscanf((*argv)[i], "--block_cache_size=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_dboptions.block_cache_size = n;
    } else if (sscanf((*argv)[i], "--bloom_bits=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_dboptions.filter_bits_per_key = n;
    } else if (strncmp((*argv)[i], "--db=", 5) == 0) {
      pdlfs::FLAGS_db = (*argv)[i] + 5;
    } else {
      fprintf(stderr, "Invalid flag: \"%s\"\n", (*argv)[i]);
      exit(1);
    }
  }

  if (pdlfs::FLAGS_reads == -1 || pdlfs::FLAGS_reads > pdlfs::FLAGS_num) {
    pdlfs::FLAGS_reads = pdlfs::FLAGS_num;
  }

  std::string default_db_path;
  // Choose a location for the test database if none given with --db=<path>
  if (pdlfs::FLAGS_db == NULL) {
    default_db_path = pdlfs::test::TmpDir() + "/fsdb_bench";
    pdlfs::FLAGS_db = default_db_path.c_str();
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
