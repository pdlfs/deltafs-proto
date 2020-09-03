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
#include "env_wrapper.h"
#include "fs.h"
#include "fsdb.h"
#include "fsro.h"

#include "pdlfs-common/leveldb/db.h"
#include "pdlfs-common/leveldb/iterator.h"
#include "pdlfs-common/leveldb/write_batch.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/env.h"
#include "pdlfs-common/fsdbbase.h"
#include "pdlfs-common/gigaplus.h"
#include "pdlfs-common/hashmap.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/rpc.h"

#include <arpa/inet.h>
#include <errno.h>
#include <ifaddrs.h>
#include <mpi.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <vector>
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
// Options for the db at the compaction input end.
FilesystemReadonlyDbOptions FLAGS_src_dbopts;

// Compaction input dir.
const char* FLAGS_src_prefix = NULL;

// Options for the db at the compaction output end.
FilesystemDbOptions FLAGS_dst_dbopts;

// Compaction output dir.
const char* FLAGS_dst_prefix = NULL;

// Clean up the db dir at the output end on bootstrapping.
bool FLAGS_dst_force_cleaning = false;

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

// Use udp.
bool FLAGS_udp = false;

// Max incoming message size in bytes for server-side UDP sockets.
size_t FLAGS_udp_max_msgsz = 1432;

// UDP server-side sender buffer size in bytes.
int FLAGS_udp_sndbuf = 512 * 1024;

// UDP server-side receiver buffer size in bytes.
int FLAGS_udp_rcvbuf = 512 * 1024;

// For hosts with multiple ip addresses, use the one starting with the
// specified prefix.
const char* FLAGS_ip_prefix = "127.0.0.1";

// Print the ip addresses of all ranks for debugging.
bool FLAGS_print_ips = false;

// Print the performance stats of each rank.
bool FLAGS_print_per_rank_stats = false;

// Total number of ranks.
int FLAGS_comm_size = 1;

// My rank number.
int FLAGS_rank = 0;

// RPC timeout in seconds.
int FLAGS_rpc_timeout = 30;

// Min number of kv pairs that must be buffered before sending an rpc.
int FLAGS_rpc_batch_min = 1;

// Max number of kv pairs that can be buffered.
int FLAGS_rpc_batch_max = 2;

// Number of async rpc senders.
int FLAGS_rpc_async_sender_threads = 16;

// Number of rpc worker threads to run.
int FLAGS_rpc_worker_threads = 0;

// Number of rpc threads to run.
int FLAGS_rpc_threads = 1;

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

void PrintEnvironment() {
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
void PrintRadosSettings() {
  fprintf(stdout, "Disable async io:   %d\n", FLAGS_rados_force_syncio);
  fprintf(stdout, "Cluster name:       %s\n", FLAGS_rados_cluster_name);
  fprintf(stdout, "Cli name:           %s\n", FLAGS_rados_cli_name);
  fprintf(stdout, "Storage pool name:  %s\n", FLAGS_rados_pool);
  fprintf(stdout, "Conf: %s\n", FLAGS_rados_conf);
}
#endif

void PrintDstSettings() {
  fprintf(stdout, "Snappy:             %d\n", FLAGS_dst_dbopts.compression);
  fprintf(stdout, "Blk cache size:     %-4d MB\n",
          int(FLAGS_dst_dbopts.block_cache_size >> 20));
  fprintf(stdout, "Blk size:           %-4d KB\n",
          int(FLAGS_dst_dbopts.block_size >> 10));
  fprintf(stdout, "Bloom bits:         %d\n",
          int(FLAGS_dst_dbopts.filter_bits_per_key));
  fprintf(stdout, "Max open tables:    %d\n",
          int(FLAGS_dst_dbopts.table_cache_size));
  fprintf(stdout, "Io monitoring:      %d\n",
          FLAGS_dst_dbopts.enable_io_monitoring);
  fprintf(stdout, "Wal off:            %d\n",
          FLAGS_dst_dbopts.disable_write_ahead_logging);
  fprintf(stdout, "Wal write size:     %-4d KB\n",
          int(FLAGS_dst_dbopts.write_ahead_log_buffer >> 10));
  fprintf(stdout, "Lsm compaction off: %d\n",
          FLAGS_dst_dbopts.disable_compaction);
  fprintf(stdout, "Memtable size:      %-4d MB\n",
          int(FLAGS_dst_dbopts.memtable_size >> 20));
  fprintf(stdout, "Tbl size:           %-4d MB\n",
          int(FLAGS_dst_dbopts.table_size >> 20));
  fprintf(stdout, "Tbl write size:     %-4d KB\n",
          int(FLAGS_dst_dbopts.table_buffer >> 10));
  fprintf(stdout, "Tbl bulk read size: %-4d KB\n",
          int(FLAGS_dst_dbopts.table_bulk_read_size >> 10));
  fprintf(stdout, "Prefetch compaction input: %d\n",
          FLAGS_dst_dbopts.prefetch_compaction_input);
  fprintf(stdout, "Db level factor:    %d\n", FLAGS_dst_dbopts.level_factor);
  fprintf(stdout, "L0 limits:          %d (soft), %d (hard)\n",
          FLAGS_dst_dbopts.l0_soft_limit, FLAGS_dst_dbopts.l0_hard_limit);
  fprintf(stdout, "L1 trigger:         %d\n",
          FLAGS_dst_dbopts.l1_compaction_trigger);
  fprintf(stdout, "Db force cleaning:  %d\n", FLAGS_dst_force_cleaning);
  fprintf(stdout, "Db: %s/r<rank>\n", FLAGS_dst_prefix);
}

void PrintHeader() {
  PrintWarnings();
  PrintEnvironment();
  fprintf(stdout, "DELTAFS PARALLEL COMPACTOR\n");
  fprintf(stdout, "rpc ip:             %s*\n", FLAGS_ip_prefix);
  char udp_info[100];
  snprintf(udp_info, sizeof(udp_info),
           "Yes (MAX_MSGSZ=%d, SO_RCVBUF=%dK, SO_SNDBUF=%dK)",
           int(FLAGS_udp_max_msgsz), FLAGS_udp_rcvbuf >> 10,
           FLAGS_udp_sndbuf >> 10);
  fprintf(stdout, "rpc use udp:        %s\n", FLAGS_udp ? udp_info : "No");
  fprintf(stdout, "rpc batch:          %d (min), %d (max)\n",
          FLAGS_rpc_batch_min, FLAGS_rpc_batch_max);
  fprintf(stdout, "rpc timeout:        %d\n", FLAGS_rpc_timeout);
  fprintf(stdout, "num sender threads: %d (max outstanding rpcs)\n",
          FLAGS_rpc_async_sender_threads);
  fprintf(stdout, "num rpc threads:    %d + %d\n", FLAGS_rpc_threads,
          FLAGS_rpc_worker_threads);
  fprintf(stdout, "num ranks:          %d\n", FLAGS_comm_size);
  fprintf(stdout, "SOURCE DB:\n");
  fprintf(stdout, "Blk cache size:     %-4d MB\n",
          int(FLAGS_src_dbopts.block_cache_size >> 20));
  fprintf(stdout, "Max open tables:    %d\n",
          int(FLAGS_src_dbopts.table_cache_size));
  fprintf(stdout, "Io monitoring:      %d\n",
          FLAGS_src_dbopts.enable_io_monitoring);
  fprintf(stdout, "Db: %s/r<rank>\n", FLAGS_src_prefix);
  fprintf(stdout, "DESTINATION DB:\n");
  PrintDstSettings();
#if defined(PDLFS_RADOS)
  fprintf(stdout, "Use rados:          %d\n", FLAGS_env_use_rados);
  if (FLAGS_env_use_rados) PrintRadosSettings();
#endif
  fprintf(stdout, "------------------------------------------------\n");
}

// Per-rank performance stats.
struct Stats {
#if defined(PDLFS_OS_LINUX)
  struct rusage start_rusage_;
  struct rusage rusage_;
#endif
  double start_;
  double finish_;
  double seconds_;
  int done_;
  int next_report_;

#if defined(PDLFS_OS_LINUX)
  static uint64_t TimevalToMicros(const struct timeval* tv) {
    uint64_t t;
    t = static_cast<uint64_t>(tv->tv_sec) * 1000000;
    t += tv->tv_usec;
    return t;
  }
#endif

  void Start() {
    next_report_ = 1000;
    done_ = 0;
    seconds_ = 0;
    start_ = CurrentMicros();
    finish_ = start_;
#if defined(PDLFS_OS_LINUX)
    getrusage(RUSAGE_THREAD, &start_rusage_);
#endif
  }

  void Stop() {
#if defined(PDLFS_OS_LINUX)
    getrusage(RUSAGE_THREAD, &rusage_);
#endif
    finish_ = CurrentMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void FinishedSingleOp() {
    done_++;
    if (FLAGS_rank == 0 && done_ >= next_report_) {
      next_report_ += 1000;
      printf("%d: Finished %d ops\r", FLAGS_rank, done_);
      fflush(stdout);
    }
  }

  void Report() {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    if (done_ < 1) done_ = 1;
    // Rate is computed on actual elapsed time, not the sum of per-rank
    // elapsed times. On the other hand, per-op latency is computed on the sum
    // of per-rank elapsed times, not the actual elapsed time.
    double elapsed = (finish_ - start_) * 1e-6;
    fprintf(stdout,
            "%-12d: %9.3f micros/op, %9.3f Kop/s, %9.3f Kops, %15d ops\n",
            FLAGS_rank, seconds_ * 1e6 / done_, done_ / 1000.0 / elapsed,
            done_ / 1000.0, done_);
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
    fflush(stdout);
  }
};

// Global performance stats.
struct GlobalStats {
  double start_;
  double finish_;
  double seconds_;  // Total seconds of all ranks
  long done_;       // Total ops done

  void Reduce(const Stats* my) {
    long done = my->done_;
    MPI_Reduce(&done, &done_, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&my->seconds_, &seconds_, 1, MPI_DOUBLE, MPI_SUM, 0,
               MPI_COMM_WORLD);
    MPI_Reduce(&my->start_, &start_, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&my->finish_, &finish_, 1, MPI_DOUBLE, MPI_MAX, 0,
               MPI_COMM_WORLD);
  }

  void Report(const char* name) {
    if (FLAGS_rank != 0) return;
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    if (done_ < 1) done_ = 1;
    // Rate is computed on actual elapsed time, not the sum of per-rank
    // elapsed times. On the other hand, per-op latency is computed on the sum
    // of per-rank elapsed times, not the actual elapsed time.
    double elapsed = (finish_ - start_) * 1e-6;
    fprintf(stdout,
            "==%-10s: %9.3f micros/op, %9.3f Mop/s, %9.3f Mops, %15ld ops\n",
            name, seconds_ * 1e6 / done_, done_ / 1000000.0 / elapsed,
            done_ / 1000000.0, done_);
    fflush(stdout);
  }
};

template <typename T>
inline bool GetFixed32(Slice* input, T* rv) {
  if (input->size() < 4) return false;
  *rv = DecodeFixed32(input->data());
  input->remove_prefix(4);
  return true;
}

class AsyncKVSender {
 private:
  port::Mutex mu_;
  port::CondVar cv_;
  bool scheduled_;  // Ture if there is an outstanding rpc pending result
  Status status_;
  rpc::If* stub_;  // Owned by us
  rpc::If::Message in_, out_;
  std::string buf_;
  size_t n_;

  static void SenderCall(void* arg) {
    AsyncKVSender* s = reinterpret_cast<AsyncKVSender*>(arg);
    MutexLock ml(&s->mu_);
    s->SendIt();
  }

  void SendIt() {
    mu_.AssertHeld();
    assert(scheduled_);
    mu_.Unlock();
    Status s = stub_->Call(in_, out_);
    if (s.ok()) {
      Slice reply = out_.contents;
      uint32_t err_code = 0;
      if (!GetFixed32(&reply, &err_code)) {
        s = Status::Corruption("Bad rpc reply header");
      } else if (err_code != 0) {
        s = Status::FromCode(err_code);
      }
    }
    mu_.Lock();
    scheduled_ = false;
    cv_.SignalAll();
    if (!s.ok() && status_.ok()) {
      status_ = s;
    }
  }

  void Schedule(ThreadPool* pool) {
    mu_.AssertHeld();
    assert(!scheduled_);
    scheduled_ = true;
    EncodeFixed32(&buf_[0], n_);
    in_.extra_buf.swap(buf_);
    in_.contents = in_.extra_buf;
    buf_.clear();
    PutFixed32(&buf_, 0);
    n_ = 0;
    if (pool != NULL) {
      pool->Schedule(AsyncKVSender::SenderCall, this);
    } else {
      SendIt();
    }
  }

 public:
  explicit AsyncKVSender(rpc::If* stub)
      : cv_(&mu_), scheduled_(false), stub_(stub), n_(0) {
    PutFixed32(&buf_, 0);
  }
  ~AsyncKVSender() { delete stub_; }

  Status Flush(ThreadPool* pool) {
    Status s;
    MutexLock ml(&mu_);
    while (true) {
      if (!status_.ok()) {
        s = status_;
        break;
      } else if (n_ == 0) {
        break;  // Done
      } else if (!scheduled_) {
        Schedule(pool);
        break;
      } else {
        cv_.Wait();
      }
    }
    return s;
  }

  Status Send(ThreadPool* pool, const Slice& key, const Slice& val) {
    Status s;
    MutexLock ml(&mu_);
    PutLengthPrefixedSlice(&buf_, key);
    PutLengthPrefixedSlice(&buf_, val);
    n_++;
    while (true) {
      if (!status_.ok()) {
        s = status_;
        break;
      } else if (n_ < FLAGS_rpc_batch_min) {
        break;  // Done
      } else if (!scheduled_) {
        Schedule(pool);
        break;
      } else if (n_ < FLAGS_rpc_batch_max) {
        break;  // Done
      } else {
        cv_.Wait();
      }
    }
    return s;
  }

  Status WaitForAsyncOperations() {
    MutexLock ml(&mu_);
    while (scheduled_) {
      cv_.Wait();
    }
    return status_;
  }
};

class Compactor : public rpc::If {
 private:
  struct Dir {  // Entry doubles as a hash entry
    DirIndexOptions* giga_opts;
    DirIndex* giga;
    Dir* next_hash;
    size_t key_length;
    uint32_t hash;  // Hash of key(); used for fast partitioning and comparisons
    char key_data[1];  // Beginning of key

    Slice key() const {  // Return the key of the dir
      return Slice(key_data, key_length);
    }
  };
  std::vector<Dir*> dirrepo_;
  HashTable<Dir> dirs_;
  RPC* rpc_;
  AsyncKVSender** async_kv_senders_;
  ThreadPool* sender_workers_;
  ThreadPool* server_workers_;
  FilesystemReadonlyDb* srcdb_;
  FilesystemDb* dstdb_;
#if defined(PDLFS_RADOS)
  rados::RadosConnMgr* mgr_;
  Env* myenv_;
#endif

  static const char* PickAddr(char* dst) {
    const size_t prefix_len = strlen(FLAGS_ip_prefix);

    struct ifaddrs *ifaddr, *ifa;
    int rv = getifaddrs(&ifaddr);
    if (rv != 0) {
      fprintf(stderr, "%d: Cannot getifaddrs: %s\n", FLAGS_rank,
              strerror(errno));
      MPI_Finalize();
      exit(1);
    }

    for (ifa = ifaddr; ifa; ifa = ifa->ifa_next) {
      if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET) {
        continue;
      }
      char tmp[INET_ADDRSTRLEN];
      if (strncmp(
              inet_ntop(AF_INET,
                        &reinterpret_cast<struct sockaddr_in*>(ifa->ifa_addr)
                             ->sin_addr,
                        tmp, sizeof(tmp)),
              FLAGS_ip_prefix, prefix_len) == 0) {
        strcpy(dst, tmp);
        break;
      }
    }

    freeifaddrs(ifaddr);

    if (!dst[0]) {
      fprintf(stderr, "%d: Cannot find a matching addr: %s\n", FLAGS_rank,
              FLAGS_ip_prefix);
      MPI_Finalize();
      exit(1);
    }

    return dst;
  }

  Env* OpenEnv() {
    if (FLAGS_env_use_rados) {
#if defined(PDLFS_RADOS)
      if (myenv_) {
        return myenv_;
      }
      FLAGS_src_dbopts.detach_dir_on_close = true;
      FLAGS_dst_dbopts.detach_dir_on_close = true;
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

  void OpenDbs() {
    Env* const env = OpenEnv();
    char dbid[100];
    srcdb_ = new FilesystemReadonlyDb(FLAGS_src_dbopts, env);
    snprintf(dbid, sizeof(dbid), "/r%d", FLAGS_rank);
    std::string dbpath = FLAGS_src_prefix;
    dbpath += dbid;
    Status s = srcdb_->Open(dbpath);
    if (!s.ok()) {
      fprintf(stderr, "%d: Cannot open db: %s\n", FLAGS_rank,
              s.ToString().c_str());
      MPI_Abort(MPI_COMM_WORLD, 1);
    }
    if (!FLAGS_env_use_rados) {
      env->CreateDir(FLAGS_dst_prefix);
    }
    dstdb_ = new FilesystemDb(FLAGS_dst_dbopts, env);
    dbpath = FLAGS_dst_prefix;
    dbpath += dbid;
    if (FLAGS_dst_force_cleaning) {
      FilesystemDb::DestroyDb(dbpath, env);
    }
    s = dstdb_->Open(dbpath);
    if (!s.ok()) {
      fprintf(stderr, "%d: Cannot open db: %s\n", FLAGS_rank,
              s.ToString().c_str());
      MPI_Abort(MPI_COMM_WORLD, 1);
    }
  }

  int OpenPort(const char* ip) {
    RPCOptions rpcopts;
    rpcopts.udp_max_unexpected_msgsz = FLAGS_udp_max_msgsz;
    rpcopts.udp_srv_sndbuf = FLAGS_udp_sndbuf;
    rpcopts.udp_srv_rcvbuf = FLAGS_udp_rcvbuf;
    if (FLAGS_rpc_worker_threads != 0) {
      server_workers_ = ThreadPool::NewFixed(FLAGS_rpc_worker_threads);
    }
    rpcopts.extra_workers = server_workers_;
    rpcopts.num_rpc_threads = FLAGS_rpc_threads;
    rpcopts.rpc_timeout = FLAGS_rpc_timeout * 1000 * 1000;
    rpcopts.mode = rpc::kServerClient;
    rpcopts.impl = rpc::kSocketRPC;
    rpcopts.uri = FLAGS_udp ? "udp://" : "tcp://";
    rpcopts.uri += ip;
    rpcopts.fs = this;
    rpc_ = RPC::Open(rpcopts);
    Status s = rpc_->Start();
    if (!s.ok()) {
      fprintf(stderr, "%d: Cannot open port: %s\n", FLAGS_rank,
              s.ToString().c_str());
      MPI_Finalize();
      exit(1);
    }
    return rpc_->GetPort();
  }

  void OpenSenders(const unsigned short* const port_info,
                   const unsigned* const ip_info) {
    if (FLAGS_rpc_async_sender_threads != 0) {
      sender_workers_ = ThreadPool::NewFixed(FLAGS_rpc_async_sender_threads);
    }
    async_kv_senders_ = new AsyncKVSender*[FLAGS_comm_size];
    struct in_addr tmp_addr;
    char tmp_uri[100];
    for (int i = 0; i < FLAGS_comm_size; i++) {
      tmp_addr.s_addr = ip_info[i];
      snprintf(tmp_uri, sizeof(tmp_uri), "%s://%s:%hu",
               FLAGS_udp ? "udp" : "tcp", inet_ntoa(tmp_addr), port_info[i]);
      rpc::If* c = rpc_->OpenStubFor(tmp_uri);
      async_kv_senders_[i] = new AsyncKVSender(c);
    }
  }

  Dir* FetchDir(const Slice& dir_key) {
    const uint32_t hash = Hash(dir_key.data(), dir_key.size(), 0);
    Dir** const pos = dirs_.FindPointer(dir_key, hash);
    Dir* dir = *pos;
    if (dir != NULL) {
      return dir;
    } else {
      Key key(dir_key);
      int zserver = Filesystem::PickupServer(DirId(key.dnode(), key.inode()));
      dir = static_cast<Dir*>(malloc(sizeof(Dir) - 1 + dir_key.size()));
      dir->key_length = dir_key.size();
      memcpy(dir->key_data, dir_key.data(), dir_key.size());
      dir->hash = hash;
      dir->giga_opts = new DirIndexOptions;
      dir->giga_opts->num_virtual_servers = FLAGS_comm_size;
      dir->giga_opts->num_servers = FLAGS_comm_size;
      dir->giga = new DirIndex(zserver, dir->giga_opts);
      dir->giga->SetAll();
      dirs_.Inject(dir, pos);
      dirrepo_.push_back(dir);
      return dir;
    }
  }

  void MapReduce(Stats* stats) {
    Status s;
    ReadOptions read_options;
    read_options.fill_cache = false;
    Iterator* const iter = srcdb_->TEST_GetDbRep()->NewIterator(read_options);
    iter->SeekToFirst();
    while (iter->Valid()) {
      const Slice key = iter->key();
      assert(key.size() > 16);
      Slice name(key.data() + 16, key.size() - 16);
      Dir* const dir = FetchDir(Slice(key.data(), 16));
      int i = dir->giga->SelectServer(name);
      s = async_kv_senders_[i]->Send(sender_workers_, key, iter->value());
      if (!s.ok()) {
        fprintf(stderr, "%d: Cannot send rpc: %s\n", FLAGS_rank,
                s.ToString().c_str());
        MPI_Abort(MPI_COMM_WORLD, 1);
      }
      stats->FinishedSingleOp();
      iter->Next();
    }
    delete iter;
    if (FLAGS_rank == 0) {
      printf("Sender flushing...%30s\r", "");
    }
    for (int i = 0; i < FLAGS_comm_size; i++) {
      s = async_kv_senders_[i]->Flush(sender_workers_);
      if (!s.ok()) {
        fprintf(stderr, "%d: Cannot flush rpc: %s\n", FLAGS_rank,
                s.ToString().c_str());
        MPI_Abort(MPI_COMM_WORLD, 1);
      }
    }
    for (int i = 0; i < FLAGS_comm_size; i++) {
      s = async_kv_senders_[i]->WaitForAsyncOperations();
      if (!s.ok()) {
        fprintf(stderr, "%d: RPC errors: %s\n", FLAGS_rank,
                s.ToString().c_str());
        MPI_Abort(MPI_COMM_WORLD, 1);
      }
    }
    if (FLAGS_rank == 0) {
      printf("Waiting for other ranks...%30s\r", "");
    }
    MPI_Barrier(MPI_COMM_WORLD);
    if (FLAGS_rank == 0) {
      printf("Db flushing...%30s\r", "");
    }
    s = dstdb_->Flush(false);
    if (!s.ok()) {
      fprintf(stderr, "%d: Cannot flush db: %s\n", FLAGS_rank,
              s.ToString().c_str());
      MPI_Abort(MPI_COMM_WORLD, 1);
    }
    if (FLAGS_rank == 0) {
      printf("Done!%30s\n", "");
    }
  }

  void CollectAndReportMoreStats() {
    unsigned long long my_nr;
    unsigned long long my_rbytes;
    unsigned long long my_nw;
    unsigned long long my_wbytes;
    if (srcdb_ && FLAGS_src_dbopts.enable_io_monitoring) {
      my_rbytes = srcdb_->GetDbEnv()->TotalRndTblBytesRead();
      my_nr = srcdb_->GetDbEnv()->TotalRndTblReads();
      unsigned long long total_rbytes;
      unsigned long long total_nr;
      MPI_Reduce(&my_rbytes, &total_rbytes, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM,
                 0, MPI_COMM_WORLD);
      MPI_Reduce(&my_nr, &total_nr, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0,
                 MPI_COMM_WORLD);
      if (FLAGS_rank == 0) {
        fprintf(stdout,
                "Total random reads: %llu "
                "(Avg read size: %.1fK, total bytes read: %llu) "
                "// src db\n",
                total_nr, 1.0 * total_rbytes / 1024.0 / total_nr, total_rbytes);
      }
    }
    if (dstdb_ && FLAGS_dst_dbopts.enable_io_monitoring) {
      my_wbytes = dstdb_->GetDbEnv()->TotalTblBytesWritten();
      my_nw = dstdb_->GetDbEnv()->TotalTblWrites();
      unsigned long long total_wbytes;
      unsigned long long total_nw;
      MPI_Reduce(&my_wbytes, &total_wbytes, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM,
                 0, MPI_COMM_WORLD);
      MPI_Reduce(&my_nw, &total_nw, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0,
                 MPI_COMM_WORLD);
      if (FLAGS_rank == 0) {
        fprintf(stdout, "Total bytes written: %llu ", total_wbytes);
        fprintf(stdout, "(Avg write size: %.1fK)\n",
                1.0 * total_wbytes / 1024.0 / total_nw);
      }
    }
    if (FLAGS_rank == 0) {
      if (dstdb_) {
        fprintf(stdout, " - Db stats: >>>\n%s\n", dstdb_->GetDbStats().c_str());
        fprintf(stdout, " - L0 stats: >>>\n%s\n",
                dstdb_->GetDbLevel0Events().c_str());
      }
    }
  }

 public:
  Compactor()
      : rpc_(NULL),
        async_kv_senders_(NULL),
        sender_workers_(NULL),
        server_workers_(NULL),
        srcdb_(NULL),
        dstdb_(NULL) {
#if defined(PDLFS_RADOS)
    mgr_ = NULL;
    myenv_ = NULL;
#endif
  }

  ~Compactor() {
    for (int i = 0; i < FLAGS_comm_size; i++) {
      delete async_kv_senders_[i];
    }
    delete[] async_kv_senders_;
    delete rpc_;
    delete server_workers_;
    delete sender_workers_;
    delete srcdb_;
    delete dstdb_;
    for (int i = 0; i < dirrepo_.size(); i++) {
      Dir* const dir = dirrepo_[i];
      delete dir->giga;
      delete dir->giga_opts;
      free(dir);
    }
#if defined(PDLFS_RADOS)
    delete myenv_;
    delete mgr_;
#endif
  }

  virtual Status Call(Message& in, Message& out) RPCNOEXCEPT {
    Status s;
    WriteBatch batch;
    int n = 0;
    Slice input = in.contents;
    if (!GetFixed32(&input, &n)) {
      s = Status::InvalidArgument("Bad rpc request encoding");
    } else {
      Slice key;
      Slice val;
      for (int i = 0; i < n; i++) {
        if (!GetLengthPrefixedSlice(&input, &key) ||
            !GetLengthPrefixedSlice(&input, &val)) {
          s = Status::InvalidArgument("Bad kv encoding");
          break;
        } else {
          batch.Put(key, val);
        }
      }
    }
    if (s.ok()) {
      s = dstdb_->TEST_GetDbRep()->Write(WriteOptions(), &batch);
    }
    char* dst = &out.buf[0];
    EncodeFixed32(dst, s.err_code());
    out.contents = Slice(dst, 4);
    return Status::OK();
  }

  void Run() {
    if (FLAGS_rank == 0) {
      PrintHeader();
      puts("Bootstrapping...");
    }
    OpenDbs();
    char ip_str[INET_ADDRSTRLEN];
    memset(ip_str, 0, sizeof(ip_str));
    unsigned myip = inet_addr(PickAddr(ip_str));
    unsigned short port = OpenPort(ip_str);
    MPI_Barrier(MPI_COMM_WORLD);
    std::string addr_map;
    addr_map.resize(FLAGS_comm_size * 6, 0);
    unsigned short* const port_info =
        reinterpret_cast<unsigned short*>(&addr_map[0]);
    unsigned* const ip_info =
        reinterpret_cast<unsigned*>(&addr_map[2 * FLAGS_comm_size]);
    MPI_Allgather(&port, 1, MPI_UNSIGNED_SHORT, port_info, 1,
                  MPI_UNSIGNED_SHORT, MPI_COMM_WORLD);
    MPI_Allgather(&myip, 1, MPI_UNSIGNED, ip_info, 1, MPI_UNSIGNED,
                  MPI_COMM_WORLD);
    if (FLAGS_print_ips) {
      puts("Dumping fs uri(s) >>>");
      for (int i = 0; i < FLAGS_comm_size; i++) {
        struct in_addr tmp_addr;
        tmp_addr.s_addr = ip_info[i];
        fprintf(stdout, "%s:%hu\n", inet_ntoa(tmp_addr), port_info[i]);
      }
      fflush(stdout);
    }
    OpenSenders(port_info, ip_info);
    GlobalStats stats;
    MPI_Barrier(MPI_COMM_WORLD);
    Stats per_rank_stats;
    if (FLAGS_rank == 0) puts("Running...");
    per_rank_stats.Start();
    MapReduce(&per_rank_stats);
    per_rank_stats.Stop();
    if (FLAGS_print_per_rank_stats) {
      per_rank_stats.Report();
    }
    MPI_Barrier(MPI_COMM_WORLD);
    stats.Reduce(&per_rank_stats);
    stats.Report("mapreduce");
    CollectAndReportMoreStats();
    if (FLAGS_rank == 0) {
      puts("Bye");
    }
  }
};

}  // namespace
}  // namespace pdlfs

namespace {
void BM_Main(int* const argc, char*** const argv) {
  pdlfs::FLAGS_src_dbopts.enable_io_monitoring = true;
  pdlfs::FLAGS_src_dbopts.use_default_logger = true;
  pdlfs::FLAGS_src_dbopts.ReadFromEnv();
  pdlfs::FLAGS_dst_dbopts.enable_io_monitoring = true;
  pdlfs::FLAGS_dst_dbopts.use_default_logger = true;
  pdlfs::FLAGS_dst_dbopts.ReadFromEnv();
  pdlfs::FLAGS_dst_force_cleaning = true;
  pdlfs::FLAGS_udp = true;

  for (int i = 1; i < (*argc); i++) {
    int n;
    char u, junk;
    if (sscanf((*argv)[i], "--rpc_timeout=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_rpc_timeout = n;
    } else if (sscanf((*argv)[i], "--rpc_threads=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_rpc_threads = n;
    } else if (sscanf((*argv)[i], "--rpc_worker_threads=%d%c", &n, &junk) ==
               1) {
      pdlfs::FLAGS_rpc_worker_threads = n;
    } else if (sscanf((*argv)[i], "--rpc_batch_min=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_rpc_batch_min = n;
    } else if (sscanf((*argv)[i], "--rpc_batch_max=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_rpc_batch_max = n;
    } else if (sscanf((*argv)[i], "--rpc_async_sender_threads=%d%c", &n,
                      &junk) == 1) {
      pdlfs::FLAGS_rpc_async_sender_threads = n;
    } else if (sscanf((*argv)[i], "--print_ips=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_print_ips = n;
    } else if (sscanf((*argv)[i], "--env_use_rados=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_env_use_rados = n;
    } else if (sscanf((*argv)[i], "--rados_force_syncio=%d%c", &n, &junk) ==
                   1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_rados_force_syncio = n;
    } else if (sscanf((*argv)[i], "--udp_sndbuf=%d%c%c", &n, &u, &junk) == 2 &&
               (u == 'K' || u == 'k')) {
      pdlfs::FLAGS_udp_sndbuf = n << 10;
    } else if (sscanf((*argv)[i], "--udp_rcvbuf=%d%c%c", &n, &u, &junk) == 2 &&
               (u == 'K' || u == 'k')) {
      pdlfs::FLAGS_udp_rcvbuf = n << 10;
    } else if (sscanf((*argv)[i], "--udp_max_msgsz=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_udp_max_msgsz = n;
    } else if (sscanf((*argv)[i], "--udp=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_udp = n;
    } else if (sscanf((*argv)[i], "--dst_force_cleaning=%d%c", &n, &junk) ==
                   1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_dst_force_cleaning = n;
    } else if (strncmp((*argv)[i], "--dst_dir=", 10) == 0) {
      pdlfs::FLAGS_dst_prefix = (*argv)[i] + 10;
    } else if (strncmp((*argv)[i], "--src_dir=", 10) == 0) {
      pdlfs::FLAGS_src_prefix = (*argv)[i] + 10;
    } else if (strncmp((*argv)[i], "--ip=", 5) == 0) {
      pdlfs::FLAGS_ip_prefix = (*argv)[i] + 5;
    } else {
      if (pdlfs::FLAGS_rank == 0) {
        fprintf(stderr, "%s:\nInvalid flag: '%s'\n", (*argv)[0], (*argv)[i]);
      }
      MPI_Finalize();
      exit(1);
    }
  }

  std::string default_dst_prefix;
  if (pdlfs::FLAGS_dst_prefix == NULL) {
    default_dst_prefix = "/tmp/deltafs_bm_out";
    pdlfs::FLAGS_dst_prefix = default_dst_prefix.c_str();
  }

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
