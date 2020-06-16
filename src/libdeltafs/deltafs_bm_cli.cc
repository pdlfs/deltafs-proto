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
#include "base64enc.h"
#include "env_wrapper.h"
#include "fs.h"
#include "fscli.h"
#include "fsdb.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/env.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/random.h"

#include <algorithm>
#include <arpa/inet.h>
#include <mpi.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdlib.h>
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
// Db options.
FilesystemDbOptions FLAGS_dbopts;

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

// If not NULL, will start a monitoring thread periodically sending perf stats
// to a local TSDB service at the specified uri.
const char* FLAGS_mon_destination_uri = NULL;

// Name for the time series data.
const char* FLAGS_mon_metric_name = "myfs.ops";

// Number of seconds for sending the next stats packet.
int FLAGS_mon_interval = 1;

// Uri for the information server.
const char* FLAGS_info_svr_uri = "tcp://127.0.0.1:10086";

// Print the ip addresses of all servers for debugging.
bool FLAGS_print_ips = false;

// Print the performance stats of each rank.
bool FLAGS_print_per_rank_stats = false;

// Skip fs checks.
bool FLAGS_skip_fs_checks = false;

// Insert keys in random order.
bool FLAGS_random_order = false;

// Force all ranks to share a single parent directory.
bool FLAGS_share_dir = false;

// Instantiate and use a local fs instead of connecting to a remote one.
bool FLAGS_fs_use_local = false;

// Combine multiple writes into a single rpc.
bool FLAGS_batched_writes = false;

// Number of writes per batch.
int FLAGS_batch_size = 16;

// Number of files to insert per rank.
int FLAGS_num = 8;

// Number of files to stat per rank.
int FLAGS_reads = -1;

// If true, do not destroy the existing database.
bool FLAGS_use_existing_db = false;

// Use the db at the following prefix.
const char* FLAGS_db_prefix = NULL;

// User id for the bench.
int FLAGS_uid = 1;

// Group id.
int FLAGS_gid = 1;

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
    next_report_ = 100;
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

  void FinishedSingleOp(int total) {
    done_++;
    if (FLAGS_rank == 0 && done_ >= next_report_) {
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
      fprintf(stdout, "%d: Finished %d ops (%.0f%%)%30s\r", FLAGS_rank, done_,
              100.0 * done_ / total, "");
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
    fprintf(stdout, "%-12d: %9.3f micros/op, %9.3f Mop/s, %9d Mops, %15d ops\n",
            FLAGS_rank, seconds_ * 1e6 / done_, done_ / 1000000.0 / elapsed,
            done_ / 1000000, done_);
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
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    if (done_ < 1) done_ = 1;
    // Rate is computed on actual elapsed time, not the sum of per-rank
    // elapsed times. On the other hand, per-op latency is computed on the sum
    // of per-rank elapsed times, not the actual elapsed time.
    double elapsed = (finish_ - start_) * 1e-6;
    fprintf(stdout,
            "==%-10s: %9.3f micros/op, %9.3f Mop/s, %9ld Mops, %15ld ops\n",
            name, seconds_ * 1e6 / done_, done_ / 1000000.0 / elapsed,
            done_ / 1000000, done_);
    fflush(stdout);
  }
};

// A wrapper over our own random object.
struct STLRand {
  STLRand(int seed) : rnd(seed) {}
  int operator()(int i) { return rnd.Next() % i; }
  Random rnd;
};

// Per-rank work state.
struct RankState {
  Stats stats;
  FilesystemCliCtx ctx;
  std::vector<uint32_t> fids;
  std::string::size_type prefix_length;
  std::string pathbuf;
  Stat stbuf;

  RankState() {
    fids.reserve(FLAGS_num);
    for (int i = 0; i < FLAGS_num; i++) {
      fids.push_back(i);
    }
    if (FLAGS_random_order) {
      std::random_shuffle(fids.begin(), fids.end(), STLRand(1000 * FLAGS_rank));
    }
    char tmp[30];
    pathbuf.reserve(100);
    pathbuf += "/";
    pathbuf += Base64Enc(tmp, FLAGS_share_dir ? 0 : FLAGS_rank).ToString();
    pathbuf += "/";
    prefix_length = pathbuf.size();
    User* const who = &ctx.who;
    who->uid = FLAGS_uid;
    who->gid = FLAGS_gid;
  }
};

// Dynamically construct uri strings based on a compact numeric server address
// representation.
class CompactUriMapper : public FilesystemCli::UriMapper {
 public:
  CompactUriMapper(const Slice& svr_map, int num_svrs, int num_ports_per_svr)
      : num_ports_per_svr_(num_ports_per_svr), num_svrs_(num_svrs) {
    port_map_ = reinterpret_cast<const unsigned short*>(&svr_map[0]);
    ip_map_ = reinterpret_cast<const unsigned*>(
        &svr_map[2 * num_ports_per_svr_ * num_svrs_]);
  }

  virtual std::string GetUri(int svr_idx, int port_idx) const {
    assert(svr_idx < num_svrs_);
    assert(port_idx < num_ports_per_svr_);
    char tmp[50];
    struct in_addr tmp_addr;
    tmp_addr.s_addr = ip_map_[svr_idx];
    snprintf(tmp, sizeof(tmp), "udp://%s:%hu", inet_ntoa(tmp_addr),
             port_map_[svr_idx * num_ports_per_svr_ + port_idx]);
    return tmp;
  }

 private:
  const unsigned short* port_map_;
  const unsigned* ip_map_;
  int num_ports_per_svr_;
  int num_svrs_;
};

class Client {
 private:
  FilesystemCli* fscli_;
  CompactUriMapper* uri_mapper_;
  std::string svr_map_;
  RPC* rpc_;
  Filesystem* fs_;
  FilesystemDb* fsdb_;
#if defined(PDLFS_RADOS)
  rados::RadosConnMgr* mgr_;
  Env* myenv_;
#endif

#if defined(PDLFS_RADOS)
  static void PrintRadosSettings() {
    fprintf(stdout, "Disable async io:   %d\n", FLAGS_rados_force_syncio);
    fprintf(stdout, "Cluster name:       %s\n", FLAGS_rados_cluster_name);
    fprintf(stdout, "Cli name:           %s\n", FLAGS_rados_cli_name);
    fprintf(stdout, "Storage pool name:  %s\n", FLAGS_rados_pool);
    fprintf(stdout, "Conf: %s\n", FLAGS_rados_conf);
  }
#endif

  static void PrintDbSettings() {
    fprintf(stdout, "Snappy:             %d\n", FLAGS_dbopts.compression);
    fprintf(stdout, "Blk cache size:     %-3d MB\n",
            int(FLAGS_dbopts.block_cache_size >> 20));
    fprintf(stdout, "Blk size:           %-3d KB\n",
            int(FLAGS_dbopts.block_size >> 10));
    fprintf(stdout, "Bloom bits:         %d\n",
            int(FLAGS_dbopts.filter_bits_per_key));
    fprintf(stdout, "Max open tables:    %d\n",
            int(FLAGS_dbopts.table_cache_size));
    fprintf(stdout, "Io monitoring:      %d\n",
            FLAGS_dbopts.enable_io_monitoring);
    fprintf(stdout, "Wal off:            %d\n",
            FLAGS_dbopts.disable_write_ahead_logging);
    fprintf(stdout, "Wal write size:     %-3d KB\n",
            int(FLAGS_dbopts.write_ahead_log_buffer >> 10));
    fprintf(stdout, "LSM COMPACTION OFF: %d\n",
            FLAGS_dbopts.disable_compaction);
    fprintf(stdout, "Memtable size:      %-3d MB\n",
            int(FLAGS_dbopts.memtable_size >> 20));
    fprintf(stdout, "Tbl size:           %-3d MB\n",
            int(FLAGS_dbopts.table_size >> 20));
    fprintf(stdout, "Tbl write size:     %-3d KB\n",
            int(FLAGS_dbopts.table_buffer >> 10));
    fprintf(stdout, "Tbl bulk read size: %-3d KB\n",
            int(FLAGS_dbopts.table_bulk_read_size >> 10));
    fprintf(stdout, "Prefetch compaction input: %d\n",
            FLAGS_dbopts.prefetch_compaction_input);
    fprintf(stdout, "Db level factor:    %d\n", FLAGS_dbopts.level_factor);
    fprintf(stdout, "L1 trigger:         %d\n",
            FLAGS_dbopts.l1_compaction_trigger);
#if defined(PDLFS_RADOS)
    fprintf(stdout, "Use rados:          %d\n", FLAGS_env_use_rados);
    if (FLAGS_env_use_rados) PrintRadosSettings();
#endif
    fprintf(stdout, "Use existing db:    %d\n", FLAGS_use_existing_db);
    fprintf(stdout, "Db: %s/r<rank>\n", FLAGS_db_prefix);
  }

  static void PrintHeader() {
    PrintWarnings();
    PrintEnvironment();
    fprintf(stdout, "Num ranks:          %d\n", FLAGS_comm_size);
    fprintf(stdout, "Fs info svr:        %s\n", FLAGS_info_svr_uri);
    fprintf(stdout, "Fs skip checks:     %d\n", FLAGS_skip_fs_checks);
    fprintf(stdout, "Num (rd/wr):        %d/%d per rank\n", FLAGS_reads,
            FLAGS_num);
    char bat_info[100];
    snprintf(bat_info, sizeof(bat_info), "%d (batch_size=%d)",
             FLAGS_batched_writes, FLAGS_batch_size);
    fprintf(stdout, "Batched writes:     %s\n",
            FLAGS_batched_writes ? bat_info : "OFF");
    char mon_info[100];
    snprintf(mon_info, sizeof(mon_info), "%s (every %ds)",
             FLAGS_mon_destination_uri, FLAGS_mon_interval);
    fprintf(stdout, "Mon:                %s\n",
            FLAGS_mon_destination_uri ? mon_info : "OFF");
    fprintf(stdout, "Random key order:   %d\n", FLAGS_random_order);
    fprintf(stdout, "Share dir:          %d\n", FLAGS_share_dir);
    if (FLAGS_fs_use_local) PrintDbSettings();
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

  static bool ParseMapData(const Slice& svr_map, int* num_svrs,
                           int* num_ports_per_svr) {
    if (svr_map.size() >= 8) {
      *num_svrs = DecodeFixed32(&svr_map[svr_map.size() - 8]);
      *num_ports_per_svr = DecodeFixed32(&svr_map[svr_map.size() - 4]);
      size_t size_per_rank = 2 * (*num_ports_per_svr) + 4;
      return (svr_map.size() == size_per_rank * (*num_svrs) + 8);
    } else {
      return false;
    }
  }

  static void ObtainSvrMap(std::string* dst) {
    using namespace rpc;
    RPCOptions rpcopts;
    rpcopts.mode = kClientOnly;
    rpcopts.uri = FLAGS_info_svr_uri;
    RPC* rpc = RPC::Open(rpcopts);
    If* const rpccli = rpc->OpenStubFor(FLAGS_info_svr_uri);
    If::Message in, out;
    EncodeFixed32(&in.buf[0], 0);
    in.contents = Slice(&in.buf[0], sizeof(uint32_t));
    Status s = rpccli->Call(in, out);
    if (!s.ok()) {
      fprintf(stderr, "Error calling fs info svr: %s\n", s.ToString().c_str());
      MPI_Finalize();
      exit(1);
    }
    if (out.contents.data() != out.extra_buf.data()) {
      dst->append(out.contents.data(), out.contents.size());
    } else {
      dst->swap(out.extra_buf);
    }
    delete rpccli;
    delete rpc;
  }

  Env* OpenEnv() {
    if (FLAGS_env_use_rados) {
#if defined(PDLFS_RADOS)
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

  void OpenLocal() {
    Env* env = OpenEnv();
    env->CreateDir(FLAGS_db_prefix);
    fsdb_ = new FilesystemDb(FLAGS_dbopts, env);
    char dbid[100];
    snprintf(dbid, sizeof(dbid), "/r%d", FLAGS_rank);
    std::string dbpath = FLAGS_db_prefix;
    dbpath += dbid;
    if (!FLAGS_use_existing_db) {
      FilesystemDb::DestroyDb(dbpath, env);
    }
    Status s = fsdb_->Open(dbpath);
    if (!s.ok()) {
      fprintf(stderr, "%d: Cannot open db: %s\n", FLAGS_rank,
              s.ToString().c_str());
      MPI_Finalize();
      exit(1);
    }

    FilesystemOptions opts;
    opts.skip_partition_checks = opts.skip_perm_checks =
        opts.skip_lease_due_checks = opts.skip_name_collision_checks =
            FLAGS_skip_fs_checks;
    opts.vsrvs = opts.nsrvs = 1;
    opts.srvid = 0;
    fs_ = new Filesystem(opts);
    fs_->SetDb(fsdb_);

    FilesystemCliOptions cliopts;
    cliopts.skip_perm_checks = FLAGS_skip_fs_checks;
    cliopts.batch_size = FLAGS_batch_size;
    fscli_ = new FilesystemCli(cliopts);
    fscli_->SetLocalFs(fs_);
  }

  void Open(int num_svrs, int num_ports_per_svr) {
    RPCOptions rpcopts;
    rpcopts.mode = rpc::kClientOnly;
    rpcopts.uri = "udp://-1:-1";
    rpc_ = RPC::Open(rpcopts);
    uri_mapper_ = new CompactUriMapper(svr_map_, num_svrs, num_ports_per_svr);
    if (FLAGS_rank == 0 && FLAGS_print_ips) {
      puts("Dumping fs uri(s) >>>");
      for (int i = 0; i < num_svrs; i++) {
        for (int j = 0; j < num_ports_per_svr; j++) {
          fprintf(stdout, "%s\n", uri_mapper_->GetUri(i, j).c_str());
        }
      }
      fflush(stdout);
    }
    FilesystemCliOptions cliopts;
    cliopts.skip_perm_checks = FLAGS_skip_fs_checks;
    cliopts.batch_size = FLAGS_batch_size;
    fscli_ = new FilesystemCli(cliopts);
    fscli_->RegisterFsSrvUris(rpc_, uri_mapper_, num_svrs, num_ports_per_svr);
  }

  void PrepareRun(RankState* const state) {
    if (!FLAGS_share_dir || FLAGS_rank == 0) {
      Status s = fscli_->Mkdir(&state->ctx, NULL, state->pathbuf.c_str(), 0755,
                               &state->stbuf);
      if (!s.ok()) {
        fprintf(stderr, "%d: Fail to mkdir: %s\n", FLAGS_rank,
                s.ToString().c_str());
        MPI_Finalize();
        exit(1);
      }
    }
  }

  void DoBatchedWrites(RankState* const state) {
    const uint64_t pid = uint64_t(FLAGS_rank) << 32;
    FilesystemCli::BAT* batch = NULL;
    state->pathbuf.resize(state->prefix_length);
    fscli_->BatchStart(&state->ctx, NULL, state->pathbuf.c_str(), &batch);
    char tmp[30];
    memset(tmp, 0, sizeof(tmp));
    for (int i = 0; i < FLAGS_num; i++) {
      Slice fname = Base64Enc(tmp, pid | state->fids[i]);
      Status s = fscli_->BatchInsert(batch, fname.c_str());
      if (!s.ok()) {
        fprintf(stderr, "%d: Cannot insert name into batch: %s\n", FLAGS_rank,
                s.ToString().c_str());
        MPI_Finalize();
        exit(1);
      }
      state->stats.FinishedSingleOp(FLAGS_num);
    }
    Status s = fscli_->BatchCommit(batch);
    if (!s.ok()) {
      fprintf(stderr, "%d: Fail to commit batch: %s\n", FLAGS_rank,
              s.ToString().c_str());
      MPI_Finalize();
      exit(1);
    }
    fscli_->BatchEnd(batch);
  }

  void DoWrites(RankState* const state) {
    const uint64_t pid = uint64_t(FLAGS_rank) << 32;
    char tmp[30];
    for (int i = 0; i < FLAGS_num; i++) {
      Slice fname = Base64Enc(tmp, pid | state->fids[i]);
      state->pathbuf.resize(state->prefix_length);
      state->pathbuf.append(fname.data(), fname.size());
      Status s = fscli_->Mkfle(&state->ctx, NULL, state->pathbuf.c_str(), 0644,
                               &state->stbuf);
      if (!s.ok()) {
        fprintf(stderr, "%d: Fail to mkfle: %s\n", FLAGS_rank,
                s.ToString().c_str());
        MPI_Finalize();
        exit(1);
      }
      state->stats.FinishedSingleOp(FLAGS_num);
    }
  }

  void DoReads(RankState* const state) {
    const uint64_t pid = uint64_t(FLAGS_rank) << 32;
    char tmp[30];
    for (int i = 0; i < FLAGS_reads; i++) {
      Slice fname = Base64Enc(tmp, pid | state->fids[i]);
      state->pathbuf.resize(state->prefix_length);
      state->pathbuf.append(fname.data(), fname.size());
      Status s = fscli_->Lstat(&state->ctx, NULL, state->pathbuf.c_str(),
                               &state->stbuf);
      if (!s.ok()) {
        fprintf(stderr, "%d: Fail to lstat: %s\n", FLAGS_rank,
                s.ToString().c_str());
        MPI_Finalize();
        exit(1);
      }
      state->stats.FinishedSingleOp(FLAGS_reads);
    }
  }

  void RunStep(const char* name, RankState* const state,
               void (Client::*method)(RankState*)) {
    GlobalStats stats;
    MPI_Barrier(MPI_COMM_WORLD);
    Stats* per_rank_stats = &state->stats;
    per_rank_stats->Start();
    (this->*method)(state);
    per_rank_stats->Stop();
    if (FLAGS_print_per_rank_stats) {
      per_rank_stats->Report();
    }
    MPI_Barrier(MPI_COMM_WORLD);
    stats.Reduce(per_rank_stats);
    if (FLAGS_rank == 0) {
      stats.Report(name);
    }
  }

  struct MonitorArg {
    Stats* stats;
    port::Mutex mutex;
    port::CondVar cv;
    bool is_mon_running;
    bool done;

    explicit MonitorArg(Stats* s)
        : stats(s), cv(&mutex), is_mon_running(false), done(false) {}
  };

  static void Send(UDPSocket* sock, Stats* stats) {
    char msg[100];
    snprintf(msg, sizeof(msg), "%s %10d %d rank=%d\n", FLAGS_mon_metric_name,
             int(time(NULL)), stats->done_, FLAGS_rank);
    Status s = sock->Send(msg);
    if (!s.ok()) {
      fprintf(stderr, "%d: Fail to send mon stats: %s\n", FLAGS_rank,
              s.ToString().c_str());
    }
  }

  static void MonitorBody(void* v) {
    MonitorArg* const arg = reinterpret_cast<MonitorArg*>(v);
    Stats* stats = arg->stats;
    UDPSocket* const sock = CreateUDPSocket();
    Status s = sock->Connect(FLAGS_mon_destination_uri);
    if (!s.ok()) {
      fprintf(stderr, "Cannot open mon socket: %s\n", s.ToString().c_str());
      delete sock;
      return;
    }

    MutexLock ml(&arg->mutex);
    arg->is_mon_running = true;
    arg->cv.SignalAll();
    while (!arg->done) {
      arg->mutex.Unlock();
      Send(sock, stats);
      SleepForMicroseconds(FLAGS_mon_interval);
      arg->mutex.Lock();
    }

    Send(sock, stats);
    arg->is_mon_running = false;
    arg->cv.SignalAll();
    delete sock;
  }

  void RunBenchmarks() {
    RankState state;
    PrepareRun(&state);
    MonitorArg mon_arg(&state.stats);
    if (FLAGS_mon_destination_uri) {
      Env::Default()->StartThread(MonitorBody, &mon_arg);
      MutexLock ml(&mon_arg.mutex);
      while (!mon_arg.is_mon_running) {
        mon_arg.cv.Wait();
      }
    }
    if (FLAGS_num != 0) {
      if (FLAGS_batched_writes) {
        RunStep("insert", &state, &Client::DoBatchedWrites);
      } else {
        RunStep("insert", &state, &Client::DoWrites);
      }
    }
    if (FLAGS_reads != 0) {
      SleepForMicroseconds(5 * 1000 * 1000);
      RunStep("fstats", &state, &Client::DoReads);
    }
    if (FLAGS_mon_destination_uri) {
      MutexLock ml(&mon_arg.mutex);
      mon_arg.done = true;
      while (mon_arg.is_mon_running) {
        mon_arg.cv.Wait();
      }
    }
  }

 public:
  Client()
      : fscli_(NULL), uri_mapper_(NULL), rpc_(NULL), fs_(NULL), fsdb_(NULL) {
#if defined(PDLFS_RADOS)
    mgr_ = NULL;
    myenv_ = NULL;
#endif
  }

  ~Client() {
    delete fscli_;
    delete uri_mapper_;
    delete rpc_;
    delete fs_;
    delete fsdb_;
#if defined(PDLFS_RADOS)
    delete myenv_;
    delete mgr_;
#endif
  }

  void Run() {
    if (FLAGS_rank == 0) {
      PrintHeader();
    }
    if (!FLAGS_fs_use_local) {
      uint32_t svr_map_size;
      if (FLAGS_rank != 0) {  // Non-roots get data from the root
        MPI_Bcast(&svr_map_size, 1, MPI_UINT32_T, 0, MPI_COMM_WORLD);
        svr_map_.resize(svr_map_size);
        MPI_Bcast(&svr_map_[0], svr_map_size, MPI_CHAR, 0, MPI_COMM_WORLD);
      } else {  // Root fetches data from remote and broadcasts it to non-roots
        ObtainSvrMap(&svr_map_);
        svr_map_size = svr_map_.size();
        MPI_Bcast(&svr_map_size, 1, MPI_UINT32_T, 0, MPI_COMM_WORLD);
        MPI_Bcast(&svr_map_[0], svr_map_size, MPI_CHAR, 0, MPI_COMM_WORLD);
      }
      int num_ports_per_svr;
      int num_svrs;
      if (!ParseMapData(svr_map_, &num_svrs, &num_ports_per_svr)) {
        if (FLAGS_rank == 0) {
          fprintf(stderr, "Cannot parse svr map\n");
        }
        MPI_Finalize();
        exit(1);
      }
      Open(num_svrs, num_ports_per_svr);
    } else {
      OpenLocal();
    }
    RunBenchmarks();
  }
};

}  // namespace
}  // namespace pdlfs

namespace {
void BM_Main(int* const argc, char*** const argv) {
  pdlfs::FLAGS_skip_fs_checks = pdlfs::FLAGS_random_order = true;
  pdlfs::FLAGS_dbopts.enable_io_monitoring = true;
  pdlfs::FLAGS_dbopts.prefetch_compaction_input = true;
  pdlfs::FLAGS_dbopts.disable_write_ahead_logging = true;
  pdlfs::FLAGS_dbopts.use_default_logger = true;
  pdlfs::FLAGS_dbopts.ReadFromEnv();

  for (int i = 1; i < (*argc); i++) {
    int n;
    char junk;
    if (sscanf((*argv)[i], "--print_ips=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_print_ips = n;
    } else if (sscanf((*argv)[i], "--print_per_rank_stats=%d%c", &n, &junk) ==
               1) {
      pdlfs::FLAGS_print_per_rank_stats = n;
    } else if (sscanf((*argv)[i], "--skip_fs_checks=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_skip_fs_checks = n;
    } else if (sscanf((*argv)[i], "--random_order=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_random_order = n;
    } else if (sscanf((*argv)[i], "--share_dir=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_share_dir = n;
    } else if (sscanf((*argv)[i], "--fs_use_local=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_fs_use_local = n;
    } else if (sscanf((*argv)[i], "--env_use_rados=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_env_use_rados = n;
    } else if (sscanf((*argv)[i], "--rados_force_syncio=%d%c", &n, &junk) ==
                   1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_rados_force_syncio = n;
    } else if (sscanf((*argv)[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_use_existing_db = n;
    } else if (sscanf((*argv)[i], "--disable_compaction=%d%c", &n, &junk) ==
                   1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_dbopts.disable_compaction = n;
    } else if (sscanf((*argv)[i], "--prefetch_compaction_input=%d%c", &n,
                      &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_dbopts.prefetch_compaction_input = n;
    } else if (sscanf((*argv)[i], "--batched_writes=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_batched_writes = n;
    } else if (sscanf((*argv)[i], "--batch_size=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_batch_size = n;
    } else if (sscanf((*argv)[i], "--num=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_num = n;
    } else if (sscanf((*argv)[i], "--reads=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_reads = n;
    } else if (sscanf((*argv)[i], "--mon_interval=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_mon_interval = n;
    } else if (strncmp((*argv)[i], "--mon_uri=", 10) == 0) {
      pdlfs::FLAGS_mon_destination_uri = (*argv)[i] + 10;
    } else if (strncmp((*argv)[i], "--info_svr_uri=", 15) == 0) {
      pdlfs::FLAGS_info_svr_uri = (*argv)[i] + 15;
    } else if (strncmp((*argv)[i], "--db=", 5) == 0) {
      pdlfs::FLAGS_db_prefix = (*argv)[i] + 5;
    } else {
      if (pdlfs::FLAGS_rank == 0) {
        fprintf(stderr, "%s:\nInvalid flag: '%s'\n", (*argv)[0], (*argv)[i]);
      }
      MPI_Finalize();
      exit(1);
    }
  }

  std::string default_db_prefix;
  // Choose a prefix for the test db if none given with --db=<path>
  if (!pdlfs::FLAGS_db_prefix) {
    default_db_prefix = "/tmp/deltafs_bm";
    pdlfs::FLAGS_db_prefix = default_db_prefix.c_str();
  }

  if (pdlfs::FLAGS_reads == -1 || pdlfs::FLAGS_reads > pdlfs::FLAGS_num) {
    pdlfs::FLAGS_reads = pdlfs::FLAGS_num;
  }

  pdlfs::Client cli;
  cli.Run();
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
