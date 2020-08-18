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
#include "fsis.h"
#include "fssvr.h"

#include "pdlfs-common/leveldb/db.h"
#include "pdlfs-common/leveldb/options.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/port.h"

#include <arpa/inet.h>
#include <errno.h>
#include <ifaddrs.h>
#include <mpi.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <vector>
#if defined(PDLFS_RADOS)
#include "pdlfs-common/rados/rados_connmgr.h"
#endif
#if defined(PDLFS_OS_LINUX)
#include <ctype.h>
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

// Use udp.
bool FLAGS_udp = false;

// Max incoming message size for UDP in bytes.
size_t FLAGS_udp_max_msgsz = 1432;

// UDP sender buffer size in bytes.
int FLAGS_udp_sndbuf = 512 * 1024;

// UDP receiver buffer size in bytes.
int FLAGS_udp_rcvbuf = 512 * 1024;

// If a host is configured with 2 or more ip addresses, use the one with the
// following prefix.
const char* FLAGS_ip_prefix = "127.0.0.1";

// Listening port for the information server.
int FLAGS_info_port = 10086;

// Number of listening ports per rank.
int FLAGS_ports_per_rank = 1;

// Print the ip addresses of all ranks for debugging.
bool FLAGS_print_ips = false;

// Initialize a dummy server for rpc testing.
bool FLAGS_dummy_svr = false;

// Skip all fs checks.
bool FLAGS_skip_fs_checks = false;

// If true, will reuse the existing fs image.
bool FLAGS_use_existing_fs = false;

// Use the db at the following prefix.
const char* FLAGS_db_prefix = NULL;

// Number of rpc worker threads to run.
int FLAGS_rpc_worker_threads = 0;

// Number of rpc threads to run.
int FLAGS_rpc_threads = 1;

class Server : public FilesystemWrapper {
 private:
  port::Mutex mu_;
  port::AtomicPointer shutting_down_;
  port::CondVar cv_;
  FilesystemInfoServer* infosvr_;
  std::vector<FilesystemServer*> svrs_;
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
    fprintf(stdout, "Blk cache size:     %-4d MB\n",
            int(FLAGS_dbopts.block_cache_size >> 20));
    fprintf(stdout, "Blk size:           %-4d KB\n",
            int(FLAGS_dbopts.block_size >> 10));
    fprintf(stdout, "Bloom bits:         %d\n",
            int(FLAGS_dbopts.filter_bits_per_key));
    fprintf(stdout, "Max open tables:    %d\n",
            int(FLAGS_dbopts.table_cache_size));
    fprintf(stdout, "Io monitoring:      %d\n",
            FLAGS_dbopts.enable_io_monitoring);
    fprintf(stdout, "Wal off:            %d\n",
            FLAGS_dbopts.disable_write_ahead_logging);
    fprintf(stdout, "Wal write size:     %-4d KB\n",
            int(FLAGS_dbopts.write_ahead_log_buffer >> 10));
    fprintf(stdout, "LSM COMPACTION OFF: %d\n",
            FLAGS_dbopts.disable_compaction);
    fprintf(stdout, "Memtable size:      %-4d MB\n",
            int(FLAGS_dbopts.memtable_size >> 20));
    fprintf(stdout, "Tbl size:           %-4d MB\n",
            int(FLAGS_dbopts.table_size >> 20));
    fprintf(stdout, "Tbl write size:     %-4d KB\n",
            int(FLAGS_dbopts.table_buffer >> 10));
    fprintf(stdout, "Tbl bulk read size: %-4d KB\n",
            int(FLAGS_dbopts.table_bulk_read_size >> 10));
    fprintf(stdout, "Prefetch compaction input: %d\n",
            FLAGS_dbopts.prefetch_compaction_input);
    fprintf(stdout, "Db level factor:    %d\n", FLAGS_dbopts.level_factor);
    fprintf(stdout, "L0 limits:          %d (soft), %d (hard)\n",
            FLAGS_dbopts.l0_soft_limit, FLAGS_dbopts.l0_hard_limit);
    fprintf(stdout, "L1 trigger:         %d\n",
            FLAGS_dbopts.l1_compaction_trigger);
#if defined(PDLFS_RADOS)
    fprintf(stdout, "Use rados:          %d\n", FLAGS_env_use_rados);
    if (FLAGS_env_use_rados) PrintRadosSettings();
#endif
    fprintf(stdout, "Use existing db:    %d\n", FLAGS_use_existing_fs);
    fprintf(stdout, "Db: %s/r<rank>\n", FLAGS_db_prefix);
  }

  static void PrintHeader() {
    PrintEnvironment();
    PrintWarnings();
    fprintf(stdout, "Rpc ip:             %s*\n", FLAGS_ip_prefix);
    char udp_info[100];
    snprintf(udp_info, sizeof(udp_info),
             "Yes (MAX_MSGSZ=%d, SO_RCVBUF=%dK, SO_SNDBUF=%dK)",
             int(FLAGS_udp_max_msgsz), FLAGS_udp_rcvbuf >> 10,
             FLAGS_udp_sndbuf >> 10);
    fprintf(stdout, "Rpc use udp:        %s\n", FLAGS_udp ? udp_info : "No");
    fprintf(stdout, "Num rpc threads:    %d + %d\n", FLAGS_rpc_threads,
            FLAGS_rpc_worker_threads);
    fprintf(stdout, "Num ports per rank: %d\n", FLAGS_ports_per_rank);
    fprintf(stdout, "Num ranks:          %d\n", FLAGS_comm_size);
    fprintf(stdout, "Fs use existing:    %d\n", FLAGS_use_existing_fs);
    fprintf(stdout, "Fs info port:       %d\n", FLAGS_info_port);
    fprintf(stdout, "Fs skip checks:     %d\n", FLAGS_skip_fs_checks);
    fprintf(stdout, "Fs dummy:           %d\n", FLAGS_dummy_svr);
    if (!FLAGS_dummy_svr) {
      PrintDbSettings();
    }
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
      FLAGS_dbopts.bulk_use_copy = false;
      FLAGS_dbopts.create_dir_on_bulk = true;
      FLAGS_dbopts.attach_dir_on_bulk = true;
      FLAGS_dbopts.detach_dir_on_bulk_end = true;
      FLAGS_dbopts.detach_dir_on_close = true;
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

  FilesystemIf* OpenFilesystem() {
    Env* env = OpenEnv();
    if (!FLAGS_env_use_rados) {
      env->CreateDir(FLAGS_db_prefix);
    }
    fsdb_ = new FilesystemDb(FLAGS_dbopts, env);
    char dbid[100];
    snprintf(dbid, sizeof(dbid), "/r%d", FLAGS_rank);
    std::string dbpath = FLAGS_db_prefix;
    dbpath += dbid;
    if (!FLAGS_use_existing_fs) {
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
    opts.vsrvs = opts.nsrvs = FLAGS_comm_size;
    opts.mydno = opts.srvid = FLAGS_rank;
    fs_ = new Filesystem(opts);
    fs_->SetDb(fsdb_);
    return fs_;
  }

  static FilesystemInfoServer* OpenInfoPort(const char* ip) {
    FilesystemInfoServerOptions infosvropts;
    infosvropts.num_rpc_threads = 1;
    char uri[50];
    snprintf(uri, sizeof(uri), "tcp://%s:%d", ip, FLAGS_info_port);
    infosvropts.uri = uri;
    FilesystemInfoServer* const infosvr = new FilesystemInfoServer(infosvropts);
    Status s = infosvr->OpenServer();
    if (!s.ok()) {
      fprintf(stderr, "%d: Cannot open info port: %s\n", FLAGS_rank,
              s.ToString().c_str());
      MPI_Finalize();
      exit(1);
    }
    return infosvr;
  }

  static FilesystemServer* OpenPort(const char* ip, FilesystemIf* const fs) {
    FilesystemServerOptions svropts;
    svropts.num_rpc_worker_threads = FLAGS_rpc_worker_threads;
    svropts.num_rpc_threads = FLAGS_rpc_threads;
    svropts.uri = FLAGS_udp ? "udp://" : "tcp://";
    svropts.uri += ip;
    svropts.udp_max_incoming_msgsz = FLAGS_udp_max_msgsz;
    svropts.udp_rcvbuf = FLAGS_udp_rcvbuf;
    svropts.udp_sndbuf = FLAGS_udp_sndbuf;
    FilesystemServer* const rpcsvr = new FilesystemServer(svropts);
    rpcsvr->SetFs(fs);
    Status s = rpcsvr->OpenServer();
    if (!s.ok()) {
      fprintf(stderr, "%d: Cannot open port: %s\n", FLAGS_rank,
              s.ToString().c_str());
      MPI_Finalize();
      exit(1);
    }
    return rpcsvr;
  }

 public:
  Server()
      : shutting_down_(NULL),
        cv_(&mu_),
        infosvr_(NULL),
        fs_(NULL),
        fsdb_(NULL) {
#if defined(PDLFS_RADOS)
    mgr_ = NULL;
    myenv_ = NULL;
#endif
  }

  ~Server() {
    delete infosvr_;
    for (size_t i = 0; i < svrs_.size(); i++) {
      delete svrs_[i];
    }
    delete fs_;
    delete fsdb_;
#if defined(PDLFS_RADOS)
    delete myenv_;
    delete mgr_;
#endif
  }

#if __cplusplus >= 201103L
#define OVERRIDE override
#else
#define OVERRIDE
#endif
  virtual Status Lokup(const User& who, const LookupStat& parent,
                       const Slice& name, LookupStat*) OVERRIDE {
    return Status::OK();
  }
  virtual Status Mkdir(const User& who, const LookupStat& parent,
                       const Slice& name, uint32_t mode, Stat*) OVERRIDE {
    return Status::OK();
  }
  virtual Status Mkfle(const User& who, const LookupStat& parent,
                       const Slice& name, uint32_t mode, Stat*) OVERRIDE {
    return Status::OK();
  }
  virtual Status Mkfls(const User& who, const LookupStat& parent,
                       const Slice& namearr, uint32_t mode,
                       uint32_t* n) OVERRIDE {
    return Status::OK();
  }
#undef OVERRIDE

  void Interrupt() {
    MutexLock ml(&mu_);
    shutting_down_.Release_Store(this);
    cv_.Signal();
  }

  void Run() {
    if (FLAGS_rank == 0) {
      PrintHeader();
    }
    FilesystemIf* const fs = FLAGS_dummy_svr ? this : OpenFilesystem();
    char ip_str[INET_ADDRSTRLEN];
    memset(ip_str, 0, sizeof(ip_str));
    unsigned myip = inet_addr(PickAddr(ip_str));
    int np = FLAGS_ports_per_rank;
    std::vector<unsigned short> myports;
    for (int i = 0; i < np; i++) {
      FilesystemServer* const svr = OpenPort(ip_str, fs);
      myports.push_back(svr->GetPort());
      svrs_.push_back(svr);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    // A svr map consists of a port map, an ip map, and a footer specifying the
    // total number of svrs and the number of ports per svr.
    std::string svr_map;
    if (FLAGS_rank != 0) {  // Non-roots send their addr info to the root
      MPI_Gather(&myports[0], np, MPI_UNSIGNED_SHORT, NULL, np,
                 MPI_UNSIGNED_SHORT, 0, MPI_COMM_WORLD);
      MPI_Gather(&myip, 1, MPI_UNSIGNED, NULL, 1, MPI_UNSIGNED, 0,
                 MPI_COMM_WORLD);
    } else {  // Root gathers info from non-roots and starts the info svr
      size_t size_per_rank = 2 * np + 4;
      svr_map.resize(size_per_rank * FLAGS_comm_size + 8);
      EncodeFixed32(&svr_map[svr_map.size() - 8], FLAGS_comm_size);
      EncodeFixed32(&svr_map[svr_map.size() - 4], np);
      unsigned short* const port_info =
          reinterpret_cast<unsigned short*>(&svr_map[0]);
      unsigned* const ip_info =
          reinterpret_cast<unsigned*>(&svr_map[2 * np * FLAGS_comm_size]);
      MPI_Gather(&myports[0], np, MPI_UNSIGNED_SHORT, port_info, np,
                 MPI_UNSIGNED_SHORT, 0, MPI_COMM_WORLD);
      MPI_Gather(&myip, 1, MPI_UNSIGNED, ip_info, 1, MPI_UNSIGNED, 0,
                 MPI_COMM_WORLD);
      infosvr_ = OpenInfoPort(ip_str);
      infosvr_->SetInfo(0, svr_map);
      if (FLAGS_print_ips) {
        struct in_addr tmp_addr;
        puts("Dumping fs uri(s) >>>");
        for (int i = 0; i < FLAGS_comm_size; i++) {
          tmp_addr.s_addr = ip_info[i];
          for (int j = 0; j < np; j++) {
            fprintf(stdout, "%s:%hu\n", inet_ntoa(tmp_addr),
                    port_info[j + i * np]);
          }
          fflush(stdout);
        }
      }
      puts("Running...");
    }
    MutexLock ml(&mu_);
    while (!shutting_down_.Acquire_Load()) {
      cv_.Wait();
    }
    if (FLAGS_rank == 0) {
      infosvr_->Close();
    }
    for (int i = 0; i < FLAGS_ports_per_rank; i++) {
      svrs_[i]->Close();
    }
    MPI_Barrier(MPI_COMM_WORLD);
    if (fsdb_ && FLAGS_rank == 0) {
      if (FLAGS_dbopts.enable_io_monitoring) {
        fprintf(stdout, "Total random reads: %llu ",
                static_cast<unsigned long long>(
                    fsdb_->GetDbEnv()->TotalRndTblReads()));
        fprintf(stdout, "(Avg read size: %.1fK, total bytes read: %llu)\n",
                1.0 * fsdb_->GetDbEnv()->TotalRndTblBytesRead() / 1024.0 /
                    fsdb_->GetDbEnv()->TotalRndTblReads(),
                static_cast<unsigned long long>(
                    fsdb_->GetDbEnv()->TotalRndTblBytesRead()));
        fprintf(stdout, "Total sequential bytes read: %llu ",
                static_cast<unsigned long long>(
                    fsdb_->GetDbEnv()->TotalSeqTblBytesRead()));
        fprintf(stdout, "(Avg read size: %.1fK)\n",
                1.0 * fsdb_->GetDbEnv()->TotalSeqTblBytesRead() / 1024.0 /
                    fsdb_->GetDbEnv()->TotalSeqTblReads());
        fprintf(stdout, "Total bytes written: %llu ",
                static_cast<unsigned long long>(
                    fsdb_->GetDbEnv()->TotalTblBytesWritten()));
        fprintf(stdout, "(Avg write size: %.1fK)\n",
                1.0 * fsdb_->GetDbEnv()->TotalTblBytesWritten() / 1024.0 /
                    fsdb_->GetDbEnv()->TotalTblWrites());
      }
      fprintf(stdout, " - Db stats: >>>\n%s\n", fsdb_->GetDbStats().c_str());
      fprintf(stdout, " - L0 stats: >>>\n%s\n",
              fsdb_->GetDbLevel0Events().c_str());
    }
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
  if (sig == SIGINT || sig == SIGTERM) {
    if (pdlfs::FLAGS_rank == 0) {
      fprintf(stdout, "\n");  // Start a newline
    }
    if (g_srvr) {
      g_srvr->Interrupt();
    }
  }
}

void BM_Main(int* const argc, char*** const argv) {
  pdlfs::FLAGS_skip_fs_checks = true;
  pdlfs::FLAGS_dbopts.enable_io_monitoring = true;
  pdlfs::FLAGS_dbopts.prefetch_compaction_input = true;
  pdlfs::FLAGS_dbopts.disable_write_ahead_logging = true;
  pdlfs::FLAGS_dbopts.use_default_logger = true;
  pdlfs::FLAGS_dbopts.ReadFromEnv();
  pdlfs::FLAGS_udp = true;

  for (int i = 1; i < (*argc); i++) {
    int n;
    char junk;
    if (sscanf((*argv)[i], "--info_port=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_info_port = n;
    } else if (sscanf((*argv)[i], "--rpc_worker_threads=%d%c", &n, &junk) ==
               1) {
      pdlfs::FLAGS_rpc_worker_threads = n;
    } else if (sscanf((*argv)[i], "--rpc_threads=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_rpc_threads = n;
    } else if (sscanf((*argv)[i], "--ports_per_rank=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_ports_per_rank = n;
    } else if (sscanf((*argv)[i], "--print_ips=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_print_ips = n;
    } else if (sscanf((*argv)[i], "--dummy_svr=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_dummy_svr = n;
    } else if (sscanf((*argv)[i], "--skip_fs_checks=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_skip_fs_checks = n;
    } else if (sscanf((*argv)[i], "--env_use_rados=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_env_use_rados = n;
    } else if (sscanf((*argv)[i], "--rados_force_syncio=%d%c", &n, &junk) ==
                   1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_rados_force_syncio = n;
    } else if (sscanf((*argv)[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_use_existing_fs = n;
    } else if (sscanf((*argv)[i], "--use_existing_fs=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_use_existing_fs = n;
    } else if (sscanf((*argv)[i], "--disable_compaction=%d%c", &n, &junk) ==
                   1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_dbopts.disable_compaction = n;
    } else if (sscanf((*argv)[i], "--prefetch_compaction_input=%d%c", &n,
                      &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_dbopts.prefetch_compaction_input = n;
    } else if (sscanf((*argv)[i], "--udp_sndbuf=%dK%c", &n, &junk) == 1) {
      pdlfs::FLAGS_udp_sndbuf = n << 10;
    } else if (sscanf((*argv)[i], "--udp_rcvbuf=%dK%c", &n, &junk) == 1) {
      pdlfs::FLAGS_udp_rcvbuf = n << 10;
    } else if (sscanf((*argv)[i], "--udp_max_msgsz=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_udp_max_msgsz = n;
    } else if (sscanf((*argv)[i], "--udp=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_udp = n;
    } else if (strncmp((*argv)[i], "--db=", 5) == 0) {
      pdlfs::FLAGS_db_prefix = (*argv)[i] + 5;
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

  std::string default_db_prefix;
  // Choose a prefix for the test db if none given with --db=<path>
  if (!pdlfs::FLAGS_db_prefix) {
    default_db_prefix = "/tmp/deltafs_bm";
    pdlfs::FLAGS_db_prefix = default_db_prefix.c_str();
  }

  pdlfs::Server svr;
  g_srvr = &svr;
  signal(SIGINT, &HandleSig);
  svr.Run();
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
