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
#include "fsis.h"
#include "fssvr.h"

#include "pdlfs-common/leveldb/db.h"
#include "pdlfs-common/leveldb/options.h"

#include "pdlfs-common/mutexlock.h"
#include "pdlfs-common/port.h"

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <ifaddrs.h>
#include <mpi.h>
#include <netdb.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <time.h>
#include <vector>

namespace pdlfs {
namespace {
// Db options.
FilesystemDbOptions FLAGS_dbopts;

// Total number of ranks.
int FLAGS_comm_size = 1;

// My rank number.
int FLAGS_rank = 0;

// If a host is configured with 1+ ip addresses, use the one with the following
// prefix.
const char* FLAGS_ip_prefix = "127.0.0.1";

// Listening port for the information server.
int FLAGS_info_port = 10086;

// Number of listening ports per rank.
int FLAGS_ports_per_rank = 1;

// Print the ip addresses of all ranks for debugging.
bool FLAGS_print_ips = false;

// Skip all fs checks.
bool FLAGS_skip_fs_checks = false;

// If true, do not destroy the existing database.
bool FLAGS_use_existing_db = false;

// Use the db at the following prefix.
const char* FLAGS_db_prefix = NULL;

class Server {
 private:
  port::Mutex mu_;
  port::AtomicPointer shutting_down_;
  port::CondVar cv_;
  FilesystemInfoServer* infosvr_;
  std::vector<FilesystemServer*> svrs_;
  Filesystem* fs_;
  FilesystemDb* fsdb_;

  static void PrintHeader() {
    PrintEnvironment();
    PrintWarnings();
    fprintf(stdout, "Num ports per rank: %d\n", FLAGS_ports_per_rank);
    fprintf(stdout, "Num ranks:          %d\n", FLAGS_comm_size);
    fprintf(stdout, "Fs info port:       %d\n", FLAGS_info_port);
    fprintf(stdout, "Use ip:             %s*\n", FLAGS_ip_prefix);
    fprintf(stdout, "Use existing db:    %d\n", FLAGS_use_existing_db);
    fprintf(stdout, "Db: %s/r<rank>\n", FLAGS_db_prefix);
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

  const char* PickAddr(char* dst) {
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

  FilesystemIf* OpenFilesystem() {
    Env* const env = Env::Default();
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
    fs_ = new Filesystem(opts);
    fs_->SetDb(fsdb_);
    return fs_;
  }

  FilesystemInfoServer* OpenInfoPort(const char* ip, int port) {
    FilesystemInfoServerOptions infosvropts;
    infosvropts.num_rpc_threads = 1;
    char uri[50];
    snprintf(uri, sizeof(uri), "%s:%d", ip, port);
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

  FilesystemServer* OpenPort(const char* ip, FilesystemIf* fs) {
    FilesystemServerOptions svropts;
    svropts.num_rpc_threads = 1;
    svropts.uri = "udp://";
    svropts.uri += ip;
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
        fsdb_(NULL) {}

  ~Server() {
    int n = svrs_.size();
    for (int i = 0; i < n; i++) {
      delete svrs_[i];
    }
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
    char ip_str[INET_ADDRSTRLEN];
    memset(ip_str, 0, sizeof(ip_str));
    const unsigned int myip = inet_addr(PickAddr(ip_str));
    int np = FLAGS_ports_per_rank;
    std::vector<unsigned short> myports;
    for (int i = 0; i < np; i++) {
      FilesystemServer* svr = OpenPort(ip_str, fs);
      myports.push_back(svr->GetPort());
      svrs_.push_back(svr);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    unsigned int* ip_info = NULL;
    if (FLAGS_rank == 0) {
      ip_info = new unsigned int[FLAGS_comm_size];
    }
    MPI_Gather(&myip, 1, MPI_UNSIGNED, ip_info, 1, MPI_UNSIGNED, 0,
               MPI_COMM_WORLD);
    if (FLAGS_rank == 0) {
      infosvr_ = OpenInfoPort(ip_str, FLAGS_info_port);
      infosvr_->SetInfo(  ///
          0, Slice(reinterpret_cast<char*>(ip_info),
                   FLAGS_comm_size * sizeof(unsigned int)));
      if (FLAGS_print_ips) {
        struct in_addr tmp_addr;
        puts("Dumping fs metadata svc uris >>>");
        for (int i = 0; i < FLAGS_comm_size; i++) {
          tmp_addr.s_addr = ip_info[i];
          fprintf(stdout, "%-5d: %s\n", i, inet_ntoa(tmp_addr));
        }
      }
      puts("Running...");
    }
    MutexLock ml(&mu_);
    while (!shutting_down_.Acquire_Load()) {
      cv_.Wait();
    }
    infosvr_->Close();
    delete[] ip_info;
    for (int i = 0; i < FLAGS_ports_per_rank; i++) {
      svrs_[i]->Close();
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
  if (sig == SIGINT || sig == SIGTERM) {
    if (pdlfs::FLAGS_rank == 0) {
      fprintf(stdout, "\n");  // Start a newline
    }
    if (g_srvr) {
      g_srvr->Interrupt();
    }
  }
}

void Doit(int* const argc, char*** const argv) {
  pdlfs::FLAGS_dbopts.ReadFromEnv();

  for (int i = 1; i < (*argc); i++) {
    int n;
    char junk;
    if (sscanf((*argv)[i], "--info_port=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_info_port = n;
    } else if (sscanf((*argv)[i], "--ports_per_rank=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_ports_per_rank = n;
    } else if (sscanf((*argv)[i], "--print_ips=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_print_ips = n;
    } else if (sscanf((*argv)[i], "--skip_fs_checks=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_skip_fs_checks = n;
    } else if (sscanf((*argv)[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_use_existing_db = n;
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
    default_db_prefix = "/tmp/deltafs_srvr";
    pdlfs::FLAGS_db_prefix = default_db_prefix.c_str();
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
