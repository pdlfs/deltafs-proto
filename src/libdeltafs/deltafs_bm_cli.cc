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
#include "fscli.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/random.h"

#include <algorithm>
#include <arpa/inet.h>
#include <mpi.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdlib.h>
#include <vector>

namespace pdlfs {
namespace {
// Total number of ranks.
int FLAGS_comm_size = 1;

// My rank number.
int FLAGS_rank = 0;

// Uri for the information server.
const char* FLAGS_info_svr_uri = "tcp://127.0.0.1:10086";

// Print the ip addresses of all servers for debugging.
bool FLAGS_print_ips = true;

// Skip fs checks.
bool FLAGS_skip_fs_checks = true;

// Insert keys in random order.
bool FLAGS_random_order = true;

// Force all ranks to share a single parent directory.
bool FLAGS_share_dir = false;

// Number of files to insert per rank.
int FLAGS_num = 8;

// User id for the bench.
int FLAGS_uid = 1;

// Group id.
int FLAGS_gid = 1;

// A wrapper over our own random object.
struct STLRand {
  STLRand(int seed) : rnd(seed) {}
  int operator()(int i) { return rnd.Next() % i; }
  Random rnd;
};

// Per-rank work state.
struct RankState {
  FilesystemCliCtx ctx;
  std::vector<uint32_t> fids;
  std::string::size_type prefix_length;
  std::string pathbuf;
  Stat tmp;

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
    ctx.who.uid = FLAGS_uid;
    ctx.who.gid = FLAGS_gid;
  }
};

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

class Benchmark {
 private:
  FilesystemCli* fscli_;
  CompactUriMapper* uri_mapper_;
  std::string svr_map_;
  RPC* rpc_;

  static void PrintHeader() {
    fprintf(stdout, "Num ranks:          %d\n", FLAGS_comm_size);
    fprintf(stdout, "Fs info svr:        %s\n", FLAGS_info_svr_uri);
    fprintf(stdout, "Fs skip checks:     %d\n", FLAGS_skip_fs_checks);
  }

  static bool ParseMapData(  ///
      const Slice& svr_map, int* num_svrs, int* num_ports_per_svr) {
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

  void Open(int num_svrs, int num_ports_per_svr) {
    using namespace rpc;
    RPCOptions rpcopts;
    rpcopts.mode = kClientOnly;
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
    cliopts.skip_perm_checks = false;
    fscli_ = new FilesystemCli(cliopts);
    fscli_->RegisterFsSrvUris(rpc_, uri_mapper_, num_svrs, num_ports_per_svr);
  }

  void PrepareWrite(RankState* const state) {
    if (!FLAGS_share_dir || FLAGS_rank == 0) {
      Status s = fscli_->Mkdir(&state->ctx, NULL, state->pathbuf.c_str(), 0755,
                               &state->tmp);
      if (!s.ok()) {
        fprintf(stderr, "%d: Fail to mkdir: %s\n", FLAGS_rank,
                s.ToString().c_str());
        MPI_Finalize();
        exit(1);
      }
    }
  }

  void DoWrite(RankState* const state) {
    const uint64_t pid = uint64_t(FLAGS_rank) << 32;
    char tmp[30];
    for (int i = 0; i < FLAGS_num; i++) {
      Slice fname = Base64Enc(tmp, pid | state->fids[i]);
      state->pathbuf.resize(state->prefix_length);
      state->pathbuf.append(fname.data(), fname.size());
      Status s = fscli_->Mkfle(&state->ctx, NULL, state->pathbuf.c_str(), 0644,
                               &state->tmp);
      if (!s.ok()) {
        fprintf(stderr, "%d: Fail to mkfle: %s\n", FLAGS_rank,
                s.ToString().c_str());
        MPI_Finalize();
        exit(1);
      }
    }
  }

  void RunBenchmarks() {
    RankState state;
    PrepareWrite(&state);
    MPI_Barrier(MPI_COMM_WORLD);
    DoWrite(&state);
  }

 public:
  Benchmark() : fscli_(NULL), uri_mapper_(NULL), rpc_(NULL) {}

  ~Benchmark() {
    delete fscli_;
    delete uri_mapper_;
    delete rpc_;
  }

  void Run() {
    if (FLAGS_rank == 0) {
      PrintHeader();
    }
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
    RunBenchmarks();
  }
};

}  // namespace
}  // namespace pdlfs

namespace {
void Doit(int* const argc, char*** const argv) {
  for (int i = 1; i < (*argc); i++) {
    int n;
    char junk;
    if (sscanf((*argv)[i], "--print_ips=%d%c", &n, &junk) == 1) {
      pdlfs::FLAGS_print_ips = n;
    } else if (sscanf((*argv)[i], "--skip_fs_checks=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      pdlfs::FLAGS_skip_fs_checks = n;
    } else {
      if (pdlfs::FLAGS_rank == 0) {
        fprintf(stderr, "%s:\nInvalid flag: '%s'\n", (*argv)[0], (*argv)[i]);
      }
      MPI_Finalize();
      exit(1);
    }
  }

  pdlfs::Benchmark bench;
  bench.Run();
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
