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

#include "pdlfs-common/coding.h"
#include "pdlfs-common/port.h"

#include <arpa/inet.h>
#include <mpi.h>
#include <netinet/in.h>
#include <stdlib.h>

namespace pdlfs {
namespace {
// Total number of ranks.
int FLAGS_comm_size = 1;

// My rank number.
int FLAGS_rank = 0;

// Uri for the information server.
const char* FLAGS_info_svr_uri = "tcp://127.0.0.1:10086";

// Print the svr map for debugging purposes.
bool FLAGS_print_svr_map = true;

class Benchmark {
 private:
  FilesystemCli* fscli_;
  RPC* rpc_;

  static void PrintHeader() {
    //
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

  void Open(const Slice& svr_map, int num_svrs, int num_ports_per_svr) {
    using namespace rpc;
    RPCOptions rpcopts;
    rpcopts.mode = kClientOnly;
    rpcopts.uri = "udp://-1:-1";
    rpc_ = RPC::Open(rpcopts);
    FilesystemCliOptions cliopts;
    fscli_ = new FilesystemCli(cliopts);
    const unsigned short* const port_info =
        reinterpret_cast<const unsigned short*>(&svr_map[0]);
    const unsigned* const ip_info = reinterpret_cast<const unsigned*>(
        &svr_map[2 * num_ports_per_svr * num_svrs]);
    if (FLAGS_rank == 0 && FLAGS_print_svr_map) {
      struct in_addr tmp_addr;
      puts("Dumping fs uri(s) >>>");
      for (int i = 0; i < num_svrs; i++) {
        tmp_addr.s_addr = ip_info[i];
        for (int j = 0; j < num_ports_per_svr; j++) {
          fprintf(stdout, "%s:%hu\n", inet_ntoa(tmp_addr),
                  port_info[j + i * num_ports_per_svr]);
        }
        fflush(stdout);
      }
    }
  }

 public:
  Benchmark() : fscli_(NULL), rpc_(NULL) {}

  ~Benchmark() {
    delete fscli_;
    delete rpc_;
  }

  void Run() {
    if (FLAGS_rank == 0) {
      PrintHeader();
    }
    uint32_t svr_map_size;
    std::string svr_map;
    if (FLAGS_rank != 0) {  // Non-roots get data from the root
      MPI_Bcast(&svr_map_size, 1, MPI_UINT32_T, 0, MPI_COMM_WORLD);
      svr_map.resize(svr_map_size);
      MPI_Bcast(&svr_map[0], svr_map_size, MPI_CHAR, 0, MPI_COMM_WORLD);
    } else {  // Root fetches data from remote and broadcasts it to non-roots
      ObtainSvrMap(&svr_map);
      svr_map_size = svr_map.size();
      MPI_Bcast(&svr_map_size, 1, MPI_UINT32_T, 0, MPI_COMM_WORLD);
      MPI_Bcast(&svr_map[0], svr_map_size, MPI_CHAR, 0, MPI_COMM_WORLD);
    }
    int num_ports_per_svr;
    int num_svrs;
    if (!ParseMapData(svr_map, &num_svrs, &num_ports_per_svr)) {
      if (FLAGS_rank == 0) {
        fprintf(stderr, "Cannot parse svr map\n");
      }
      MPI_Finalize();
      exit(1);
    }
    Open(svr_map, num_svrs, num_ports_per_svr);
  }
};

}  // namespace
}  // namespace pdlfs

namespace {
void Doit(int* const argc, char*** const argv) {
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