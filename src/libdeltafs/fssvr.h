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
#pragma once

#include "fscom.h"

#include "pdlfs-common/rpc.h"

namespace pdlfs {

struct FilesystemServerOptions {
  FilesystemServerOptions();
  rpc::Engine impl;            // RPC impl selector. Default: rpc::kSocketRPC
  int num_rpc_worker_threads;  // Default: 0
  int num_rpc_threads;         // Default: 1
  // Implementation-specific initialization string for RPC.
  // Default: udp://0.0.0.0:10086
  std::string uri;
  // Max incoming message size for UDP.
  // Default: 1432
  size_t udp_max_incoming_msgsz;
  // SO_RCVBUF and SO_SNDBUF for UDP. Set to -1 to use system defaults.
  int udp_rcvbuf;  // Default: -1
  int udp_sndbuf;  // Default: -1
  // Logger object for progressing/error information.
  // Default: NULL, which causes Logger::Default() to be used.
  Logger* info_log;
};

// Each filesystem server acts as a request dispatcher. An embedded rpc server
// handles network communication and listens to incoming requests. Each client
// request received by the embedded rpc server is passed to the filesystem
// server for processing. The filesystem server processes such a request by
// routing it to a corresponding request handler for handling. Requests are
// routed according to a routing table established at the time of server
// initialization.
class FilesystemServer : public rpc::If {
 public:
  explicit FilesystemServer(const FilesystemServerOptions& options);
  virtual Status Call(Message& in, Message& out) RPCNOEXCEPT;
  virtual ~FilesystemServer();

  void SetFs(FilesystemIf* fs);
  Status OpenServer();
  Status Close();

  std::string GetUsageInfo() const;
  int GetPort() const;
  If* TEST_CreateSelfCli();  // Create a client that connects the server itself
  If* TEST_CreateCli(const std::string& uri);
  typedef Status (*RequestHandler)(FilesystemIf*, Message& in, Message& out);
  // Reset the handler for a specific type of operations.
  void TEST_Remap(int i, RequestHandler h);

 private:
  // No copying allowed
  void operator=(const FilesystemServer&);
  FilesystemServer(const FilesystemServer& other);
  FilesystemServerOptions options_;
  FilesystemIf* fs_;  // Not owned by us
  RequestHandler* hmap_;
  ThreadPool* rpc_workers_;
  RPC* rpc_;
};

}  // namespace pdlfs
