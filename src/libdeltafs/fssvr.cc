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
#include "fssvr.h"

namespace pdlfs {

FilesystemServerOptions::FilesystemServerOptions()
    : impl(rpc::kSocketRPC),
      num_rpc_worker_threads(0),
      num_rpc_threads(1),
      uri("udp://0.0.0.0:10086"),
      udp_max_incoming_msgsz(1432),
      udp_rcvbuf(-1),
      udp_sndbuf(-1),
      info_log(NULL) {}

FilesystemServer::FilesystemServer(  ///
    const FilesystemServerOptions& options)
    : options_(options),
      fs_(NULL),
      hmap_(NULL),
      rpc_workers_(NULL),
      rpc_(NULL) {
  hmap_ = new RequestHandler[rpc::kNumOps];
  memset(hmap_, 0, rpc::kNumOps * sizeof(void*));
  hmap_[rpc::kLokup] = Lokup;
  hmap_[rpc::kMkdir] = Mkdir;
  hmap_[rpc::kMkfle] = Mkfle;
  hmap_[rpc::kMkfls] = Mkfls;
  hmap_[rpc::kBukin] = Bukin;
  hmap_[rpc::kLstat] = Lstat;
  if (!options_.info_log) {
    options_.info_log = Logger::Default();
  }
}

void FilesystemServer::SetFs(FilesystemIf* const fs) {
  fs_ = fs;  ///
}

Status FilesystemServer::Call(Message& in, Message& out) RPCNOEXCEPT {
  if (in.contents.size() >= 4) {
    return hmap_[DecodeFixed32(&in.contents[0])](fs_, in, out);
  } else {
    return Status::InvalidArgument("Bad rpc req");
  }
}

rpc::If* FilesystemServer::TEST_CreateSelfCli() {
  if (rpc_) {
    std::string uri = rpc_->GetUri();
    size_t p = uri.find("0.0.0.0");
    if (p != std::string::npos) {
      uri.replace(p, strlen("0.0.0.0"), "127.0.0.1");
    }
    return rpc_->OpenStubFor(uri);
  } else {
    return NULL;
  }
}

rpc::If* FilesystemServer::TEST_CreateCli(const std::string& uri) {
  return rpc_ ? rpc_->OpenStubFor(uri) : NULL;
}

void FilesystemServer::TEST_Remap(int i, RequestHandler h) {
  hmap_[i] = h;  ///
}

FilesystemServer::~FilesystemServer() {
  delete rpc_;
  delete rpc_workers_;
  delete[] hmap_;
}

Status FilesystemServer::Close() {
  if (rpc_) return rpc_->Stop();
  return Status::OK();
}

int FilesystemServer::GetPort() const { return rpc_ ? rpc_->GetPort() : -1; }

std::string FilesystemServer::GetUsageInfo() const {
  if (rpc_) return rpc_->GetUsageInfo();
  return std::string();
}

Status FilesystemServer::OpenServer() {
  RPCOptions options;
  options.fs = this;
  options.impl = rpc::kSocketRPC;
  options.mode = rpc::kServerClient;
  if (options_.num_rpc_worker_threads) {
    rpc_workers_ = ThreadPool::NewFixed(options_.num_rpc_worker_threads);
  }
  options.extra_workers = rpc_workers_;
  options.num_rpc_threads = options_.num_rpc_threads;
  options.info_log = options_.info_log;
  options.uri = options_.uri;
  options.udp_max_unexpected_msgsz = options_.udp_max_incoming_msgsz;
  options.udp_srv_rcvbuf = options_.udp_rcvbuf;
  options.udp_srv_sndbuf = options_.udp_sndbuf;
  rpc_ = RPC::Open(options);
  Status status = rpc_->Start();
  if (status.ok()) {
#if VERBOSE >= 2
    Log(options_.info_log, 2, "Filesystem server is up: %s",
        rpc_->GetUri().c_str());
#endif
  }
  return status;
}

}  // namespace pdlfs
