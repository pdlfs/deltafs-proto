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
    : impl(rpc::kSocketRPC), num_rpc_threads(1), uri(":10086") {}

FilesystemServer::FilesystemServer(  ///
    const FilesystemServerOptions& options)
    : options_(options), fs_(NULL), hmap_(NULL), rpc_(NULL) {
  hmap_ = new RequestHandler[kNumOps];
  memset(hmap_, 0, kNumOps * sizeof(void*));
  hmap_[kLokup] = Lokup;
  hmap_[kMkdir] = Mkdir;
  hmap_[kMkfle] = Mkfle;
  hmap_[kMkfls] = Mkfls;
  hmap_[kLstat] = Lstat;
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
  delete[] hmap_;
}

Status FilesystemServer::Close() {
  if (rpc_) return rpc_->Stop();
  return Status::OK();
}

Status FilesystemServer::OpenServer() {
  RPCOptions options;
  options.fs = this;
  options.impl = rpc::kSocketRPC;
  options.mode = rpc::kServerClient;
  options.num_rpc_threads = options_.num_rpc_threads;
  options.uri = options_.uri;
  rpc_ = RPC::Open(options);
  return rpc_->Start();
}

}  // namespace pdlfs
