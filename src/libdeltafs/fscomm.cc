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
#include "fscomm.h"

#include "pdlfs-common/coding.h"

namespace pdlfs {

namespace {
// clang-format off
char* EncodeLookupStat(char* dst, const LookupStat& stat) {
  EncodeFixed64(dst, stat.DnodeNo());       dst += 8;
  EncodeFixed64(dst, stat.InodeNo());       dst += 8;
  EncodeFixed64(dst, stat.LeaseDue());      dst += 8;
  EncodeFixed32(dst, stat.ZerothServer());  dst += 4;
  EncodeFixed32(dst, stat.DirMode());       dst += 4;
  EncodeFixed32(dst, stat.UserId());        dst += 4;
  EncodeFixed32(dst, stat.GroupId());       dst += 4;
  return dst;
}

char* EncodeStat(char* dst, const Stat& stat) {
  EncodeFixed64(dst, stat.DnodeNo());       dst += 8;
  EncodeFixed64(dst, stat.InodeNo());       dst += 8;
  EncodeFixed32(dst, stat.FileMode());      dst += 4;
  EncodeFixed32(dst, stat.UserId());        dst += 4;
  EncodeFixed32(dst, stat.GroupId());       dst += 4;
  return dst;
}

char* EncodeUser(char* dst, const User& u) {
  EncodeFixed32(dst, u.uid);  dst += 4;
  EncodeFixed32(dst, u.gid);  dst += 4;
  return dst;
}

bool GetLookupStat(Slice* input, LookupStat* stat) {
  if (input->size() < 40) return false;
  const char* p = input->data();
  stat->SetDnodeNo(DecodeFixed64(p));       p += 8;
  stat->SetInodeNo(DecodeFixed64(p));       p += 8;
  stat->SetLeaseDue(DecodeFixed64(p));      p += 8;
  stat->SetZerothServer(DecodeFixed32(p));  p += 4;
  stat->SetDirMode(DecodeFixed32(p));       p += 4;
  stat->SetUserId(DecodeFixed32(p));        p += 4;
  stat->SetGroupId(DecodeFixed32(p));
  input->remove_prefix(40);
  return true;
}

bool GetStat(Slice* input, Stat* stat) {
  if (input->size() < 28) return false;
  const char* p = input->data();
  stat->SetDnodeNo(DecodeFixed64(p));       p += 8;
  stat->SetInodeNo(DecodeFixed64(p));       p += 8;
  stat->SetFileMode(DecodeFixed32(p));      p += 4;
  stat->SetUserId(DecodeFixed32(p));        p += 4;
  stat->SetGroupId(DecodeFixed32(p));
  input->remove_prefix(28);
  return true;
}

bool GetUser(Slice* input, User* u) {
  if (input->size() < 8) return false;
  const char* p = input->data();
  u->uid = DecodeFixed32(p);  p += 4;
  u->gid = DecodeFixed32(p);
  input->remove_prefix(8);
  return true;
}

bool GetFixed32(Slice* input, uint32_t* op) {
  if (input->size() < 4) return false;
  *op = DecodeFixed32(input->data());
  input->remove_prefix(4);
  return true;
}
}  // namespace

// clang-format on
namespace rpc {
Status MkdirOperation::operator()(If::Message& in, If::Message& out) {
  Status s;
  uint32_t op;
  MkdirOptions options;
  LookupStat pa;
  Stat stat;
  Slice input = in.contents;
  if (!GetFixed32(&input, &op) || !GetLookupStat(&input, &pa) ||
      !GetLengthPrefixedSlice(&input, &options.name) ||
      !GetUser(&input, &options.me) || !GetFixed32(&input, &options.mode)) {
    s = Status::InvalidArgument("Wrong mkdir input");
  } else {
    s = fs_->Mkdir(options.me, pa, options.name, options.mode, &stat);
    char* dst = &out.buf[0];
    EncodeFixed32(dst, s.err_code());
    char* p = dst + 4;
    if (s.ok()) {
      p = EncodeStat(p, stat);
    }
    out.contents = Slice(dst, p - dst);
  }
  return s;
}

Status MkdirCli::operator()(  ///
    const MkdirOptions& options, MkdirRet* ret) {
  Status s;
  If::Message in;
  char* dst = &in.buf[0];
  EncodeFixed32(dst, kMkdir);
  char* p = dst + 4;
  p = EncodeLookupStat(p, *options.parent);
  p = EncodeLengthPrefixedSlice(p, options.name);
  p = EncodeUser(p, options.me);
  EncodeFixed32(p, options.mode);
  p += 4;
  in.contents = Slice(dst, p - dst);
  If::Message out;
  uint32_t rv;
  s = rpc_->Call(in, out);
  if (!s.ok()) {
    return s;
  }
  Slice input = out.contents;
  if (!GetFixed32(&input, &rv)) {
    return Status::Corruption("Bad rpc reply header");
  } else if (rv != 0) {
    return Status::FromCode(rv);
  } else if (!GetStat(&input, ret->stat)) {
    return Status::Corruption("Bad rpc reply");
  } else {
    return s;
  }
}
}  // namespace rpc
Status Mkdir(FilesystemIf* fs, rpc::If::Message& in, rpc::If::Message& out) {
  return rpc::MkdirOperation(fs)(in, out);
}

}  // namespace pdlfs
