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
#include "fsis.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

namespace pdlfs {
class FilesystemInfoServerTest {
 public:
  FilesystemInfoServerTest()  ///
      : srv_(new FilesystemInfoServer(options_)) {}
  virtual ~FilesystemInfoServerTest() {  ///
    delete srv_;
  }

  FilesystemInfoServerOptions options_;
  FilesystemInfoServer* srv_;
};

TEST(FilesystemInfoServerTest, StartAndStop) {
  ASSERT_OK(srv_->OpenServer());
  ASSERT_OK(srv_->Close());
}

TEST(FilesystemInfoServerTest, EmptyItem) {
  ASSERT_OK(srv_->OpenServer());
  rpc::If* const cli = srv_->TEST_CreateSelfCli();
  rpc::If::Message in, out;
  EncodeFixed32(&in.buf[0], 0);
  in.contents = Slice(&in.buf[0], sizeof(uint32_t));
  ASSERT_OK(cli->Call(in, out));
  ASSERT_TRUE(out.contents.empty());
  ASSERT_OK(srv_->Close());
  delete cli;
}

TEST(FilesystemInfoServerTest, VeryLargeItem) {
  ASSERT_OK(srv_->OpenServer());
  std::string biginfo;
  Random rnd(test::RandomSeed());
  srv_->SetInfo(0, test::RandomString(&rnd, 1000 * 1000, &biginfo));
  rpc::If* const cli = srv_->TEST_CreateSelfCli();
  rpc::If::Message in, out;
  EncodeFixed32(&in.buf[0], 0);
  in.contents = Slice(&in.buf[0], sizeof(uint32_t));
  ASSERT_OK(cli->Call(in, out));
  ASSERT_EQ(out.contents.size(), biginfo.size());
  ASSERT_EQ(out.contents, biginfo);
  ASSERT_OK(srv_->Close());
  delete cli;
}

TEST(FilesystemInfoServerTest, Mapping) {
  ASSERT_OK(srv_->OpenServer());
  srv_->SetInfo(0, "12345");
  srv_->SetInfo(1, "23456");
  srv_->SetInfo(2, "34567");
  rpc::If* const cli = srv_->TEST_CreateSelfCli();
  rpc::If::Message in, out;
  EncodeFixed32(&in.buf[0], 1);
  in.contents = Slice(&in.buf[0], sizeof(uint32_t));
  ASSERT_OK(cli->Call(in, out));
  ASSERT_EQ(out.contents.size(), 5);
  ASSERT_EQ(out.contents, "23456");
  ASSERT_OK(srv_->Close());
  delete cli;
}

}  // namespace pdlfs

int main(int argc, char* argv[]) {
  return pdlfs::test::RunAllTests(&argc, &argv);
}
