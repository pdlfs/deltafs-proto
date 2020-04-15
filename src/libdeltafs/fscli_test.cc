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
#include "fs.h"
#include "fscomm.h"
#include "fsdb.h"

#include "pdlfs-common/testharness.h"

namespace pdlfs {

class FilesystemCliTest {
 public:
  FilesystemCliTest() : fsloc_(test::TmpDir() + "/fscli_test") {
    DestroyDB(fsloc_, DBOptions());
    fscli_ = new FilesystemCli(options_);
    me_.gid = me_.uid = 1;
  }

  ~FilesystemCliTest() {  ///
    delete fscli_;
  }

  Status Creat(const char* path) {
    return fscli_->Mkfle(me_, NULL, path, 0660, &ignored_);
  }

  Status Mkdir(const char* path) {
    return fscli_->Mkdir(me_, NULL, path, 0770, &ignored_);
  }

  Status Exist(const char* path) {
    return fscli_->Lstat(me_, NULL, path, &ignored_);
  }

  Stat ignored_;
  FilesystemOptions fsopts_;
  FilesystemCliOptions options_;
  FilesystemCli* fscli_;
  std::string fsloc_;
  User me_;
};

TEST(FilesystemCliTest, OpenAndClose) {
  ASSERT_OK(fscli_->OpenFilesystemCli(fsopts_, fsloc_));
  ASSERT_OK(fscli_->TEST_ProbeDir(DirId(0, 0)));
  ASSERT_OK(Exist("/"));
  ASSERT_OK(Exist("//"));
  ASSERT_OK(Exist("///"));
}

TEST(FilesystemCliTest, Files) {
  ASSERT_OK(fscli_->OpenFilesystemCli(fsopts_, fsloc_));
  ASSERT_OK(Creat("/1"));
  ASSERT_CONFLICT(Creat("/1"));
  ASSERT_OK(Exist("/1"));
  ASSERT_OK(Exist("//1"));
  ASSERT_OK(Exist("///1"));
  ASSERT_ERR(Exist("/1/"));
  ASSERT_ERR(Exist("//1//"));
  ASSERT_NOTFOUND(Exist("/2"));
  ASSERT_OK(Creat("/2"));
}

TEST(FilesystemCliTest, Dirs) {
  ASSERT_OK(fscli_->OpenFilesystemCli(fsopts_, fsloc_));
  ASSERT_OK(Mkdir("/1"));
  ASSERT_CONFLICT(Mkdir("/1"));
  ASSERT_CONFLICT(Creat("/1"));
  ASSERT_OK(Exist("/1"));
  ASSERT_OK(Exist("/1/"));
  ASSERT_OK(Exist("//1"));
  ASSERT_OK(Exist("//1//"));
  ASSERT_OK(Exist("///1"));
  ASSERT_OK(Exist("///1///"));
  ASSERT_NOTFOUND(Exist("/2"));
  ASSERT_OK(Mkdir("/2"));
}

TEST(FilesystemCliTest, Subdirs) {
  ASSERT_OK(fscli_->OpenFilesystemCli(fsopts_, fsloc_));
  ASSERT_OK(Mkdir("/1"));
  ASSERT_OK(Mkdir("/1/a"));
  ASSERT_CONFLICT(Mkdir("/1/a"));
  ASSERT_CONFLICT(Creat("/1/a"));
  ASSERT_OK(Exist("/1/a"));
  ASSERT_OK(Exist("/1/a/"));
  ASSERT_OK(Exist("//1//a"));
  ASSERT_OK(Exist("//1//a//"));
  ASSERT_OK(Exist("///1///a"));
  ASSERT_OK(Exist("///1///a///"));
  ASSERT_NOTFOUND(Exist("/1/b"));
  ASSERT_OK(Mkdir("/1/b"));
}

TEST(FilesystemCliTest, Resolv) {
  ASSERT_OK(fscli_->OpenFilesystemCli(fsopts_, fsloc_));
  ASSERT_OK(Mkdir("/1"));
  ASSERT_OK(Mkdir("/1/2"));
  ASSERT_OK(Mkdir("/1/2/3"));
  ASSERT_OK(Mkdir("/1/2/3/4"));
  ASSERT_OK(Mkdir("/1/2/3/4/5"));
  ASSERT_OK(Creat("/1/2/3/4/5/6"));
  ASSERT_OK(Exist("/1"));
  ASSERT_OK(Exist("/1/2"));
  ASSERT_OK(Exist("/1/2/3"));
  ASSERT_OK(Exist("/1/2/3/4"));
  ASSERT_OK(Exist("/1/2/3/4/5"));
  ASSERT_ERR(Exist("/1/2/3/4/5/6/"));
  ASSERT_ERR(Exist("/2/3"));
  ASSERT_ERR(Exist("/1/2/4/5"));
  ASSERT_ERR(Exist("/1/2/3/5"));
  ASSERT_ERR(Creat("/1/2/3/4/5/6/7"));
}

}  // namespace pdlfs

int main(int argc, char* argv[]) {
  return pdlfs::test::RunAllTests(&argc, &argv);
}
