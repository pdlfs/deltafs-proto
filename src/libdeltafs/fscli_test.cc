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

#include "pdlfs-common/leveldb/db.h"
#include "pdlfs-common/leveldb/options.h"

#include "pdlfs-common/testharness.h"

namespace pdlfs {

class FilesystemCliTest {
 public:
  typedef FilesystemCli::BAT BATCH;
  typedef FilesystemCli::AT AT;
  FilesystemCliTest()
      : fsdb_(NULL),
        fs_(NULL),
        fscli_(NULL),
        fsloc_(test::TmpDir() + "/fscli_test") {
    DestroyDB(fsloc_, DBOptions());
    me_.gid = me_.uid = 1;
  }

  Status OpenFilesystemCli() {
    fsdb_ = new FilesystemDb(fsdbopts_);
    Status s = fsdb_->Open(fsloc_);
    if (s.ok()) {
      fscli_ = new FilesystemCli(fscliopts_);
      fs_ = new Filesystem(fsopts_);
      fscli_->SetLocalFs(fs_);
      fs_->SetDb(fsdb_);
    }
    return s;
  }

  ~FilesystemCliTest() {
    delete fscli_;
    delete fs_;
    delete fsdb_;
  }

  Status Atdir(const char* path, AT** result, const AT* at = NULL) {
    return fscli_->Atdir(me_, at, path, result);
  }

  Status Creat(const char* path, const AT* at = NULL) {
    return fscli_->Mkfle(me_, at, path, 0660, &tmp_);
  }

  Status Mkdir(const char* path, const AT* at = NULL) {
    return fscli_->Mkdir(me_, at, path, 0770, &tmp_);
  }

  Status Exist(const char* path, const AT* at = NULL) {
    return fscli_->Lstat(me_, at, path, &tmp_);
  }

  Status BatchStart(const char* path, BATCH** result, const AT* at = NULL) {
    return fscli_->BatchStart(me_, at, path, result);
  }

  Status BatchInsert(const char* name, BATCH* batch) {
    return fscli_->BatchInsert(batch, name);
  }

  Status BatchCommit(BATCH* batch) {  ///
    return fscli_->BatchCommit(batch);
  }

  Status BatchEnd(BATCH* batch) {  ///
    return fscli_->BatchEnd(batch);
  }

  Stat tmp_;
  FilesystemDbOptions fsdbopts_;
  FilesystemDb* fsdb_;
  FilesystemOptions fsopts_;
  Filesystem* fs_;
  FilesystemCliOptions fscliopts_;
  FilesystemCli* fscli_;
  std::string fsloc_;
  User me_;
};

TEST(FilesystemCliTest, OpenAndClose) {
  ASSERT_OK(OpenFilesystemCli());
  ASSERT_OK(fscli_->TEST_ProbeDir(DirId(0)));
  ASSERT_EQ(fscli_->TEST_TotalDirsInMemory(), 0);
  ASSERT_OK(fscli_->TEST_ProbePartition(DirId(0), 0));
  ASSERT_EQ(fscli_->TEST_TotalPartitionsInMemory(), 1);
  ASSERT_EQ(fscli_->TEST_TotalDirsInMemory(), 1);
}

TEST(FilesystemCliTest, Files) {
  ASSERT_OK(OpenFilesystemCli());
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
  ASSERT_OK(OpenFilesystemCli());
  ASSERT_OK(Exist("/"));
  ASSERT_OK(Exist("//"));
  ASSERT_OK(Exist("///"));
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
  ASSERT_OK(OpenFilesystemCli());
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
  ASSERT_OK(OpenFilesystemCli());
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

TEST(FilesystemCliTest, Atdir) {
  ASSERT_OK(OpenFilesystemCli());
  AT *d0, *d1, *d2, *d3, *d4, *d5;
  ASSERT_OK(Atdir("//", &d0));
  ASSERT_OK(Mkdir("/1", d0));
  ASSERT_OK(Exist("/1", d0));
  ASSERT_OK(Atdir("/1", &d1, d0));
  ASSERT_OK(Mkdir("/2", d1));
  ASSERT_OK(Exist("/2", d1));
  ASSERT_OK(Atdir("/2", &d2, d1));
  ASSERT_OK(Mkdir("/3", d2));
  ASSERT_OK(Exist("/3", d2));
  ASSERT_OK(Atdir("/3", &d3, d2));
  ASSERT_OK(Mkdir("/4", d3));
  ASSERT_OK(Exist("/4", d3));
  ASSERT_OK(Atdir("/4", &d4, d3));
  ASSERT_OK(Mkdir("/5", d4));
  ASSERT_OK(Exist("/5", d4));
  ASSERT_OK(Atdir("/5", &d5, d4));
  ASSERT_OK(Creat("/a", d5));
  ASSERT_OK(Creat("/b", d5));
  ASSERT_OK(Creat("/c", d5));
  ASSERT_OK(Creat("/d", d5));
  ASSERT_OK(Creat("/e", d5));
  ASSERT_OK(Creat("/f", d5));
  ASSERT_OK(Exist("/1/2/3/4/5/a", d0));
  ASSERT_OK(Exist("/2/3/4/5/b", d1));
  ASSERT_OK(Exist("/3/4/5/c", d2));
  ASSERT_OK(Exist("/4/5/d", d3));
  ASSERT_OK(Exist("/5/e", d4));
  ASSERT_OK(Exist("/f", d5));
  fscli_->Destroy(d5);
  fscli_->Destroy(d4);
  fscli_->Destroy(d3);
  fscli_->Destroy(d2);
  fscli_->Destroy(d1);
}

TEST(FilesystemCliTest, BatchCtx) {
  ASSERT_OK(OpenFilesystemCli());
  BATCH *bat, *bat1, *bat2;
  ASSERT_OK(Mkdir("/a"));
  ASSERT_OK(BatchStart("/a", &bat));
  ASSERT_EQ(fscli_->TEST_TotalLeasesAtPartition(DirId(0), 0), 1);
  ASSERT_ERR(Mkdir("/a/1"));
  ASSERT_ERR(Exist("/a/2"));
  ASSERT_ERR(Creat("/a/3"));
  ASSERT_OK(BatchEnd(bat));
  ASSERT_EQ(fscli_->TEST_TotalLeasesAtPartition(DirId(0), 0), 0);
  ASSERT_NOTFOUND(BatchStart("/b", &bat));
  ASSERT_OK(Mkdir("/c"));
  ASSERT_OK(BatchStart("/c", &bat1));
  ASSERT_OK(BatchStart("/c", &bat2));
  ASSERT_OK(BatchEnd(bat1));
  ASSERT_OK(BatchEnd(bat2));
}

TEST(FilesystemCliTest, BatchCreats) {
  ASSERT_OK(OpenFilesystemCli());
  BATCH* bat;
  ASSERT_OK(Mkdir("/a"));
  ASSERT_OK(BatchStart("/a", &bat));
  ASSERT_OK(BatchInsert("1", bat));
  ASSERT_ERR(Exist("/a/1"));
  ASSERT_OK(BatchInsert("2", bat));
  ASSERT_ERR(Exist("/a/2"));
  ASSERT_OK(BatchInsert("3", bat));
  ASSERT_ERR(Exist("/a/3"));
  ASSERT_OK(BatchCommit(bat));
  ASSERT_ERR(BatchInsert("4", bat));
  ASSERT_OK(BatchCommit(bat));
  ASSERT_OK(BatchEnd(bat));
  ASSERT_OK(Exist("/a/1"));
  ASSERT_OK(Exist("/a/2"));
  ASSERT_OK(Exist("/a/3"));
}

}  // namespace pdlfs

int main(int argc, char* argv[]) {
  return pdlfs::test::RunAllTests(&argc, &argv);
}
