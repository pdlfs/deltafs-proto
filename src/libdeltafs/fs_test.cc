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

#include "pdlfs-common/leveldb/db.h"
#include "pdlfs-common/leveldb/options.h"

#include "pdlfs-common/testharness.h"

namespace pdlfs {

class FilesystemTest {
 public:
  FilesystemTest()
      : fsdb_(NULL), fs_(NULL), fsloc_(test::TmpDir() + "/fs_test") {
    me_.gid = me_.uid = 1;
    dirmode_ = 0777;
    due_ = -1;
  }

  Status OpenFilesystem() {
    Status s = ReopenFilesystem(fsloc_);
    return s;
  }

  Status ReopenFilesystem(const std::string& fsloc) {
    delete fs_;
    fs_ = NULL;
    delete fsdb_;
    fsdb_ = NULL;
    DestroyDB(fsloc, DBOptions());
    Env* env = Env::GetUnBufferedIoEnv();
    fsdb_ = new FilesystemDb(fsdbopts_, env);
    Status s = fsdb_->Open(fsloc);
    if (s.ok()) {
      fs_ = new Filesystem(fsopts_);
      fs_->SetDb(fsdb_);
    }
    return s;
  }

  ~FilesystemTest() {
    delete fs_;
    delete fsdb_;
  }

  Status Exist(uint64_t dir_id, const std::string& name) {
    LookupStat p;
    p.SetDnodeNo(0);
    p.SetInodeNo(dir_id);
    p.SetZerothServer(0);
    p.SetDirMode(dirmode_);
    p.SetUserId(0);
    p.SetGroupId(0);
    p.SetLeaseDue(due_);
    p.AssertAllSet();
    Stat tmp;
    return fs_->Lstat(me_, p, name, &tmp);
  }

  Status BulkIn(uint64_t dir_id, const std::string& table_dir) {
    LookupStat p;
    p.SetDnodeNo(0);
    p.SetInodeNo(dir_id);
    p.SetZerothServer(0);
    p.SetDirMode(dirmode_);
    p.SetUserId(0);
    p.SetGroupId(0);
    p.SetLeaseDue(due_);
    p.AssertAllSet();
    return fs_->Bukin(me_, p, table_dir);
  }

  Status BatchedCreat(uint64_t dir_id, const std::string& namearr,
                      uint32_t* n) {
    LookupStat p;
    p.SetDnodeNo(0);
    p.SetInodeNo(dir_id);
    p.SetZerothServer(0);
    p.SetDirMode(dirmode_);
    p.SetUserId(0);
    p.SetGroupId(0);
    p.SetLeaseDue(due_);
    p.AssertAllSet();
    Stat tmp;
    return fs_->Mkfls(me_, p, namearr, 0660, n);
  }

  Status Creat(uint64_t dir_id, const std::string& name) {
    LookupStat p;
    p.SetDnodeNo(0);
    p.SetInodeNo(dir_id);
    p.SetZerothServer(0);
    p.SetDirMode(dirmode_);
    p.SetUserId(0);
    p.SetGroupId(0);
    p.SetLeaseDue(due_);
    p.AssertAllSet();
    Stat tmp;
    return fs_->Mkfle(me_, p, name, 0660, &tmp);
  }

  uint32_t dirmode_;
  uint64_t due_;
  FilesystemDbOptions fsdbopts_;
  FilesystemDb* fsdb_;
  FilesystemOptions fsopts_;
  Filesystem* fs_;
  std::string fsloc_;
  User me_;
};

TEST(FilesystemTest, OpenAndClose) {
  ASSERT_OK(OpenFilesystem());
  FilesystemDir* dir = fs_->TEST_ProbeDir(DirId(0));
  ASSERT_TRUE(dir != NULL);
  fs_->TEST_Release(dir);
  ASSERT_EQ(fs_->TEST_TotalDirsInMemory(), 1);
}

TEST(FilesystemTest, Files) {
  ASSERT_OK(OpenFilesystem());
  ASSERT_OK(Creat(0, "a"));
  ASSERT_OK(Creat(0, "b"));
  ASSERT_OK(Creat(0, "c"));
  ASSERT_OK(Exist(0, "a"));
  ASSERT_OK(Exist(0, "b"));
  ASSERT_OK(Exist(0, "c"));
  ASSERT_EQ(fs_->TEST_LastIno(), 3);
}

TEST(FilesystemTest, DuplicateNames) {
  ASSERT_OK(OpenFilesystem());
  ASSERT_OK(Creat(0, "a"));
  ASSERT_CONFLICT(Creat(0, "a"));
  ASSERT_OK(Creat(0, "b"));
  ASSERT_EQ(fs_->TEST_LastIno(), 2);
}

TEST(FilesystemTest, NoDupChecks) {
  fsopts_.skip_name_collision_checks = true;
  ASSERT_OK(OpenFilesystem());
  ASSERT_OK(Creat(0, "a"));
  ASSERT_OK(Creat(0, "a"));
}

TEST(FilesystemTest, LeaseExpired) {
  due_ = 0;
  ASSERT_OK(OpenFilesystem());
  ASSERT_ERR(Creat(0, "a"));
}

TEST(FilesystemTest, NoLeaseDueChecks) {
  fsopts_.skip_lease_due_checks = true;
  due_ = 0;
  ASSERT_OK(OpenFilesystem());
  ASSERT_OK(Creat(0, "a"));
}

TEST(FilesystemTest, AccessDenied) {
  dirmode_ = 0770;
  ASSERT_OK(OpenFilesystem());
  ASSERT_ERR(Creat(0, "a"));
}

TEST(FilesystemTest, NoPermissionChecks) {
  fsopts_.skip_perm_checks = true;
  dirmode_ = 0770;
  ASSERT_OK(OpenFilesystem());
  ASSERT_OK(Creat(0, "a"));
}

TEST(FilesystemTest, BatchedCreats) {
  ASSERT_OK(OpenFilesystem());
  std::string namearr;
  PutLengthPrefixedSlice(&namearr, "a");
  PutLengthPrefixedSlice(&namearr, "b");
  PutLengthPrefixedSlice(&namearr, "c");
  PutLengthPrefixedSlice(&namearr, "d");
  PutLengthPrefixedSlice(&namearr, "e");
  uint32_t n = 5;
  ASSERT_OK(BatchedCreat(0, namearr, &n));
  ASSERT_EQ(n, 5);
  ASSERT_OK(Exist(0, "a"));
  ASSERT_OK(Exist(0, "b"));
  ASSERT_OK(Exist(0, "c"));
  ASSERT_OK(Exist(0, "d"));
  ASSERT_OK(Exist(0, "e"));
  ASSERT_EQ(fs_->TEST_LastIno(), 5);
}

TEST(FilesystemTest, ErrorInBatch) {
  ASSERT_OK(OpenFilesystem());
  std::string namearr;
  PutLengthPrefixedSlice(&namearr, "a");
  PutLengthPrefixedSlice(&namearr, "b");
  PutLengthPrefixedSlice(&namearr, "c");
  PutLengthPrefixedSlice(&namearr, "a");
  PutLengthPrefixedSlice(&namearr, "e");
  uint32_t n = 5;
  ASSERT_CONFLICT(BatchedCreat(0, namearr, &n));
  ASSERT_EQ(n, 3);
  ASSERT_OK(Exist(0, "a"));
  ASSERT_OK(Exist(0, "b"));
  ASSERT_OK(Exist(0, "c"));
  ASSERT_EQ(fs_->TEST_LastIno(), 3);
  ASSERT_CONFLICT(Creat(0, "a"));
  ASSERT_CONFLICT(Creat(0, "b"));
  ASSERT_CONFLICT(Creat(0, "c"));
  ASSERT_OK(Creat(0, "d"));
  ASSERT_EQ(fs_->TEST_LastIno(), 4);
}

TEST(FilesystemTest, EmptyBulkIn) {
  ASSERT_OK(OpenFilesystem());
  ASSERT_OK(ReopenFilesystem(fsloc_ + "/fs2"));
  ASSERT_OK(BulkIn(0, fsloc_));
}

TEST(FilesystemTest, BulkIn) {
  ASSERT_OK(OpenFilesystem());
  ASSERT_OK(Creat(0, "a"));
  ASSERT_OK(Creat(0, "b"));
  ASSERT_OK(Creat(0, "c"));
  ASSERT_OK(fsdb_->Flush(false));
  ASSERT_OK(ReopenFilesystem(fsloc_ + "/fs2"));
  ASSERT_OK(BulkIn(0, fsloc_));
  ASSERT_OK(Exist(0, "a"));
  ASSERT_OK(Exist(0, "b"));
  ASSERT_OK(Exist(0, "c"));
}

}  // namespace pdlfs

int main(int argc, char* argv[]) {
  return pdlfs::test::RunAllTests(&argc, &argv);
}
