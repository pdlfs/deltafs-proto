/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */
#include "rados_db_env.h"

#include "pdlfs-common/leveldb/db.h"
#include "pdlfs-common/leveldb/options.h"
#include "pdlfs-common/testharness.h"

#include <algorithm>
#include <stdio.h>
#include <string.h>
#include <vector>

// Parameters for opening ceph.
namespace {
const char* FLAGS_user_name = "client.admin";
const char* FLAGS_rados_cluster_name = "ceph";
const char* FLAGS_pool_name = "test";
const char* FLAGS_conf = NULL;  // Use ceph defaults
}  // namespace

namespace pdlfs {
namespace rados {

class RadosDbEnvBulkTest {
 public:
  RadosDbEnvBulkTest() {
    working_dir1_ = test::TmpDir() + "/rados_bulk1";
    working_dir2_ = test::TmpDir() + "/rados_bulk2";
    RadosConnMgrOptions options;
    mgr_ = new RadosConnMgr(options);
    env_ = NULL;
  }

  void Open() {
    RadosConn* conn;
    Osd* osd;
    ASSERT_OK(mgr_->OpenConn(  ///
        FLAGS_rados_cluster_name, FLAGS_user_name, FLAGS_conf,
        RadosConnOptions(), &conn));
    ASSERT_OK(mgr_->OpenOsd(conn, FLAGS_pool_name, RadosOptions(), &osd));
    env_ = mgr_->OpenEnv(osd, true, RadosEnvOptions());
    env_->CreateDir(working_dir1_.c_str());
    env_->CreateDir(working_dir2_.c_str());
    mgr_->Release(conn);
  }

  ~RadosDbEnvBulkTest() {
    env_->DeleteDir(working_dir2_.c_str());
    env_->DeleteDir(working_dir1_.c_str());
    delete env_;
    delete mgr_;
  }

  DBOptions GetIoSimplifiedDbOptions() {
    DBOptions options;
    options.info_log = Logger::Default();
    options.rotating_manifest = true;
    options.skip_lock_file = true;
    return options;
  }

  std::string GetFromDb(const std::string& key, DB* db) {
    std::string tmp;
    Status s = db->Get(ReadOptions(), key, &tmp);
    if (s.IsNotFound()) {
      tmp = "NOT_FOUND";
    } else if (!s.ok()) {
      tmp = s.ToString();
    }
    return tmp;
  }

  std::string working_dir1_;
  std::string working_dir2_;
  RadosConnMgr* mgr_;
  Env* env_;
};

TEST(RadosDbEnvBulkTest, BulkIn) {
  Open();
  DBOptions options = GetIoSimplifiedDbOptions();
  options.create_if_missing = true;
  options.env = env_;
  DB* db;
  ASSERT_OK(DB::Open(options, working_dir1_, &db));
  WriteOptions wo;
  ASSERT_OK(db->Put(wo, "k1", "v1"));
  FlushOptions fo;
  ASSERT_OK(db->FlushMemTable(fo));
  delete db;
  options.error_if_exists = false;
  ASSERT_OK(DB::Open(options, working_dir2_, &db));
  InsertOptions in;
  in.method = kCopy;
  ASSERT_OK(db->AddL0Tables(in, working_dir1_));
  ASSERT_EQ("v1", GetFromDb("k1", db));
  delete db;
  DestroyDB(working_dir2_, options);
  DestroyDB(working_dir1_, options);
}

}  // namespace rados
}  // namespace pdlfs

namespace {
inline void PrintUsage() {
  fprintf(stderr, "Use --cluster, --user, --conf, and --pool to conf test.\n");
  exit(1);
}

void ParseArgs(int argc, char* argv[]) {
  for (int i = 1; i < argc; ++i) {
    ::pdlfs::Slice a = argv[i];
    if (a.starts_with("--cluster=")) {
      FLAGS_rados_cluster_name = argv[i] + strlen("--cluster=");
    } else if (a.starts_with("--user=")) {
      FLAGS_user_name = argv[i] + strlen("--user=");
    } else if (a.starts_with("--conf=")) {
      FLAGS_conf = argv[i] + strlen("--conf=");
    } else if (a.starts_with("--pool=")) {
      FLAGS_pool_name = argv[i] + strlen("--pool=");
    } else {
      PrintUsage();
    }
  }

  printf("Cluster name: %s\n", FLAGS_rados_cluster_name);
  printf("User name: %s\n", FLAGS_user_name);
  printf("Storage pool: %s\n", FLAGS_pool_name);
  printf("Conf: %s\n", FLAGS_conf);
}

}  // namespace

int main(int argc, char* argv[]) {
  if (argc > 1) {
    ParseArgs(argc, argv);
    return ::pdlfs::test::RunAllTests(&argc, &argv);
  } else {
    return 0;
  }
}
