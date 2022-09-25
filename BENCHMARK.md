
Benchmark Guide
=======

```
XXXXXXXXX
XX      XX                 XX                  XXXXXXXXXXX
XX       XX                XX                  XX
XX        XX               XX                  XX
XX         XX              XX   XX             XX
XX          XX             XX   XX             XXXXXXXXX
XX           XX  XXXXXXX   XX XXXXXXXXXXXXXXX  XX         XX
XX          XX  XX     XX  XX   XX       XX XX XX      XX
XX         XX  XX       XX XX   XX      XX  XX XX    XX
XX        XX   XXXXXXXXXX  XX   XX     XX   XX XX    XXXXXXXX
XX       XX    XX          XX   XX    XX    XX XX           XX
XX      XX      XX      XX XX   XX X    XX  XX XX         XX
XXXXXXXXX        XXXXXXX   XX    XX        XX  XX      XX
```

[![DOI](https://zenodo.org/badge/253742457.svg)](https://zenodo.org/badge/latestdoi/253742457)

In this document, we write down the steps for running deltafs benchmarks that execute the experiments in our sc21 paper on a single node. Our paper compares deltafs performance with indexfs. In this guide, we first specify how to compile and prepare deltafs for testing. We then show how to run our benchmark code to obtain deltafs and indexfs performance numbers.

Our paper runned experiments on the CMU's Narwhal computing cluster and used up to 128 compute nodes as client nodes for running our benchmark code. In this guide, on the other hand, we will run all experiments on a single node. Our goal is to showcase how to re-execute the experiments as we did in the paper. Following this guide won't reproduce the same results as reported. While showing the steps for reruns, we will document key differences between our rerun and the runs that we did in the paper.

# System Requirement

DeltaFS is easy to build and run. As our README says, a Linux box and a recent gcc/g++ (4.8.5+) compiler will do. In this guide, we will assume an Ubuntu platform and we will start from here.

The first step is to prepare the node with a few basic packages for compiling and building deltafs.

```bash
sudo apt-get install gcc g++ make cmake libsnappy-dev libmpich-dev mpich git
```

Note that in our paper we have also installed `librados-dev` for deltafs to store data in a shared underlying ceph rados object store. In this guide it is skipped as we only perform experiments on a single node and will be storing data on the node's local filesystem. We used `mpich` in this guide as we did in our paper, but `openmpi` will work equally well.

# Building DeltaFS

Our spirit is to put everything related to the experiments of this guide in `/tmp/deltafs`, including the deltafs source code that we are about to obtain. We will put it in `/tmp/deltafs/src`.

```bash
mkdir -p /tmp/deltafs/src
cd /tmp/deltafs/src
git clone https://github.com/pdlfs/deltafs-proto.git .
git checkout v2021.5
```

We then use `cmake` to build deltafs. Our plan is to use `/tmp/deltafs/build` as the build dir and `/tmp/deltafs/install` as the install dir.

```bash
mkdir -p /tmp/deltafs/build
cd /tmp/deltafs/build
cmake -DCMAKE_INSTALL_PREFIX=/tmp/deltafs/install \
 -DCMAKE_BUILD_TYPE=RelWithDebInfo \
 -DCMAKE_C_COMPILER=`which gcc` \
 -DCMAKE_CXX_COMPILER=`which g++` \
 -DMPI_C_COMPILER=`which mpicc.mpich` \
 -DMPI_CXX_COMPILER=`which mpicxx.mpich` \
 -DDELTAFS_COMMON_INTREE=ON \
 -DDELTAFS_MPI=ON \
 -DPDLFS_SNAPPY=ON \
 ../src
```

Here's a sample of the output generated by cmake.

```
[02:18 qingzhen@h0 build] > cmake -DCMAKE_INSTALL_PREFIX=/tmp/deltafs/install \
>  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
>  -DCMAKE_C_COMPILER=`which gcc` \
>  -DCMAKE_CXX_COMPILER=`which g++` \
>  -DMPI_C_COMPILER=`which mpicc.mpich` \
>  -DMPI_CXX_COMPILER=`which mpicxx.mpich` \
>  -DDELTAFS_COMMON_INTREE=ON \
>  -DDELTAFS_MPI=ON \
>  -DPDLFS_SNAPPY=ON \
>  ../src
-- The C compiler identification is GNU 5.4.0
-- The CXX compiler identification is GNU 5.4.0
-- Detecting C compiler ABI info
-- Detecting C compiler ABI info - done
-- Check for working C compiler: /usr/bin/gcc - skipped
-- Detecting C compile features
-- Detecting C compile features - done
-- Detecting CXX compiler ABI info
-- Detecting CXX compiler ABI info - done
-- Check for working CXX compiler: /usr/bin/g++ - skipped
-- Detecting CXX compile features
-- Detecting CXX compile features - done
-- Found PkgConfig: /usr/bin/pkg-config (found version "0.29.1")
-- note: using built-in pkg_check_module imported targets
-- Found Snappy: /usr/include
-- Enabled Snappy - PDLFS_SNAPPY=ON
-- Looking for pthread.h
-- Looking for pthread.h - found
-- Performing Test CMAKE_HAVE_LIBC_PTHREAD
-- Performing Test CMAKE_HAVE_LIBC_PTHREAD - Failed
-- Check if compiler accepts -pthread
-- Check if compiler accepts -pthread - yes
-- Found Threads: TRUE
-- Performing Test flag-Wpedantic
-- Performing Test flag-Wpedantic - Success
-- Performing Test flag-Wall
-- Performing Test flag-Wall - Success
-- Performing Test flag-Wno-long-long
-- Performing Test flag-Wno-long-long - Success
-- Performing Test flag-Wno-sign-compare
-- Performing Test flag-Wno-sign-compare - Success
-- Found MPI_C: /usr/lib/x86_64-linux-gnu/libmpich.so (found version "3.1")
-- Found MPI_CXX: /usr/lib/x86_64-linux-gnu/libmpichcxx.so (found version "3.1")
-- Found MPI: TRUE (found version "3.1")
-- Configuring done
-- Generating done
-- Build files have been written to: /tmp/deltafs/build
```

We then hit `make` to compile the code. Use `-j` to specify the desired level of concurrency according to your system configuration.

```bash
make -j 8
```

Once it is done, we request an install.

```bash
make install
```

Artifacts will be installed to `/tmp/deltafs/install` as shown below.

```
[02:24 qingzhen@h0 install] > ls -l
total 16K
drwxr-xr-x 2 qingzhen TableFS 4.0K Aug  7 02:24 bin
drwxr-xr-x 4 qingzhen TableFS 4.0K Aug  7 02:24 include
drwxr-xr-x 2 qingzhen TableFS 4.0K Aug  7 02:24 lib
drwxr-xr-x 3 qingzhen TableFS 4.0K Aug  7 02:24 share
```

# Run 1: IndexFS

Our paper compares DeltaFS with IndexFS. DeltaFS code is able to act as IndexFS. To do this, we use `deltafs_bm_srv` at `/tmp/deltafs/install/bin` to run as IndexFS servers and `deltafs_bm_cli` to run as IndexFS clients. In real world environments, we would run IndexFS server processes on dedicated server nodes and run IndexFS clients inside job processes on compute nodes as we did in our paper. In this guide, we run both on a single node.

We will need two shells. One for IndexFS servers. The other for clients. Do the following to start 2 IndexFS server processes with data stored at `/tmp/deltafs/run1`.

```bash
mkdir -p /tmp/deltafs/run1/
rm -rf /tmp/deltafs/run1/*
cd /tmp/deltafs/install/bin
mpirun.mpich -np 2 \
 -env DELTAFS_Db_write_ahead_log_buffer 65536 \
 -env DELTAFS_Db_disable_write_ahead_logging 0 \
 -env DELTAFS_Db_disable_compaction 0 \
 -env DELTAFS_Db_l0_soft_limit 8 \
 -env DELTAFS_Db_l0_hard_limit 12 \
 -env DELTAFS_Db_compression 1 \
 ./deltafs_bm_svr \
 --db=/tmp/deltafs/run1/svr \
 --skip_fs_checks=0 \
 --ports_per_rank=1 \
 --rpc_threads=2
```

The server will run (start listening to clients) and display the following information.

```
[02:52 qingzhen@h0 bin] > mpirun.mpich -np 2 \
>  -env DELTAFS_Db_write_ahead_log_buffer 65536 \
>  -env DELTAFS_Db_disable_write_ahead_logging 0 \
>  -env DELTAFS_Db_disable_compaction 0 \
>  -env DELTAFS_Db_l0_soft_limit 8 \
>  -env DELTAFS_Db_l0_hard_limit 12 \
>  -env DELTAFS_Db_compression 1 \
>  ./deltafs_bm_svr \
>  --db=/tmp/deltafs/run1/svr \
>  --skip_fs_checks=0 \
>  --ports_per_rank=1 \
>  --rpc_threads=2
Date:       Sat Aug  7 02:52:37 2021
CPU:        160 * Intel(R) Xeon(R) CPU E7-L8867  @ 2.13GHz
CPUCache:   30720 KB
rpc ip:             127.0.0.1*
rpc use udp:        Yes (MAX_MSGSZ=1432, SO_RCVBUF=512K, SO_SNDBUF=512K)
Num rpc threads:    2 + 0
Num ports per rank: 1
Num ranks:          2
Fs use existing:    0 (readonly=0)
Fs info port:       10086
Fs skip checks:     0
Fs dummy:           0
READONLY DB CHAIN:
MAIN DB:
Snappy:             1
Blk cache size:     0    MB
Blk size:           4    KB
Bloom bits:         10
Memtable size:      8    MB
Tbl size:           4    MB
Tbl write size:     256  KB (min), 512 KB (max)
Tbl cache size:     2500
Io monitoring:      1
Lsm compaction off: 0
Db level factor:    8
L0 limits:          8 (soft), 12 (hard)
L1 trigger:         5
Prefetch compaction input: 1
Prefetch read size: 256  KB
Wal off:            0
Wal write size:     64   KB
Use existing db:    0
Db: /tmp/deltafs/run1/svr/r<rank>
------------------------------------------------
Running...
```

Next, switch to the other shell and run 4 IndexFS client processes to generate the test workload.

```bash
cd /tmp/deltafs/install/bin
mpirun.mpich -np 4 \
 ./deltafs_bm_cli \
 --share_dir=1 \
 --n=200000 \
 --read_phases=1 \
 --info_svr_uri=tcp://127.0.0.1:10086 \
 --skip_fs_checks=0
```

The clients will run, display progress, and print test results when done.

```
[02:57 qingzhen@h0 bin] > mpirun.mpich -np 4 \
>  ./deltafs_bm_cli \
>  --share_dir=1 \
>  --n=200000 \
>  --read_phases=1 \
>  --info_svr_uri=tcp://127.0.0.1:10086 \
>  --skip_fs_checks=0
Date:       Sat Aug  7 02:58:43 2021
CPU:        160 * Intel(R) Xeon(R) CPU E7-L8867  @ 2.13GHz
CPUCache:   30720 KB
Num ranks:          4
Fs use existing:    0 (prepare_run=mkdir)
Fs use local:       0
Fs infosvr locatio: tcp://127.0.0.1:10086
Fs skip checks:     0
RPC timeout:        30 s
Creats:             200000 x 1 per rank (0 KB data per file)
Batched creats:     OFF
Bulk in:            OFF
Lstats:             200000 x 1 per rank
Mon:                OFF
Random key order:   1
Share dir:          1
------------------------------------------------
preparing run...
==insert    :    49.499 micros/op,     0.081 Mop/s,           9.928 s,          800000 ops
sleeping for 5 seconds...
==fstats    :    52.306 micros/op,     0.076 Mop/s,          10.474 s,          800000 ops
```

Now, switch back to the original shell and type `^C` to turn down the servers.

# Run 2: IndexFS Bulk Insertion

Our second run involves running IndexFS with a novel optimization named bulk insertion. The experiment workflow remains unchanged: we first start servers and then launch clients.

We still need 2 shells. Servers go with the first shell. We still start 2 servers, but this time we will store data at `/tmp/deltafs/run2`.

```bash
mkdir -p /tmp/deltafs/run2/
rm -rf /tmp/deltafs/run2/*
cd /tmp/deltafs/install/bin
mpirun.mpich -np 2 \
 -env DELTAFS_Db_write_ahead_log_buffer 65536 \
 -env DELTAFS_Db_disable_write_ahead_logging 0 \
 -env DELTAFS_Db_disable_compaction 0 \
 -env DELTAFS_Db_l0_soft_limit 8 \
 -env DELTAFS_Db_l0_hard_limit 12 \
 -env DELTAFS_Db_compression 1 \
 ./deltafs_bm_svr \
 --db=/tmp/deltafs/run2/svr \
 --skip_fs_checks=0 \
 --ports_per_rank=1 \
 --rpc_threads=2
```

Then, we launch clients. This time, we will additionally ask clients to perform bulk insertion. We do this by specifying `--bk=1`.

```bash
cd /tmp/deltafs/install/bin
mpirun.mpich -np 4 \
 ./deltafs_bm_cli \
 --share_dir=1 \
 --n=200000 \
 --bk=1 \
 --db=/tmp/deltafs/run2/cli \
 --read_phases=1 \
 --info_svr_uri=tcp://127.0.0.1:10086 \
 --skip_fs_checks=0
```

This time, we will see that the write phase is finishing much faster.

```
[03:14 qingzhen@h0 bin] > mpirun.mpich -np 4 \
>  ./deltafs_bm_cli \
>  --share_dir=1 \
>  --n=200000 \
>  --bk=1 \
>  --db=/tmp/deltafs/run2/cli \
>  --read_phases=1 \
>  --info_svr_uri=tcp://127.0.0.1:10086 \
>  --skip_fs_checks=0
Date:       Sat Aug  7 03:14:05 2021
CPU:        160 * Intel(R) Xeon(R) CPU E7-L8867  @ 2.13GHz
CPUCache:   30720 KB
Num ranks:          4
Fs use existing:    0 (prepare_run=mkdir)
Fs use local:       0
Fs infosvr locatio: tcp://127.0.0.1:10086
Fs skip checks:     0
RPC timeout:        30 s
Creats:             200000 x 1 per rank (0 KB data per file)
Batched creats:     OFF
Bulk in:            1
BK DB:
Snappy:             0
Blk size:           4    KB
Bloom bits:         10
Memtable size:      8    MB
Tbl write size:     256  KB (min), 512 KB (max)
Wal off:            0
Wal write size:     4    KB
Db: /tmp/deltafs/run2/cli/b<dir>
Lstats:             200000 x 1 per rank
Mon:                OFF
Random key order:   1
Share dir:          1
------------------------------------------------
preparing run...
==insert    :     6.489 micros/op,     0.615 Mop/s,           1.301 s,          800000 ops
sleeping for 5 seconds...
==fstats    :    47.960 micros/op,     0.083 Mop/s,           9.593 s,          800000 ops
```

Before moving to the next run, we switch back to the servers and type `^C` to turn them down.

# Run 3: DeltaFS

Now, we will be testing DeltaFS. An important thesis of DeltaFS is no dedicated metadata servers. As a result, DeltaFS will run a bit differently. That is, we won't be starting servers and then launching clients. Instead, we will run experiments through 3 separate phases: write phase, parallel compaction phase, and read phase.

We start with writes. We will store data at `/tmp/deltafs/run3`.

```
mkdir -p /tmp/deltafs/run3
rm -rf /tmp/deltafs/run3/*
cd /tmp/deltafs/install/bin
mpirun.mpich -np 4 \
 -env DELTAFS_Db_write_ahead_log_buffer 65536 \
 -env DELTAFS_Db_disable_write_ahead_logging 0 \
 -env DELTAFS_Db_disable_compaction 1 \
 -env DELTAFS_Db_compression 1 \
 ./deltafs_bm_cli \
 --db=/tmp/deltafs/run3/origin \
 --share_dir=1 \
 --n=200000 \
 --read_phases=0 \
 --skip_fs_checks=1 \
 --fs_use_local=1
```

Here's a sample output.

```
[03:33 qingzhen@h0 bin] > mpirun.mpich -np 4 \
>  -env DELTAFS_Db_write_ahead_log_buffer 65536 \
>  -env DELTAFS_Db_disable_write_ahead_logging 0 \
>  -env DELTAFS_Db_disable_compaction 1 \
>  -env DELTAFS_Db_compression 1 \
>  ./deltafs_bm_cli \
>  --db=/tmp/deltafs/run3/origin \
>  --share_dir=1 \
>  --n=200000 \
>  --read_phases=0 \
>  --skip_fs_checks=1 \
>  --fs_use_local=1
Date:       Sat Aug  7 03:33:46 2021
CPU:        160 * Intel(R) Xeon(R) CPU E7-L8867  @ 2.13GHz
CPUCache:   30720 KB
Num ranks:          4
Fs use existing:    0 (prepare_run=mkdir)
Fs use local:       1
Fs infosvr locatio: N/A
Fs skip checks:     1
RPC timeout:        N/A
Creats:             200000 x 1 per rank (0 KB data per file)
Batched creats:     OFF
Bulk in:            OFF
Lstats:             200000 x 0 per rank
Mon:                OFF
Random key order:   1
Share dir:          1
CLIENT DB:
Snappy:             1
Blk cache size:     0    MB
Blk size:           4    KB
Bloom bits:         10
Memtable size:      8    MB
Tbl size:           4    MB
Tbl write size:     256  KB (min), 512 KB (max)
Tbl cache size:     2500 (max open tables)
Io monitoring:      1
Lsm compaction off: 1
Wal off:            0
Wal write size:     64   KB
Use existing db:    0
Db: /tmp/deltafs/run3/origin/r<rank>
------------------------------------------------
preparing run...
==insert    :     5.892 micros/op,     0.678 Mop/s,           1.179 s,          800000 ops
Total random reads: 0 (Avg read size: -nanK, total bytes read: 0)
Total sequential bytes read: 0 (Avg read size: -nanK)
Total bytes written: 3421409 (Avg write size: 238.7K)
 - Db stats: >>>
                              Files                  Compactions
Level  Files Size(MB) Input0 Input1 Output Counts Time(sec) Read(MB) Write(MB)
------------------------------------------------------------------------------
  0        2        3      0      0      2      2         0        0         3

 - L0 stats: >>>
Soft-Limit Hard-Limit MemTable
0          0          0
```

We then perform a parallel compaction. IndexFS did this through its dedicated server processes. In DeltaFS, the spirit is to do it through explicit job runs.

```bash
cd /tmp/deltafs/install/bin
mpirun.mpich -np 4 \
 -env DELTAFS_Db_disable_write_ahead_logging 1 \
 -env DELTAFS_Db_compression 1 \
 ./parallel_compactor \
 --src_dir=/tmp/deltafs/run3/origin \
 --dst_dir=/tmp/deltafs/run3/compact \
 --rpc_async_sender_threads=16 \
 --rpc_worker_threads=0 \
 --rpc_batch_min=4 \
 --rpc_batch_max=8 \
 --rpc_threads=2
```

Here's its output.

```
[03:39 qingzhen@h0 bin] > mpirun.mpich -np 4 \
>  -env DELTAFS_Db_disable_write_ahead_logging 1 \
>  -env DELTAFS_Db_compression 1 \
>  ./parallel_compactor \
>  --src_dir=/tmp/deltafs/run3/origin \
>  --dst_dir=/tmp/deltafs/run3/compact \
>  --rpc_async_sender_threads=16 \
>  --rpc_worker_threads=0 \
>  --rpc_batch_min=4 \
>  --rpc_batch_max=8 \
>  --rpc_threads=2
Date:       Sat Aug  7 03:39:16 2021
CPU:        160 * Intel(R) Xeon(R) CPU E7-L8867  @ 2.13GHz
CPUCache:   30720 KB
DELTAFS PARALLEL COMPACTOR
rpc ip:             127.0.0.1*
rpc use udp:        Yes (MAX_MSGSZ=1432, SO_RCVBUF=512K, SO_SNDBUF=512K)
rpc batch:          4 (min), 8 (max)
rpc timeout:        30
num sender threads: 16 (max outstanding rpcs)
num rpc threads:    2 + 0
num ranks:          4
SOURCE DB:
Blk cache:          (nil)
Tbl cache:          (nil)
Io monitoring:      1
Db: /tmp/deltafs/run3/origin/r<rank>
DESTINATION DB:
Snappy:             1
Blk cache size:     0    MB
Blk size:           4    KB
Bloom bits:         10
Memtable size:      8    MB
Tbl size:           4    MB
Tbl write size:     256  KB
Tbl cache size:     2500
Io monitoring:      1
Lsm compaction off: 0
Db level factor:    8
L0 limits:          8 (soft), 12 (hard)
L1 trigger:         5
Prefetch compaction input: 0
Prefetch read size: 256  KB
Wal off:            1
Db force cleaning:  1
Db: /tmp/deltafs/run3/compact/r<rank>
------------------------------------------------
Bootstrapping...
Running...
Done!
==mapreduce :    10.040 micros/op,     0.398 Mop/s,           2.009 s,          800004 ops
Total random reads: 7224 (Avg read size: 1.9K, total bytes read: 13713802) // src db
Total bytes written: 13891605 (Avg write size: 242.3K)
 - Db stats: >>>
                              Files                  Compactions
Level  Files Size(MB) Input0 Input1 Output Counts Time(sec) Read(MB) Write(MB)
------------------------------------------------------------------------------
  0        2        3      0      0      2      2         0        0         3

 - L0 stats: >>>
Soft-Limit Hard-Limit MemTable
0          0          0

Bye
```

Now, we need 2 shells for the final read phase. In the first shell, we start transient DeltaFS servers for reads.

```bash
cd /tmp/deltafs/install/bin
mpirun.mpich -np 4 \
 ./deltafs_bm_svr \
 --use_existing_fs=1 \
 --db=/tmp/deltafs/run3/compact \
 --skip_fs_checks=0 \
 --ports_per_rank=1 \
 --rpc_threads=2
```

Here's the servers' output:

```
[03:42 qingzhen@h0 bin] > mpirun.mpich -np 4 \
>  ./deltafs_bm_svr \
>  --use_existing_fs=1 \
>  --db=/tmp/deltafs/run3/compact \
>  --skip_fs_checks=0 \
>  --ports_per_rank=1 \
>  --rpc_threads=2
Date:       Sat Aug  7 03:42:20 2021
CPU:        160 * Intel(R) Xeon(R) CPU E7-L8867  @ 2.13GHz
CPUCache:   30720 KB
rpc ip:             127.0.0.1*
rpc use udp:        Yes (MAX_MSGSZ=1432, SO_RCVBUF=512K, SO_SNDBUF=512K)
Num rpc threads:    2 + 0
Num ports per rank: 1
Num ranks:          4
Fs use existing:    1 (readonly=1)
Fs info port:       10086
Fs skip checks:     0
Fs dummy:           0
READONLY DB CHAIN:
MAIN DB:
Snappy:             0
Blk cache size:     0    MB
Blk size:           4    KB
Bloom bits:         10
Memtable size:      8    MB
Tbl size:           4    MB
Tbl write size:     256  KB (min), 512 KB (max)
Tbl cache size:     2500
Io monitoring:      1
Lsm compaction off: 0
Db level factor:    8
L0 limits:          8 (soft), 12 (hard)
L1 trigger:         5
Prefetch compaction input: 1
Prefetch read size: 256  KB
Wal off:            1
Use existing db:    1
Db: /tmp/deltafs/run3/compact/r<rank>
------------------------------------------------
Running...
```

We then switch to the other shell and run clients.

```bash
cd /tmp/deltafs/install/bin
mpirun.mpich -np 4 \
 ./deltafs_bm_cli \
 --use_existing_fs=1 \
 --share_dir=1 \
 --n=200000 \
 --write_phases=0 \
 --read_phases=1 \
 --info_svr_uri=tcp://127.0.0.1:10086 \
 --skip_fs_checks=0
```

Here's the result.

```
[03:44 qingzhen@h0 bin] > mpirun.mpich -np 4 \
>  ./deltafs_bm_cli \
>  --use_existing_fs=1 \
>  --share_dir=1 \
>  --n=200000 \
>  --write_phases=0 \
>  --read_phases=1 \
>  --info_svr_uri=tcp://127.0.0.1:10086 \
>  --skip_fs_checks=0
Date:       Sat Aug  7 03:44:20 2021
CPU:        160 * Intel(R) Xeon(R) CPU E7-L8867  @ 2.13GHz
CPUCache:   30720 KB
Num ranks:          4
Fs use existing:    1 (prepare_run=lstat)
Fs use local:       0
Fs infosvr locatio: tcp://127.0.0.1:10086
Fs skip checks:     0
RPC timeout:        30 s
Creats:             200000 x 0 per rank (0 KB data per file)
Batched creats:     OFF
Bulk in:            OFF
Lstats:             200000 x 1 per rank
Mon:                OFF
Random key order:   1
Share dir:          1
------------------------------------------------
preparing run...
==fstats    :    69.035 micros/op,     0.057 Mop/s,          13.921 s,          800000 ops
```

Done