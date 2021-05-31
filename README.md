**Massive serverless client middleware for fast distributed file system metadata functions.**

[![CI](https://github.com/pdlfs/deltafs-proto/actions/workflows/ci.yml/badge.svg)](https://github.com/pdlfs/deltafs-proto/actions/workflows/ci.yml)
[![DOI](https://zenodo.org/badge/253742457.svg)](https://zenodo.org/badge/latestdoi/253742457)
[![Build Status](https://travis-ci.org/pdlfs/deltafs-proto.svg?branch=master)](https://travis-ci.org/pdlfs/deltafs-proto)
[![GitHub Release](https://img.shields.io/github/release/pdlfs/deltafs-proto.svg)](https://github.com/pdlfs/deltafs-proto/releases)
[![License](https://img.shields.io/badge/license-New%20BSD-blue.svg)](LICENSE.txt)

DeltaFS Prototype
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

DeltaFS was developed, in part, under U.S. Government contract 89233218CNA000001 for Los Alamos National Laboratory (LANL), which is operated by Triad National Security, LLC for the U.S. Department of Energy/National Nuclear Security Administration. Please see the accompanying [LICENSE.txt](LICENSE.txt) for further information.

# Platform

DeltaFS is able to run on Linux, Mac OS, as well as most UNIX platforms for both development and local testing purposes. To run DeltaFS in production, only Linux is supported at this moment. DeltaFS is mostly written in C++. C++11 is NOT required to compile DeltaFS, but will be leveraged given that the compiler supports it. C++14 or later is currently not used. A compiler that supports C++14 or later can still compile DeltaFS code. It is just that DeltaFS does not currently use any C++14 or later features.

# Software requirements

Compiling DeltaFS requires a recent C/C++ compiler, cmake, make, mpi, snappy, and Ceph RADOS. On Ubuntu 14.04 LTS or later, you may use the following to prepare the system environment for DeltaFS.

```bash
sudo apt-get install gcc g++ make  # Alternatively, this may also be clang
sudo apt-get install cmake cmake-curses-gui
sudo apt-get install libsnappy-dev librados-dev
sudo apt-get install libmpich-dev  # Alternatively, this may also be libopenmpi-dev
sudo apt-get install mpich
```

# Building

After all software dependencies are installed, we can proceed to building DeltaFS. DeltaFS uses cmake and we suggest you to do an out-of-source build. To do that, we create a build directory (`mkdir build`) beneath the root DeltaFS source directory and run 'ccmake' from it:

```bash
git clone git@github.com:pdlfs/deltafs-proto.git
cd deltafs-proto
mkdir build
cd build
ccmake -DDELTAFS_COMMON_INTREE=ON ..
```

Choose the following cmake options and then type 'c' (potentially multiple times) until we see `[g] Generate`:

```bash
 BUILD_SHARED_LIBS                ON
 BUILD_TESTS                      ON
 CMAKE_BUILD_TYPE                 RelWithDebInfo
 CMAKE_INSTALL_PREFIX             /usr/local
 CMAKE_PREFIX_PATH
 DELTAFS_COMMON_INTREE            ON
 DELTAFS_MPI                      ON
 PDLFS_GFLAGS                     OFF
 PDLFS_GLOG                       OFF
 PDLFS_RADOS                      ON
 PDLFS_SNAPPY                     ON
 PDLFS_VERBOSE                    1
```

Type 'g' to generate build files and exit CMake. Next, we do:

```bash
make
```

Finally, we run tests to check if DeltaFS is correctly built:

```bash
ctest -VV  ## <-- This is double V, not W
```

Sample test output can be checked at https://github.com/pdlfs/deltafs-proto/actions/workflows/ci.yml. It takes about 5 minutes to run all tests.

# DONE
