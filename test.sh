#!/bin/sh -l

git clone --recursive https://github.com/hyrise/hyrise.git
cd hyrise

./install_dependencies.sh
apt-get install psql

mkdir clang-debug && cd clang-debug && cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ ..
make WorkloadStatistics SimpleExecutor hyriseServer

./hyriseServer --benchmark_data=TPC-H:10 &
server_pid=$!



kill -9 $server_pid