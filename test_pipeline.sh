#!/bin/sh -l

git clone --recursive https://github.com/Bouncner/hyrise_workload_analysis_plugin.git

cd hyrise_workload_analysis_plugin/hyrise
./install_dependencies.sh
cd ..

mkdir clang-debug && cd clang-debug && cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ ..
make WorkloadStatistics SimpleExecutor hyriseServer

./hyriseServer --benchmark_data=TPC-H:0.01 &
server_pid=$!

cat > commands.sql << EOF
INSERT INTO meta_plugins(name) VALUES ('rel/libWorkloadHandler.so');
select count(*) from meta_benchmark_items;
EOF

cat > TPCH_Q01.exp << EOF
#!/usr/bin/expect
spawn psql -h localhost -p 5432 -f commands.sql
expect "22"
EOF

chmod +x TPCH_Q01.exp
./TPCH_Q01.exp
ret=$?

kill -9 $server_pid
exit $ret
