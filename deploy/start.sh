nohup ./bin/tinyscheduler-server > ./log/scheduler.log 2>&1& echo $! > ./pid/scheduler.pid
nohup ./bin/tinykv-server -path=./data >./log/kv.log 2>&1& echo $! > ./pid/kv.pid
nohup ./bin/tinysql-server --store=tikv --path="127.0.0.1:2379" >./log/server.log 2>&1& echo $! > ./pid/server.pid