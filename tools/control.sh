#!/bin/bash

getpid()
{
    local server_pid=`ps -ef | grep iot-data-sync | grep "./bin/iot-data-sync" | grep -v bash | grep -v grep | awk '{print $2}'`
    if [ "X$server_pid" = "X" ];
    then
        echo "0"
    fi
    echo "$server_pid"
}

#查看pid
pid()
{
    local pid=$(getpid)
    echo "pid: $pid"
}

#停止服务
stop()
{
    local pid=$(getpid)
    echo "Stopping server, pid: $pid"
    if [ $pid -gt 0 ];
    then
        kill -SIGTERM $pid
    fi
    return 0
}

#启动服务
start()
{
    local old_pid=$(getpid)
    if [ "X$old_pid" != "X" ]; then
        echo "server running now. begin stop..."
        stop
        sleep 5
        echo "server stop success."
    fi
    ulimit -n 327680 && ulimit -u 65535
    export LD_LIBRARY_PATH=/home/q/system/qbus/
    nohup  ./bin/iot-data-sync > ./nohup_data_sync.log 2>&1 &
    sleep 2
    local new_pid=$(getpid)
    echo "started new server, pid: $new_pid"
}

cmd()
{
    case $1 in
        pid)
            pid
            ;;
        start)
            start
            ;;
        stop)
            stop
            ;;
        *)
            echo "start, stop"
            ;;
    esac
    return 0
}

cmd $1
