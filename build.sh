#!/bin/bash

ModuleName=iot-data-sync

export GOPATH=`pwd`
export LD_LIBRARY_PATH=$GOPATH/src/vendor/360.cn/qbus/


make clean && make

if [ $? -ne 0 ]; then
    echo "build failed"
    exit
else
    echo "build succeed"
fi

rm -rf output
mkdir output
mkdir -p output/log
mkdir -p output/bin
cp  -R   ./tools   ./output
cp  -R   ./conf    ./output
cp ./$ModuleName ./output/bin

chmod 777 output -R
