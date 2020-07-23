#!/bin/bash

ModuleName=iot-data-sync

export GOPATH=`pwd`

make clean && make

if [ $? -ne 0 ]; then
    echo "build failed"
    exit 1
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
