#!/bin/bash

PUBLIC_DIR=/home/go/data-sync

SERVICE_DIR=/home/q/system/data-sync

mkdir -p $SERVICE_DIR
mkdir -p $SERVICE_DIR/data
cp -R $PUBLIC_DIR/conf $SERVICE_DIR/
cp -R $PUBLIC_DIR/tools $SERVICE_DIR/

cp -R $PUBLIC_DIR/bin  $SERVICE_DIR/

cd $SERVICE_DIR
bash ./tools/control.sh start





