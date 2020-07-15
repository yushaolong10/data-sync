#!/bin/bash

PUBLIC_DIR=/home/sync360/data-sync

SERVICE_DIR=/home/q/system/data-sync

mkdir -p $SERVICE_DIR
mkdir -p $SERVICE_DIR/data
cp -R $PUBLIC_DIR/conf $SERVICE_DIR/
cp -R $PUBLIC_DIR/tools $SERVICE_DIR/

cp -R $PUBLIC_DIR/bin  $SERVICE_DIR/
chown sync360:sync360 $SERVICE_DIR -R
cd $SERVICE_DIR
bash ./tools/control.sh start





