#!/bin/bash

PROJECT_DIR=/home/user/   # change with the location of the repo in your computer

# go to the project directory
cd ${PROJECT_DIR}

# Start kafka and zookeeper containers
cd setup/kafka_setup
./setup_kafka.sh

# start influxdb service
cd ../influxdb_setup
./setup_influxdb.sh

