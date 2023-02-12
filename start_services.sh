#!/bin/bash

# Start kafka and zookeeper containers
cd setup/kafka_setup
./setup_kafka.sh

# start influxdb service
cd ../influxdb_setup
./setup_influxdb.sh

# start telegraf - TODO: maybe start it manually and not from here
cd ../telegraf_setup
#telegraf --config telegraf.conf

# return to the original directory
cd ../../
