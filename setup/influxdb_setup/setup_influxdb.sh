#!/bin/bash

# color codes for better terminal output
GREEN='\033[0;32m'  # green color
NC='\033[0m' # No Color

# check if infludb container exists
if [ ! "$(docker ps -a | grep influxdb)" ]; then
    # if it doesn't exist, create it and start it
    echo -e "${GREEN}Creating and starting influxdb container...${NC}";
    docker run --name influxdb -p 8086:8086 influxdb:2.0.9;
else
    # else, simply start the container
    echo -e "${GREEN}Starting influxdb container${NC}";
    docker start influxdb;
fi
