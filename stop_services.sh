#!/bin/bash

# color codes for better output formatting
GREEN='\033[0;32m'  # green color
NC='\033[0m' # No Color

# stop all services by shuting down their docker containers
echo -e "${GREEN}Shuting down services...${NC}"
docker stop influxdb kafka zookeeper

echo -e "${GREEN}Success!${NC}"
