#!/bin/bash

# code for optional bash parameter --remove-container
REMOVE_CONTAINERS=false
while [ "$1" ]; do
        case $1 in
            --remove-containers) REMOVE_CONTAINERS=true ;;
            *) echo 'Error in command line parsing' >&2
               exit 1
        esac
        shift
    done

# color codes for better output formatting
GREEN='\033[0;32m'  # green color
NC='\033[0m' # No Color

# stop all services by shuting down their docker containers
echo -e "${GREEN}Shuting down services...${NC}"
docker stop influxdb kafka zookeeper

if "$REMOVE_CONTAINERS"; then
    echo -e "${GREEN}Removing docker containers...${NC}"
    docker rm kafka       # remove kafka container
    docker rm zookeeper   # remove zookeeper container
    docker rm influxdb    # remove influxdb container

    docker image rm bitnami/kafka:latest     # remove kafka image
    docker image rm bitnami/zookeeper:latest # remove zookeeper image
    docker image rm influxdb                 # remove influxdb image
    
    docker volume prune -f   # remove unused volumes
fi
