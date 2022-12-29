#! /bin/bash

# get value of optional parameter with-cleanup
cleanup=false
while [ "$1" ]; do
        case $1 in
            --with-cleanup) cleanup=true ;;
            *) echo 'Error in command line parsing' >&2
               exit 1
        esac
        shift
    done

# color codes for better terminal output
GREEN='\033[0;32m'  # green color
NC='\033[0m' # No Color

# create docker containers that will be running in the background
echo -e "${GREEN}Creating Docker Containers...${NC}"
docker-compose up -d

# wait 15 seconds until kafka is fully functional to avoid warnings
sleep 15

# create topics in kafka container
echo -e "${GREEN}Creating Topics in Kafka Container...${NC}"

docker exec -it kafka kafka-topics.sh --create  --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic TH1

docker exec -it kafka kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic HVAC1

docker exec -it kafka kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Etot

docker exec -it kafka kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic W1

# docker clean-up
if "$cleanup"; then
    echo -e "${GREEN}Clean up Process...${NC}"
    docker stop kafka   # stop kafka container
    docker rm kafka     # remove kafka container
    docker stop zookeeper # stop zookeeper container
    docker rm zookeeper   # remove zookeeper container

    docker image rm bitnami/kafka:latest     # remove kafka image
    docker image rm bitnami/zookeeper:latest # remove zookeeper image

    docker volume prune -f   # remove unused volumes
fi

