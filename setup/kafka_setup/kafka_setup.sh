#! /bin/bash

# color codes for better terminal output
GREEN='\033[0;32m'  # green color
NC='\033[0m' # No Color

# create docker containers that will be running in the background
echo -e "${GREEN}Creating Docker Containers...${NC}"
docker-compose up -d

# create topics in kafka container
echo -e "${GREEN}Creating Topics in Kafka Container...${NC}"
# docker exec -it kafka /bin/bash
docker exec -it kafka /opt/kafka_*/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic TH1

docker exec -it kafka /opt/kafka_*/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic HVAC1

docker exec -it kafka /opt/kafka_*/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic Etot

docker exec -it kafka /opt/kafka_*/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic W1


# docker clean-up
echo -e "${GREEN}Clean up Process...${NC}"
docker stop kafka   # stop kafka container
docker rm kafka     # remove kafka container
docker stop zookeeper # stop zookeeper container
docker rm zookeeper   # remove zookeeper container

docker image rm confluentinc/cp-kafka:latest     # remove kafka image
docker image rm confluentinc/cp-zookeeper:latest  # remove zookeeper image

docker volume prune    # remove unused volumes

