echo -e "${GREEN}Clean up Process...${NC}"
docker stop kafka   # stop kafka container
docker rm kafka     # remove kafka container
docker stop zookeeper # stop zookeeper container
docker rm zookeeper   # remove zookeeper container
docker image rm bitnami/kafka:latest     # remove kafka image
docker image rm bitnami/zookeeper:latest # remove zookeeper image
docker volume prune -f   # remove unused volumes