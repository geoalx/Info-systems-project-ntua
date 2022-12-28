#!/usr/bin/env bash
# Script that download docker-compose, assuming you already have successfully installed docker
# If you have not install docker, run setup_docker.sh before running this script

# create a docker directory inside home folder
mkdir -p  ~/.docker/cli-plugins/

# download the latest release for docker compose
curl -SL https://github.com/docker/compose/releases/download/v2.14.2/docker-compose-linux-x86_64 -o ~/.docker/cli-plugins/docker-compose

# set docker-compose to executable
chmod +x ~/.docker/cli-plugins/docker-compose

# add docker-compose to bin to use it from any directory
sudo cp ~/.docker/cli-plugins/docker-compose /usr/local/bin/docker-compose

# check that the installation was successful - the two versions should be the same
docker compose version
docker-compose --version  