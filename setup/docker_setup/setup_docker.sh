#!/usr/bin/env bash
# Script that automates the downloading and configuration for docker 

# first update the system packages
sudo apt update

# install necessary dependencies 
sudo apt install apt-transport-https curl gnupg-agent ca-certificates software-properties-common -y

# add GPGK key and install docker (works for ubuntu 22.04 and 20.04)
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu focal stable"
sudo apt install docker-ce docker-ce-cli containerd.io -y

# run docker without sudo command
sudo usermod -aG docker $USER 
newgrp docker

# check that docker has been installed successfully
docker version
