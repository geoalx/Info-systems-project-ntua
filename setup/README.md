# Setup

## General Comments
This folder contains scripts that automate the download and setup process of the various components that are required. The project is running on docker containers so docker is necessarry. Apart from docker, apache kafka is used as the message broker, influxdb is used to store the data in a database and finally grafana is needed to perform live data dashboards. The script that produces the fake data is written in python so either python or anaconda is required.

## Python/Anaconda Setup
* If you have installed anaconda, you can create a virtual environment with all necessary dependencies using the environment.yml file located inside [python_setup] folder. To do this use the following command (assuming you are inside the python_setup directory):
```sh
conda create -f environment.yml
```
* If you don't want to use anaconda or you don't have installed it, then you can download the required python packages using the command:
```sh
pip install -r requirements.txt
```

## Docker Setup
In order to download and setup docker, navigate to the project's home directory and then run the following command to install docker:
```sh
sudo setup/docker_setup/setup_docker.sh
```
Then, in order to also install docker-compose, run the command below:
```sh
sudo setup/docker_setup/setup_docker_compose.sh
```
At this point you should be ready to go with docker and docker-compose.

## Kafka and InfluxDB setup
After installing docker, you can setup and start all the necessary components of the project by running the <b>start_services.sh</b> script. First, open the script using any editor of your choice, and set the <b>PROJECT_DIR</b> parameter to the directory where the project is located in your computer. Then, in order to run the scripts for starting and stoppping all services from any directory, navigate to the home directoru of the project and use the command:
```sh
    cp start_services.sh /bin/start_services.sh
    cp stop_services.sh /bin/stop_servives.sh
```
Now you can start the necesssary services by running the command:
```sh
start_services.sh
```
If you want to stop the services, you can do it using the command:
```sh
stop_services.sh
```


[python_setup]: https://github.com/geoalx/Info-systems-project-ntua/tree/main/setup/python_setup
