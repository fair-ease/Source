#!/bin/bash
#

# if you run from the scripts folder eg: cd scripts; ./run_docker_jupyter.sh
# you need the follow row otherwise comment/delete
#cd ..

docker build --no-cache -f docker/Dockerfile_source --label docker_source --tag docker_source:1.4.2 .

#
