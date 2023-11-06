#!/bin/bash
#

# if you run from the scripts folder eg: cd scripts; ./run_docker_jupyter.sh
# you need the follow row otherwise comment/delete
#cd ..

docker build --no-cache -f docker/Dockerfile_jupyter --label docker_jupyter_source --tag docker_jupyter_source:1.4.2 .

#

