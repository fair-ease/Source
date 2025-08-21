#!/bin/bash
#

# if you run from the scripts folder eg: cd scripts; ./run_docker_jupyter.sh
# you need the follow row otherwise comment/delete
#cd ..

docker build --no-cache -f docker/Dockerfile_source_python-alpine --label docker_source_python --tag docker_source_python-alpine:1.4.2 .

#
