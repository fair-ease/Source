#!/bin/bash
#

# if you run from the scripts folder eg: cd scripts; ./run_docker_jupyter.sh
# you need the follow row otherwise comment/delete
#cd ..

#docker build --no-cache -f docker/Dockerfile_source_jupyter_python-debian --label docker_source_jupyter_python-debian --tag docker_source_jupyter_python-debian:1.4.2 .
docker build -f docker/Dockerfile_source_jupyter_python-3.13-debian --label docker_source_jupyter_python-3.13-debian --tag docker_source_jupyter_python-3.13-debian:1.4.2 .

#
