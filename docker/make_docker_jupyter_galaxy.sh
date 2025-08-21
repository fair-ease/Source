#!/bin/bash
#

# if you run from the scripts folder eg: cd scripts; ./run_docker_jupyter.sh
# you need the follow row otherwise comment/delete
#cd ..

#docker build --no-cache -f docker/Dockerfile_source_jupyter_python-3.10-debian --label docker_source_jupyter_python-3.10-debian --tag docker_source_jupyter_python-3.10-debian:1.4.2 .
docker build -f docker/Dockerfile_jupyter_galaxy --label docker_jupyter_galaxy --tag docker_jupyter_galaxy:1.4.2 .

#
