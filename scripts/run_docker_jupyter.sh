#!/bin/bash
#

# if you run from the scripts folder eg: cd scripts; ./run_docker_jupyter.sh
# you need the follow row otherwise comment/delete
#cd ..

# remove the container
docker run --rm -it --name source_jupyter -v ${PWD}/:/home/jovyan/ -p 8888:8888 docker_source_jupyter:1.4.2

# mantain the container
#docker run -it --name source_jupyter -v ${PWD}/:/home/jovyan/ -p 8888:8888 docker_source_jupyter:1.4.2

#
