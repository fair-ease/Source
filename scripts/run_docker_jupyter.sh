#!/bin/bash
#

# if you run from the scripts folder eg: cd scripts; ./run_docker_jupyter.sh
# you need the follow row otherwise comment/delete
#cd ..

# test - remove the container
docker run --rm -it --name source_jupyter -v ${PWD}/notebooks:/home/jovyan/notebooks/ -p 8888:8888 docker_source_jupyter:1.4.2

# OK
#docker run -it --name source_jupyter -v ${PWD}/notebooks:/home/jovyan/notebooks/ -p 8888:8888 docker_source_jupyter:1.4.2

#
