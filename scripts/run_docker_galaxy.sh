#!/bin/bash
#

# if you run from the scripts folder eg: cd scripts; ./run_docker_jupyter.sh
# you need the follow row otherwise comment/delete
#cd ..

docker run --rm -it --name source_jupyter_galaxy -v ${PWD}/notebook:/home/jovyan/notebook/ -p 8888:8888 docker_source_jupyter_galaxy:1.4.2

#
