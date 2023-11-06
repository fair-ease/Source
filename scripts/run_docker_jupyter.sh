#!/bin/bash
#

# if you run from the scripts folder eg: cd scripts; ./run_docker_jupyter.sh
# you need the follow row otherwise comment/delete
#cd ..

docker run --rm -it --name jupyter_source -v ${PWD}/notebook:/home/jovyan/notebook/ -p 8888:8888 docker_jupyter_source:1.4.2

#
