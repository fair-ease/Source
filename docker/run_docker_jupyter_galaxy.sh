#!/bin/bash
#

# if you run from the scripts folder eg: cd scripts; ./run_docker_jupyter.sh
# you need the follow row otherwise comment/delete
#cd ..

# remove the container
docker run --rm -it --name jupyter_galaxy -v ${PWD}/:/usr/src/app/ -p 7788:8888 docker_jupyter_galaxy:1.4.2

# mantain the container
#docker run -it --name source_jupyter_3.10 -v ${PWD}/:/usr/src/app/ -p 8888:8888 docker_jupyter_python-3.10-debian:1.4.2

#
