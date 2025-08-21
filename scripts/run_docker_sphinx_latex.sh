#!/bin/bash
#

# if you run from the scripts folder eg: cd scripts; ./run_docker_jupyter.sh
# you need the follow row otherwise comment/delete
#cd ..

# latex
docker run --rm -v $(pwd)/docs:/docs sphinx-latex:latest make latexpdf
# rst2pdf
#docker run --rm -v $(pwd)/docs:/docs sphinx-latex:latest sphinx-build -b pdf source build/pdf
# simplepdf
#docker run --rm -v $(pwd)/docs:/docs sphinx-latex:latest make simplepdf

#
