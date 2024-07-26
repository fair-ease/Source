#!/bin/bash
#

# if you run from the scripts folder eg: cd scripts; ./run_docker_jupyter.sh
# you need the follow row otherwise comment/delete
#cd ..

# html
docker run --rm -v $(pwd)/docs:/docs sphinx:latest make html
# rst2pdf - work
#docker run --rm -v $(pwd)/docs:/docs sphinx:latest sphinx-build -b pdf source build/pdf
# simplepdf - work
#docker run --rm -v $(pwd)/docs:/docs sphinx:latest make simplepdf simplepdf_file_name="SOURCE.pdf"
# pdf_generate - need more configuration
#docker run --rm -v $(pwd)/docs:/docs sphinx:latest sphinx-pdf-generate source build/html

#
