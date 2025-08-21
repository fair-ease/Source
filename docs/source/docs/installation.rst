.. index:: Installation

.. _installation-ref:


************
Installation
************

.. warning::

  Work in progress !!


installation
============


.. index:: installation - github

.. _use_installation-ref:

github
------
  
the repository on  github is at: `github.com/fair-ease/Source <https://github.com/fair-ease/Source>`_

.. code-block:: bash
  
  #
  git clone https://github.com/fair-ease/Source.git

  #
  cd Source
  
  
.. index:: use

.. _use-ref:

USE
===

.. index:: use - standalone

.. _use_standalone-ref:

standalone
----------  
  
The best choice for use SOURCE in standalone is to use it in a virtual python environment; this require python 3

.. code-block:: bash
  
  # if you have installed only python 3
  python -m venv .venv
  
  # otherwise
  python3 -m venv .venv
  
.. note::
  
  The name of the folder with the virtual environment can have any name you like.
  
  In this case the name is **.venv**
  

  
it's necessary to activate the environment with a command
  
.. code-block:: bash
  
  #
  source .venv/bin/activate
  
  # just an update of pip if present
  pip install --upgrade pip
  

Some others simple and useful command for check the pip version and/or update all the pip modules installed

.. code-block:: bash
  
  # check the outdated packages
  pip list --outdated
  
  # update all the package installed
  pip list -o | cut -f1 -d' ' | tr " " "\n" | awk '{if(NR>=3)print}' | cut -d' ' -f1 | xargs -n1 pip install -U
  
  # install a specific packages
  python -m pip install copernicusmarine
  
  # for install a specific version of a package
  pip install --force-reinstall -v "copernicusmarine==1.2.4"
  
  # for install all the packages required
  pip install -r docker/requirements.txt
  
  # for install notebook
  pip install notebook
  
  

Now it's possible to install all the libraries with ``pip`` or run a python script for check and update the environment

.. code-block:: bash
  
  python src/00_check_system.py
  


.. index:: use - docker

.. _use_docker-ref:

docker
------ 
  
The first thing is to create the docker image with the script in the scripts folde.
There are some different flavours for create the image

.. code-block:: bash
  
  src/make_docker_source_python-debian_com.sh
  

  

.. index:: use - notebook

.. _use_notebook-ref:

notebook
--------

You can install ``jupyter notebook`` in the real or virtual environment with the command:

.. code-block:: bash
  
  #
  pip install notebook jupyter

  # run jupyter notebook from the root of 'Source'
  jupyter-lab
  
  
  

.. attention::
  
  Some files for the configuration must be linked or copied inside the folder notebook from the src folder
  
  eg: variables.py, datasets.py
  
  .. code:: bash
  
    # 
    ln src/variables.py notebooks/variables.py
    ln src/datasets.py notebooks/datasets.py
    
    # 
    cp src/variables.py notebooks/variables.py
    cp src/datasets.py notebooks/datasets.py
    



.. index:: use - docker-notebook

.. _use_docker-notebook-ref:

docker notebook
---------------

You can use SOURCE in a virtual environment with notebook; there is a script that create the doker image in the folder scripts. The image is based on the official 'docker-stack' image on 'https://quay.io/repository/jupyter/base-notebook' and install SOURCE with all the libraries needed for a correct execution.

.. code-block:: bash

  #
  scripts/make_docker_source_jupyter.sh


There is a script for run the notebook environment: scripts/run_docker_jupyter.sh; if you modify this file, you can chose if maintain the container or not. In any case the mapping of the root of Source in the virtual environment permit to save and maintain everythings.
If you start a new container or re-start an old one the system produce a new token; for the re-start of the previous container you need to refresh the web page or create a new one.

.. code-block:: bash

  #
  scripts/run_docker_jupyter.sh


For stop the the activity of the container you can use ```CTRL-c```

For check if the container is present or not:

.. code-block:: bash

  #
  docker ps -a

If the container 'source_jupyter' is present you can restart the container with the command:


.. code-block:: bash

  #
  docker start --attach -i source_jupyter


If you need to delete:


.. code-block:: bash

  #
  docker rm source_jupyter

If the container is present you don't have the possibility to create a new one with the same name !!
Only start or delete.

The major advantage to mantain the container is the possibility to add more library o packages to the environment but this require more knowledge about docker.





