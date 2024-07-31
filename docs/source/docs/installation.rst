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
  
  git clone https://github.com/fair-ease/Source.git

  
  
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

  

  

