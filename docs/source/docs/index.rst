.. index:: SOURCE

.. _source-ref:


******
SOURCE
******

.. warning::

  Work in progress !!

  
The package **Sea Observations Utility for Reprocessing, Calibration and Evaluation** (SOURCE) is a Python 3.x package which has been developed within the framework of `RITMARE project <http://www.ritmare.it>`_ by the `oceanography team in Istituto Nazionale di Geofisica e Vulcanologia INGV <http://www.ingv.it>`_.

SOURCE aims to manage jobs with in situ observations and model data from Ocean General Circulation Models (OGCMs) in order to:

* Assess the quality of sea observations using original quality flags and reprocessing the data using global range check, spike removal, stuck value test and recursive statistic quality check;
* return optimized daily and hourly time series of specific EOV (Essential Ocean Variables);
* extract and aggregate in time model data at specific locations and depths;
* evaluate OGCMs accuracy in terms of difference and absolute error.

SOURCE is written in Python, an interpreted programming language highly adopted in the last decade because it is versatile, ease-to-use and fast to develop. SOURCE is developed and maintained as a module and it benefits from Python's open source utilities, such as:

* Vectorized numerical data analysis (numPy, sciPy, ObsPy and pandas);
* machine learning tools (scikit-learn);
* hierarchical data storage (netCDF-4) (HDF-5 extension);
* relational metadata storage using Structured Query Language (SQL) as management system.

SOURCE is relocatable in the sense that it can be adapted to any basin worldwide, provided that the input data follow a specific standard format.


.. index:: SOURCE - condition of use

.. _source_condition-ref:

Condition of use
================

SOURCE usability is subjected to Creative Commons CC-BY-SA-NC license.


.. index:: SOURCE - how to cite

.. _source_howto-ref:

How to cite
===========

If you use this software, please cite the following article: SOURCE: Sea Observations Utility for Reprocessing, Calibration and Evaluation. Here it is the citation:

`https://doi.org/10.3389/fmars.2021.750387 <https://doi.org/10.3389/fmars.2021.750387>`_


.. index:: SOURCE - code location

.. _source_codelocation-ref:

Code location
=============

The code development is carried out using **git**, a distributed **version control system**, which allows to track and disseminate all new builds, releases, and bug fixes. SOURCE is released for public use in the ZENODO platform at `http://doi.org/10.5281/zenodo.5008245 <http://doi.org/10.5281/zenodo.5008245>`_ with a **Creative Commons CC-BY-SA-NC** license.


.. index:: SOURCE - installation

.. _source_installation-ref:

Installation
============

User has to download the latest release in zipped version from here:

.. admonition:: zenodo

  `http://doi.org/10.5281/zenodo.5008245 <http://doi.org/10.5281/zenodo.5008245>`_
  
  
Alternatively, using git SOURCE source code can be cloned directly from a branch:

.. admonition:: github

  git clone --branch <branchname> <SOURCE-git-repo-dir>/SOURCE.git <out-dir>
  
  
After the extraction of the archive (if needed), the installation of the software is the same of a generic Python package using the setup.py installer:

.. admonition:: python

  python3 setup.py install
  
  
Please make sure to have all the prerequisites installed in order to properly use SOURCE.


.. admonition:: conda

  conda ??


.. admonition:: conda

  pip ??


.. index:: SOURCE - module structure

.. _source_module-ref:

Module structure
================

SOURCE is composed of three main modules:

* Observations module which manages in situ data pre and post processing and metadata relational database building;
* model post processing module which manages model data aggregation and interpolation at specific platforms defined by the observational module;
* Calibration and Validation (Cal/Val) module which allows to assess the quality of OGCMs versus observations.




