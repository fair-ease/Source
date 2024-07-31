.. index:: Configurations

.. _configurations-ref:


**************
Configurations
**************

.. warning::

  Work in progress !!

  

.. index:: Configurations - variables

.. _conf-variables-ref:


variables
=========

The idea is to keep all the variables configured in a single file to avoid making mistakes as the output of one module coincides with the input of another.

Being a python module it will be possible to import only the variables needed for the specific module, with a variable name adapted to the specific module

for example:

.. code-block:: python
  
  work_dir = os.getcwd()
  ...
  output_dir = work_dir + '/output/'
  ...
  downld_type = 'MO'
  downld_directory = input_dir + 'OBSERVATION/' + downld_type + '/'
  ...
  obs_InDir = downld_directory
  ...
  
  


All the variables are configured in the file :file:`src/variables.py`.


.. code-block:: python2
  
  #
  import sys, os
  import datetime
  from datetime import timedelta
  from shapely.geometry import box, Point, Polygon
  
  work_dir = os.getcwd()
  #work_dir = os.path.dirname(os.getcwd())
  sys.path.append(work_dir)
  
  input_dir = work_dir + '/input/'
  output_dir = work_dir + '/output/'
  
  downld_type = 'MO'
  downld_directory = input_dir + 'OBSERVATION/' + downld_type + '/'
  
  ...
  
  #
  InsMerDir = work_dir + '/input/OBSERVATION/mergeddm/sea_water_temperature'
  #
  ModMerDir = work_dir + '/input/MODEL/merged/dm/sea_water_potential_temperature'
  



.. index:: Configurations - datasets

.. _conf-datasets-ref:


datasets
========

The dataset is configured inside the file :file:`src/datasets.py`, it's a python dictionary structure that contain the name of the dataset and some others dictionary with other elements that permit to define a more detailed information for the use of the dataset.

The default content of the file :file:`src/datasets.py`:

.. code-block:: python2

  #
  datasets = {
    "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr" : {
      "config" : {
        "dataset_version" : "202311",
        "force_download" : True,
        "overwrite_output_data" : True,
        "index_parts" : True,
        "disable_progress_bar" : False,
        "no_directories" : True,
      },
      "details" : {
        "product" : 'INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030', # name of the In S>
        "name" : 'cmems_obs-ins_med_phybgcwav_mynrt_na_irr', # name of the dataset av>
        "index_files" : ['index_history.txt'], # file describing the content of the h>
        "index_platform" : 'index_platform.txt', # files describing the netwotk of pl>
      },
      "filters" : {
        "condition_1" : "info['poligonOverlap'] == True",
        "condition_2" : "info['timeOverlap'] == True",
        "condition_3" : "info['data_type'] == 'MO'",
        "condition_4" : "info['file_type'] == 'TS'",
        "condition_5" : "info['parameters_y'].str.contains('DEPH') == True",
      },
    },
  }





