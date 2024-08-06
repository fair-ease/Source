
from variables import work_dir
from SOURCE import *
# import specific modules for copernicus
from SOURCE.data_source.copernicus import *

import copernicusmarine as cm

import datasets as dts
from variables import input_dir
from variables import output_dir
from variables import downld_directory as download_dir
from variables import downld_flag_bbox as flag_bbox
from variables import downld_targeted_bbox as targeted_bbox
# uncomment the variable for bbox or poly
# bbox
#from variables import downls_bbox_geom as aoi_geom
# poly
from variables import downld_poly_geom as aoi_geom
from variables import downld_check_map as map
from variables import targeted_range

#import sys,os
#print(sys.version_info)
#try:
#  test = %env
#except:
#  test = ""
#print(test)

#print("dir() :",dir())
#print()
#print("dir(tools) :",dir(tools))
#print()
#print("dir(copernicus_functions) :",dir(copernicus_functions))
#print()
#print("dir(data_source) :",dir(data_source))
#print()
#print("dir(data_source.copernicus) :",dir(data_source.copernicus))
#print()
#print("dir(utilities) :",dir(utilities))
#print()

#import sys
#sys.exit()

print("input !!")

print(input_dir)
print(output_dir)
print(cm.__version__)
print()

tools.checks.check_directory(input_dir)
tools.checks.check_directory(output_dir)
tools.checks.check_directory(download_dir)

#print(dts.datasets)
# login to copernicus marine just one time
#cm.login()

# loop over datasets
for dataset, data in dts.datasets.items():
  print("dataset :",dataset)

  # downlod index files from copernicusmarine
  data_source.copernicus.utilities.download_index(input_dir,dataset,data)

##### TO DO #####
  # check AOI on map
  # from shell will be createan image in output_dir
  #tools.utilities.shell_create_map(output_dir,aoi_geom,flag_bbox,map)
  # from jupiter
  #tools.utilities.jupiter_create_map(aoi_geom,flag_bbox,map)

  #
  info = data_source.copernicus.copernicus_functions.getIndexFilesInfo(
    data["details"],
    input_dir,
    targeted_bbox, 
    aoi_geom
  )
  #print(type(info))
  # print("info :",info.info())
  # print("info :",info.head())

  # targeted_range = '2024-05-27T00:00:00Z/2024-06-10T00:00:00Z' #set your own!
  print(targeted_range)
  info['timeOverlap'] = info.apply(
    data_source.copernicus.copernicus_functions.timeOverlap,
    targeted_range=targeted_range,
    axis=1
  )

  # apply the filter
  subset = data_source.copernicus.utilities.filter_downloads(info,data["filters"])
  #print(subset)
  print()
  #print(type(subset))

  # create download list
  data_source.copernicus.utilities.download_files_list(subset)

  # download data from copernicusmarine
  data_source.copernicus.utilities.download_data(dataset,download_dir)

  # Check the position of the data:
##### TO DO #####


