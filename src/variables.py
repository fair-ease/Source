import sys, os
import subprocess
import datetime
from datetime import timedelta

try:
  from shapely.geometry import box, Point, Polygon
except ModuleNotFoundError:
  #if 'shapely' not in sys.modules:
  subprocess.check_call(['pip', 'install', 'shapely'])
  from shapely.geometry import box, Point, Polygon

# the work dir must be in the root of "Source"
work_dir = os.getcwd()
if 'notebooks' in work_dir :
  work_dir = os.path.dirname(os.getcwd())
sys.path.append(work_dir)

input_dir = work_dir + '/input/'
output_dir = work_dir + '/output/'

# output_directory = parent_dir+'/inputs/OBSERVATION/MO/'#It changes according to selected data-type
downld_type = 'MO'
downld_directory = input_dir + 'OBSERVATION/' + downld_type + '/'

# flag bbox or polygon
#downld_flag_bbox = True
#
downld_flag_bbox = False

# bbox
downld_lat_min = 30.00
downld_lat_max = 46.00
downld_lon_min = -6.00
downld_lon_max = 37.00

#
downld_targeted_bbox = [downld_lon_min, downld_lat_min, downld_lon_max, downld_lat_max]  # (minx, miny, maxx, maxy)

downld_bbox_lon_lat_list = [
  [downld_lon_min,downld_lat_min],
  [downld_lon_min,downld_lat_max],
  [downld_lon_max,downld_lat_max],
  [downld_lon_max,downld_lat_min]
]
downls_bbox_geom = Polygon(downld_bbox_lon_lat_list)

downld_bbox_edge = [
  [-6.00,30.00],
  [37.00,46.00]
]

# polygon
downld_poly_lon_lat_list = [
  [-18.50, 30.00],
  [-18.50, 43.20],
  [-0.80, 43.20],
  [16.00, 46.50],
  [26.80, 41.80],
  [26.80, 40.30],
  [36.50, 38.00],
  [36.50, 30.00],
]
downld_poly_geom = Polygon(downld_poly_lon_lat_list)

downld_poly_edge = [
  [30.00,-18.50],
  [46.50,36.50]
]
#
downld_check_map = [38.00, 20.00, 4] # map: y (lat), x (lon), zoom level

map_flavours = [
  "OpenStreetMap",
  "cartodb positron",
  "Stadia.StamenToner",
  "Stadia.StamenTonerBackground",
  "Stadia.StamenTonerLite",
  "Stadia.StamenTerrainBackground",
  "Esri.WorldImagery",
  "Esri.WorldTerrain",
  "Esri.WorldShadedRelief",
  "Esri.WorldPhysical",
  "Esri.OceanBasemap",
  "Esri.WorldGrayCanvas",
  "CartoDB.Positron",
  "CartoDB.PositronNoLabels",
  "CartoDB.DarkMatter",
  "CartoDB.DarkMatterNoLabels",
  "CartoDB.Voyager",
  "CartoDB.VoyagerNoLabels",
  "GeoportailFrance.orthos",
  "USGS.USTopo"
]

# numberOfFiles = 200 #we will check just a sample of files not all
max_num_downld_files = 1000

# Observation base-data path
#InDir=parent_dir+'/inputs/OBSERVATION/MO'
obs_InDir = downld_directory

# Essential Ocean Variables to be extract
obs_InFields = 'sea_water_temperature'

# Working directory
#WorkDir=parent_dir+'/inputs/OBSERVATION/work'
obs_WorkDir = input_dir + 'OBSERVATION/work/'

# Pre-processed directory
#PreDir=parent_dir+'/inputs/OBSERVATION/pre-processed'
obs_PreDir = input_dir + 'OBSERVATION/pre-processed/'

# Output directory
#OutDir=parent_dir+'/inputs/OBSERVATION/statistic/'#output'
obs_OutDir = input_dir + 'OBSERVATION/statistic/'#output'

# Climatology directory
#CliDir=parent_dir+'/inputs/OBSERVATION/history/statistic_qc_3/output/climatology'
obs_CliDir = input_dir + 'OBSERVATION/history/statistic/output/climatology/'

# Merged directory
#MerDir=parent_dir+'/inputs/OBSERVATION/merged'
obs_MerDir = input_dir + 'OBSERVATION/merged/'

# History directory: in CREATION mode you have to create the reference history:
#HisDir=parent_dir+'/inputs/OBSERVATION/history/'
obs_HisDir = input_dir + 'OBSERVATION/history/'

# Original DAC valid quality flags to use (space separated string, example: "0 1 2")
sel_QF = '0 1 2'

# Processing mode
#pr_mode = eval('True') #(False=CREATION MODE; TRUE=UPDATE MODE)
pr_mode = eval('False') #(False=CREATION MODE; TRUE=UPDATE MODE)

#Region longitude - latitude limits
Reg = '-18.125 36.5 30 46'

#Iteration number
it_nu = 3

# Temporal resolution
Temp_res = 'dm'

# Masking foreign seas switch for Mediterranean Sea processing 
msk_med = 'True'

# base url
mod_base_url='http://oceano.bo.ingv.it/erddap/griddap'

# dataset
mod_dataID = 'MedRea16_3D'

# file type
mod_ftype = '.nc'

# date start
YY_start = 2023
MM_start = 5
DD_start = 27
# date end
YY_end = 2024
MM_end = 6
DD_end = 10

start_date = datetime.date(YY_start,MM_start,DD_start)
end_date   = datetime.date(YY_end,MM_end,DD_end)

start_date_ymd = start_date.strftime("%Y%m%d")
end_date_ymd   = end_date.strftime("%Y%m%d")

start_date_iso = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
end_date_iso   = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

# time range
delta_time = 14
start_date_iso_back = end_date - timedelta(days = delta_time)
#targeted_range = str(start_date_iso) + '/' + str(end_date_iso)
targeted_range = str(start_date_iso_back.strftime("%Y-%m-%dT%H:%M:%SZ")) + '/' + str(end_date_iso)

str_date = start_date_iso_back.strftime("%Y%m%d")
end_date = end_date_ymd

# Define the depths of interest
mod_dpt_start = 1.472
mod_dpt_stop = 4.587

# Definine the domain of interest (bounding box)
mod_lat_min = 30.1875
mod_lat_max = 45.9375
mod_lon_min = -6.
mod_lon_max = 36.25
mod_id_sub = 1
mod_upper_left = [mod_lat_max, mod_lon_min]
mod_upper_right = [mod_lat_max, mod_lon_max]
mod_lower_right = [mod_lat_min, mod_lon_max]
mod_lower_left = [mod_lat_min, mod_lon_min]

# Define the variable(s) of interest
mod_variable_name = ['votemper','vosaline']

# Model data path
mod_InDir = work_dir + '/input/MODEL/'

# ??
mod_file_output = mod_InDir + mod_dataID + '_' + str(start_date_ymd) + '_' + str(end_date_ymd) + '.nc'

# Essential Ocean Variables to be extract
mod_InFields = 'sea_water_potential_temperature'

# Temporal resolution
mod_Temp_res = 'dm'   # daily mean

# Working directory
mod_WorkDir = mod_InDir + 'work/input_' + Temp_res + '/'
#if not os.path.exists(WorkDir_Model):
#    os.makedirs(WorkDir_Model)

# Post-processed directory
mod_PostDir = mod_InDir + 'post-processed' + '/'

# Temporary working directory
mod_TmpDir = mod_InDir + 'work/work_' + Temp_res + '/'

# Merged directory
mod_MerDir = mod_InDir + 'merged/' + Temp_res + '/'

# History input observation
mod_InSituHisDir = work_dir + '/input/OBSERVATION/history/'

# History model directory 
mod_HisDir = work_dir + '/input/MODEL/history/'

# If not present embedded in the model dataset, the user must provide the land-sea mask separately in NetCDF format
mod_LS_mask = work_dir + '/input/STATIC/meshmask_SYS4C-sys4a5.nc'

#Maximum acceptable horizontal distance in km
mod_Dist_max = 12

#
InsMerDir = work_dir + '/input/OBSERVATION/mergeddm/sea_water_temperature'
#
ModMerDir = work_dir + '/input/MODEL/merged/dm/sea_water_potential_temperature'




