# SOURCE
The package **Sea Observations Utility for Reprocessing, Calibration and Evaluation** (SOURCE) is a Python 3.x package 
which has been developed within the framework of  RITMARE project (http://www.ritmare.it) by the oceanography team in 
Istituto Nazionale di Geofisica e Vulcanologia INGV (http://www.ingv.it). 

SOURCE aims to manage jobs with in situ observations and model data from **Ocean General Circulation Models** (OGCMs) in order to:

* Assess the quality of sea observations using original quality flags and reprocessing the data using global range check, spike removal, stuck value test and recursive statistic quality check;
* return optimized daily and hourly time series of specific EOV (Essential Ocean Variables);
* extract and aggregate in time model data at specific locations and depths;
* evaluate OGCMs accuracy in terms of difference and absolute error.

SOURCE is written in Python, an interpreted programming language highly adopted in the last decade because it is versatile, 
ease-to-use and fast to develop. SOURCE is developed and maintained as a module and it benefits from 
Python's open source utilities, such as:

* Vectorized numerical data analysis (**numPy**, **sciPy**, **ObsPy** and **pandas**); 
* machine learning tools (**scikit-learn**);
* hierarchical data storage (**netCDF-4**) (**HDF-5** extension);
* relational metadata storage using **Structured Query Language** (SQL) as management system.

SOURCE is relocatable in the sense that it can be adapted to any basin worldwide, provided that the input data 
follow a specific standard format.

## Condition of use
SOURCE usability is subjected to Creative Commons CC-BY-SA-NC license.

## How to cite
If you use this software, please cite the following article:
```
TODO
```
## Code location
The code development is carried out using **git**, a distributed **version control system**, 
which allows to track and disseminate all new builds, releases, and bug fixes. 
SOURCE is released for public use in the ZENODO platform at http://doi.org/10.5281/zenodo.5008245 
with a Creative Commons CC-BY-SA-NC license.

## Installation
User has to download the latest release in zipped version from here:
```
TODO
```
Alternatively, using **git** SOURCE source code can be cloned directly from a branch:
```
git clone --branch <branchname> <SOURCE-git-repo-dir>/SOURCE.git <out-dir>
```
After the extraction of the archive (if needed), the installation of the software is the same of a generic Python package 
using the **setup.py** installer:
```
python3 setup.py install
```
Please make sure to have all the prerequisites installed in order to properly use SOURCE.

## Module structure
SOURCE is composed of three main modules:
* Observations module which manages in situ data pre and post processing and metadata relational database building; 
* model post processing module which manages model data aggregation and interpolation at specific platforms defined by 
the observational module;
* calibration and Validation (Cal/Val) module which allows to assess the quality of OGCMs versus observations.

## Run options
SOURCE can be run in two modes:
* **creation** (default): A new in situ, model or Cal/Val database is created from scratch;
* **update**: A in situ, model or Cal/Val database is created using another existing **historical** database and can
also be concatenated to it to enlarge the collections.
NOTE: as the creation mode is the default, only the differences in update mode will be noted in this documentation.

## How to load
Every part of the module can loaded with its arguments in two modes:
 * directly into an existing Python environment using the **import** command;
 * launched directly using the Python terminal. The module is **OS independent**.
 
### Python environment execution
The entire module can be imported using:
```
import SOURCE
[...]
```
To load for example only the observations module one can alternatively use:
```
import SOURCE.obs_postpro
[...]
```
or
```
from SOURCE import obs_postpro
[...]
```
To load for example in situ pre processing submodule one can alternatively use:
```
import SOURCE
[...]
SOURCE.obs_postpro.insitu_tac_pre_processing.insitu.tac_pre_processing()
[...]
```
or
```
from SOURCE.obs_postpro import insitu_tac_pre_processing
[...]
insitu_tac_pre_processing.insitu.tac_pre_processing()
[...]
```
or
```
from SOURCE.obs_postpro.insitu_tac_pre_processing import insitu_tac_pre_processing
[...]
insitu.tac_pre_processing()
[...]
```
or similar, and then use the functions of the module with their arguments in the usual way.

### Note
The double **dot** import or load is needed because every function lives in a Python file that contains it 
with the same name of the function.

### Terminal execution
To execute for example the in situ pre processing sub-module one can use:
```
python3 ${SOURCE_DIR}/obs_postpro/insitu_tac_pre_processing.py ${ARGS}
```
## Help and logging
Every component of SOURCE have a small description that can be obtained while loading it without any argument. 
Further, a message of the mandatory and optional arguments will arise to help the component run correctly.

The most important modules and functions of SOURCE have also a **helper** that can be called in Python environment 
by using the **help** function. For example:
```
help(SOURCE.obs_postpro.insitu_tac_pre_processing)
```
Regardless of calling inside a Python environment or in OS terminal window, every module, sub-module or function 
outputs some information about what it is doing, called **logging**, in standard output or standard error. 
Almost every component of source has the **verbose** option that can be disabled by setting it to **False**. 

By default, the verbosity is set to **True**.

## Access and download Copernicus marine products
All Copernicus Marine Environment Monitoring Service (CMEMS) related procedures needs to access CMEMS data products 
already downloaded and stored in the machine where the software is installed (also NFS file systems are supported).
Access to CMEMS data is easy and free of charge, but user need to be registered and logged in.
Here is the [link](https://resources.marine.copernicus.eu/?option=com_sla) to CMEMS registration form.
In order to download the data by using the web browser, the following steps are needed:
* Search for the needed product(s);
* Add selected product(s) to the cart;
* On each product in the cart, enter to view the details and click on "Download product"; 
* Login to CMEMS service;
* Choose download options and then **ftp access**.
The data can also be downloaded directly using CMEMS credentials with **wget** or **curl** programs.

## in situ relational DB
The in situ relational database give full metadata information for the processed 
platforms. It consists of four files:
1. **devices.csv**, CSV table with the sequent header:
    1. Device ID;
    2. Device Name.
2. **organizations.csv**, CSV table with the sequent header:
    1. Organization ID;
    2. Organization name;
    3. Organization Country (reverse searched from url extension, empty for generic url extensions);
    4. Organization Weblink (if available).
3. **variables.csv**, CSV table with the sequent header:
    1. Variable ID;
    2. Variable long_name attribute;;
    3. Variable standard_name attribute;
    4. Variable units.
4. **probes.csv**, CSV table with the sequent header:
    1. Probe ID;
    2. Probe SOURCE platform_code attribute;
    3. Probe name (if available or matches with probes_names.csv table);
    4. Probe WMO;
    5. Device type ID;
    6. Organization ID;
    7. Variable IDs;
    8. Per variable average longitudes;
    9. Per variable average latitudes;
    10. Per variable record starts;
    11. Per variable record ends;
    12. Per variable sampling times (ddd hh:mm:ss form);
    13. Per variable depth levels;
    14. Per variable quality controls information;
    15. Per variable ancillary notes;
    16. Probe link (if available).

## Observations module
The observations module consists in 4 sub modules:
* pre processing;
* reprocessing;
* metadata DB merging;
* whole in situ DB merging.

### How to...
* Create a reprocessed in situ historical database (**creation** mode):
    1. Preprocess the data using **insitu_tac_pre_processing** from **obs_postpro** module;
    2. Reprocess the preprocessed data using **obs_postpro**.
* Create a reprocessed in situ update database (**update** mode):
    1. Preprocess the new data using **insitu_tac_pre_processing** from **obs_postpro** module;
    2. Reprocess the preprocessed data using **obs_postpro**, giving the climatology directory of the historical collection.
* Concatenate historical databases with an **update**:
    1. Merge the in situ relational DBs using **metadata_merger**;
    2. Merge the in situ databases using **real_time_concatenator** from **obs_postpro** module;
* Concatenate two preprocessed in situ databases (every mode):
    1. Merge the two in situ databases and metadata using **pointwise_datasets_concatenator**.

### CMEMS in situ TAC pre processing sub-module
```
insitu_tac_pre_processing(in_dir, in_fields_standard_name_str, work_dir, out_dir, valid_qc_values,
                          first_date_str, last_date_str, region_boundaries_str, med_sea_masking, 
                          in_instrument_types_str, names_file, verbose)
```
CMEMS in situ Thematic Assembly Center (TAC) observations pre processing.

#### Prepare CMEMS observations data sources
In order to properly pre process CMEMS service observations data,
the data itself must have been already downloaded using CMEMS service 
to a common directory.
NOTE: all the datasets needed for preprocessing have to be stored in the same directory, without subfolders.

#### **Mandatory inputs**
* **in_dir**: CMEMS downloaded in situ observations directory;
* **in_fields_standard_name_str** input variables standard_name attributes to process (space separated string), 
for example: "sea_water_temperature sea_water_practical_salinity", please read 
[CF conventions standard name table](https://cfconventions.org/Data/cf-standard-names/77/build/cf-standard-name-table.html) 
to find the correct strings to insert);
* **work_dir**: base working directory;
* **out_dir**: output directory;
* **valid_qc_values**: CMEMS DAC quality flags values to use (space separated string with 0 to 9 values, for example: "0 1 2"). 
Please read [CMEMS Product User Manual (PUM)](https://resources.marine.copernicus.eu/documents/PUM/CMEMS-INS-PUM-013.pdf)
 to properly set the flag values.

#### **Optional inputs**
* **first_date_str** (default **None**): start date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format.;
* **last_date_str** (default **None**): end date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format;
* **region_boundaries_str** (default **"-180 180 0 180"**): region longitude - latitude limits (space separated string, 
min_lon, max_lon (deg E); min_lat, max_lat (deg N)). Used to draw a LatLon box where to run;
* **med_sea_masking** (default **False**): masking foreign seas switch for Mediterranean Sea processing.
Used to remove pre processed probes outside the basin when LatLon box of the Mediterranean Sea is selected;
* **in_instrument_types_str** (default **None**): CMEMS "instrument type" metadata filter (space separated string). Used
to process only certain platform types (for example: "'mooring' 'coastal structure'". 
Please read [CMEMS Product User Manual (PUM)](https://resources.marine.copernicus.eu/documents/PUM/CMEMS-INS-PUM-013.pdf) 
to properly write the attribute string. NOTE: must put quotes outside attributes with spaces to protect them from character escaping);
* **names_file** (default internal file **probes_names.csv**): CSV table with two columns:
    1. platform_code;
    2. platform name.
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **metadata relational database**, containing
**devices.csv**, **organizations.csv**, **variables.csv** and **probes.csv**
(read specific section);

* **processing_information.csv**: CSV table with the sequent header:
    1. platform_code;
    2. institution;
    3. platform name;
    4. WMO;
    5. Platform type;
    6. Average longitude;
    7. Average latitude;
    8. processing information.
* **observations database** in netCDF-4 format, divided by variable standard names and with selected quality flags applied, 
containing:
    1. platform instantaneous latitude, longitude and depth dimension variables;
    2. platform time;
    3. DAC quality checked time series;
    4. global attributes containing original datasets and pre processing information.

#### Module wise dependencies
find_variable_name, pointwise_datasets_concatenator, time_check, time_calc

#### Observations module wise dependencies
insitu_tac_platforms_finder, insitu_tac_timeseries_extractor, data_information_calc, 
time_from_index, depth_calc, mean_variance_nc_variable, unique_values_nc_variable,
quality_check_applier

### Reprocessing sub-module
```
obs_postpro(in_csv_dir, in_dir, in_fields_standard_name_str, work_dir, out_dir,
            routine_qc_iterations, climatology_dir, first_date_str, last_date_str,
            region_boundaries_str, med_sea_masking=False, in_instrument_types_str, verbose)
```
Observational module reprocessing tool from preprocessed DB. May work in **creation**
or **update** mode, if platform climatologies are provided instead of self
computed from the processed data.

#### **Mandatory inputs**
* **in_csv_dir**: metadata relational database directory, containing
**devices.csv**, **organizations.csv**, **variables.csv** and **probes.csv**
(read specific section);
* **in_dir**: Pre processed observations netCDF database directory;
* **in_fields_standard_name_str** input variables standard_name attributes to process (space separated string), 
for example: "sea_water_temperature sea_water_practical_salinity", please read "variables.csv" from 
preprocessed metadata relational DB to find the correct strings to insert);
* **work_dir**: base working directory;
* **out_dir**: output directory;
* **routine_qc_iterations**: Routine quality check iterations number (N, integer). Options:
    1. N = -1 for original DAC quality controls only (NO QC));
    2. N = 0 for gross check quality controls only (NO_SPIKES_QC);
    3. N >= 1 for N statistic quality check iterations (STATISTIC_QC_N);

#### **Optional inputs**
* **climatology_dir**  (default **None**) Platform climatology data directory for update mode;
* **first_date_str** (default **None**): start date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format.;
* **last_date_str** (default **None**): end date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format;
* **region_boundaries_str** (default **"-180 180 0 180"**): region longitude - latitude limits (space separated string, 
min_lon, max_lon (deg E); min_lat, max_lat (deg N)). Used to draw a LatLon box where to run;
* **med_sea_masking** (default **False**): masking foreign seas switch for Mediterranean Sea processing.
Used to remove pre processed probes outside the basin when LatLon box of the Mediterranean Sea is selected;
* **in_instrument_types_str** (default **None**): "instrument type" metadata filter (space separated string). Used
to process only certain platform types (for example: "'mooring' 'coastal structure'". 
Please read the devices table to properly write the attribute string.
NOTE: must put quotes outside attributes with spaces to protect them from character escaping);
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **metadata relational database**, edited during reprocessing, containing
**devices.csv**, **organizations.csv**, **variables.csv** and **probes.csv**
(read specific section);

* **rejection_process.csv** (if routine_qc_iterations >= 0): 
CSV table with the sequent header:
    1. Probe CMEMS platform_code attribute;
    2. Variable standard_names;
    3. Total data amount for each variable;
    4. filled data for each variable;
    5. rejection amount for each variable by global range check data;
    6. rejection amount for each variable by spike test data;
    7. rejection amount for each variable by stuck value test data;
    8. (if routine_qc_iterations >= 1)
    rejection amount for each variable for each statistic phase.
* **reprocessed database** in netCDF-4 format, divided by and variable standard names and with selected quality flags applied, 
containing:
    1. probe latitude;
    2. probe longitude;
    3. field depths;
    4. time counter and boundaries;
    5. RAW, post processed and time averaged fields;
    7. global attributes containing original datasets and post process specs.
* **platform climatologies**: per-probe and per-field monthly mean climatology 
averages, standard deviation and filtered density profiles dataset.

#### Module wise dependencies
duplicated_records_remover, records_monotonicity_fixer, time_check, time_calc

#### Observations module wise dependencies
time_averager, time_series_post_processing, quality_check_applier, 
depth_aggregator, depth_calc

### Metadata merger sub-module
```
metadata_merger(base_csv_dir, update_csv_dir, merged_csv_dir,
                base_is_dominant, verbose)
```
Merger for two SOURCE relational metadata databases, with option of first one dominant.

#### **Mandatory inputs**
* **base_csv_dir**: base metadata relational database directory, containing
**devices.csv**, **organizations.csv**, **variables.csv** and **probes.csv**
(read specific section);
* **update_csv_dir**: update metadata relational database directory, containing
**devices.csv**, **organizations.csv**, **variables.csv** and **probes.csv**
(read specific section);
* **merged_csv_dir**: merged metadata relational database directory;

#### **Optional inputs**
* **base_is_dominant**  (default **True**) Base database is dominant in merge process;
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **merged metadata relational database**, containing
**devices.csv**, **organizations.csv**, **variables.csv** and **probes.csv**
(read specific section).

## Model data module
The Model data module consists allows to process CMEMS or **NEMO** ([link](https://www.nemo-ocean.eu/)) 
model data.

### How to...
* Create a model historical or update database:
    1. Process the data using **model_postpro** module;
    from **model_postpro** module using an already created in situ relational database.
* Concatenate historical databases with an **update**:
    1. Merge the in situ relational DBs using **metadata_merger** module;
    2. Merge the model databases using **real_time_concatenator** module.

### CMEMS / NEMO model processing module
```
model_postpro(in_csv_dir, in_dir, in_fields_standard_name_str, work_dir, out_dir,
              grid_observation_distance, mesh_mask_file, first_date_str, last_date_str,
              concatenate_datasets_switch, vertical_interpolation_switch, verbose)
```
Model data nearest point extractor and concatenator.

#### Prepare model data sources
There are two different data sources that SOURCE can handle:
* CMEMS model data;
* **NEMO** ocean model outputs formatted model data.

In order to properly process CMEMS service model data,
the data itself must have been already downloaded using CMEMS service (read specific section)
to a common directory. Notes:
* All the datasets needed for preprocessing have to be stored in the same directory;
* All the variables stored in the netCDF files MUST have properly set the **standard_name**
attribute, otherwise SOURCE will not find them;
* There MUST not be time duplication in model data.

In order to speedup the concatenation, one suggestion would be to split model datasets in 
the input directory into subfolders. Differently from observations data, 
model data can also be stored in subfolders. In this case, each folder MUST be named 
with the standard_name attribute of the field that the datasets contains inside. Example:
* input directory --> **sea_water_temperature** directory --> **all datasets with sea_water_temperature here**
etc.

#### **Mandatory inputs**
* **in_csv_dir**: metadata relational database directory, containing
**devices.csv**, **organizations.csv**, **variables.csv** and **probes.csv**
(read specific section);
* **in_dir**: model data input directory;
* **in_fields_standard_name_str** input variables standard_name attributes to process (space separated string), 
for example: "sea_water_temperature sea_water_practical_salinity", please read "variables.csv" from 
preprocessed metadata relational DB to find the correct strings to insert);
* **work_dir**: base working directory;
* **out_dir**: output directory;
* **grid_observation_distance**: grid-to-observation maximum acceptable distance (km);

#### **Optional inputs**
* **mesh_mask_file** (default **None**): model mesh mask file (if not provided land points are taken using model datasets themselves);
* **first_date_str** (default **None**): start date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format.;
* **last_date_str** (default **None**): end date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format;
* **concatenate_datasets_switch** (default **True**): datasets concatenation switch;
* **vertical_interpolation_switch** (default **True**): vertical interpolation switch;
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **model database** in netCDF-4 format, divided by variable standard names, containing:
    1. model nearest latitude;
    2. model nearest longitude;
    3. in situ ported model depths (or full model depths if vertical interpolation switch is off);
    4. time counter and boundaries;
    5. model data time series;
    6. global attributes containing original datasets and post process specs.

#### Module wise dependencies
ptmp_to_temp

#### Model data module wise dependencies
model_datasets_concatenator, vertical_interpolation

## Skill assessment module
How to:
* Create a model evaluation historical or update database:
    1. Process the data using **insitu_evaluation** module;
    using an already created in situ and model databases (see specific sections).
* Concatenate historical databases with an **update**:
    1. Merge the in situ relational DBs using **metadata_merger** module;
    2. Merge the evaluation databases using **real_time_concatenator** module.
```
insitu_evaluation(in_csv_dir, first_dir, second_dir, in_fields_standard_name_str, out_dir,
                  first_date_str, last_date_str, first_title_str, second_title_str)
```
compute two different databases evaluation (Cal/Val).

#### **Mandatory inputs**
* **in_csv_dir**: metadata relational database directory, containing
**devices.csv**, **organizations.csv**, **variables.csv** and **probes.csv**
(read specific section);
* **first_dir**: first data input directory (model);
* **second_dir**: second data input directory (observations);
* **in_fields_standard_name_str** input variables standard_name attributes to process (space separated string), 
for example: "sea_water_temperature sea_water_practical_salinity", please read "variables.csv" from 
preprocessed metadata relational DB to find the correct strings to insert);
* **out_dir**: output directory;

#### **Optional inputs**
* **first_date_str** (default **None**): start date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format.;
* **last_date_str** (default **None**): end date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format;
* **first_title_str** (default **first**): First database comprehension variable name title (with no spaces);
* **second_title_str** (default **second**): Second database comprehension variable name title (with no spaces).

#### **Outputs**
* **model database** in netCDF-4 format, divided by variable standard names, containing:
    1. first horizontal coordinates;
    2. second horizontal coordinates;
    3. field depths;
    4. time counter and boundaries;
    5. first time series;
    6. second time series;
    7. absolute error profile time series;
    8. difference profile time series;
    9. time averaged absolute error profile;
    10. time averaged difference profile;
    11. global attributes containing additional information.

#### Module wise dependencies
find_variable_name, time_calc

## Other modules
* KML in situ information locations creator;
* real time concatenator.

### KML creator module
```
create_probes_earth_map(in_csv_dir, in_fields_standard_name_str, out_kml_file,
                        first_date_str, last_date_str, region_boundaries_str)
```
Create Google Earth KML file with probes locations.

#### **Mandatory inputs**
* **in_csv_dir**: metadata relational database directory, containing
**devices.csv**, **organizations.csv**, **variables.csv** and **probes.csv**
(read specific section);
* **in_fields_standard_name_str** input variables standard_name attributes to process (space separated string);
* **out_kml_file**: output file;

#### **Optional inputs**
* **first_date_str** (default **None**): start date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format.;
* **last_date_str** (default **None**): end date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format;
* **region_boundaries_str** (default **"-180 180 0 180"**): region longitude - latitude limits (space separated string, 
min_lon, max_lon (deg E); min_lat, max_lat (deg N)). Used to draw a LatLon box where to run;

#### **Outputs**
* **KML file** , containing geo referenced map of probes with information.

### Real time concatenator module
```
real_time_concatenator(in_dir_1, in_dir_2, work_dir, out_dir, in_fields_standard_name_str,
                       first_date_str, last_date_str, verbose)
```
Create Google Earth KML file with probes locations.

#### **Mandatory inputs**
* **in_dir_1**: first data input directory;
* **in_dir_2**: second data input directory;
* **in_fields_standard_name_str** input variables standard_name attributes to process (space separated string);
* **work_dir**: base working directory;
* **out_dir**: output directory;

#### **Optional inputs**
* **first_date_str** (default **None**): start date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format.;
* **last_date_str** (default **None**): end date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format;
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **concatenated database** in netCDF-4 format, divided by and variable standard names, containing:
    1. probe latitude;
    2. probe longitude;
    3. field depths;
    4. concatenated time counter and boundaries;
    5. concatenated fields;
    7. global attributes containing original datasets and processing specs.

#### Module wise dependencies
duplicated_records_remover, pointwise_datasets_concatenator, records_monotonicity_fixer, time_check

## Other module-wise functions
```
duplicated_records_remover(in_file, out_file, verbose)
```
Remove duplicated records in netCDF files.

#### **Mandatory inputs**
* **in_file**: Input dataset;
* **out_file**: output dataset.

#### **Optional inputs**
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **netCDF-4 dataset** with removed duplication.

```
find_variable_name(in_file, in_variable_attribute_name, in_variable_attribute_value, verbose)
```
Find variable name given an attribute name and value.

#### **Mandatory inputs**
* **in_file**: Input dataset;
* **in_variable_attribute_name**: input variable attribute name;
* **in_variable_attribute_value**: input variable attribute value;

#### **Optional inputs**
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **netCDF variable name** associated to given attribute.

```
pointwise_datasets_concatenator(in_list, out_file, in_fields_standard_name_str,
                                first_date_str, last_date_str, verbose)
```
remove duplicated records in netCDF files.

#### **Mandatory inputs**
* **in_list**: List of model datasets to concatenate;
* **out_file**: output concatenated dataset;

#### **Optional inputs**
* **in_fields_standard_name_str** input variables standard_name attributes to process (space separated string);
* **first_date_str** (default **None**): start date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format.;
* **last_date_str** (default **None**): end date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format;
* **verbose**(default **True**): verbosity switch.
#### **Outputs**
* **netCDF-4 dataset** with concatenated variables preserving original ordering and possible duplicates.

#### Module wise dependencies
time_calc

```
ptmp_to_temp(ptmp_file, salt_file, temp_file, verbose)
```
Compute sea water in situ temperature from potential temperature and salinity.

#### **Mandatory inputs**
* **ptmp_file**: input potential temperature dataset;
* **salt_file**: input salinity dataset;
* **temp_file**: output temperature dataset;

#### **Optional inputs**
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **netCDF-4 dataset** with converted temperature field.

#### Module wise dependencies
find_variable_name

```
records_monotonicity_fixer(in_file, out_file, verbose)
```
Reorder decreasing records segments in netCDF files.

#### **Mandatory inputs**
* **in_file**: Input dataset;
* **out_file**: output dataset.

#### **Optional inputs**
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **netCDF-4 dataset** with reordered record dimension.

```
time_calc(in_file, verbose)
```
Compute most probable record sampling in netCDF files.

#### **Mandatory inputs**
* **in_file**: Input dataset.

#### **Optional inputs**
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **record time** as time delta (seconds).

```
time_check(in_file, verbose)
```
Compute time step verification in netCDF files.

#### **Mandatory inputs**
* **in_file**: Input dataset.

#### **Optional inputs**
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **status** integer from 0 to 3:
    0. record coordinate is strict monotonic increasing (no duplicates);
    1. record coordinate is monotonic increasing (duplicates);
    2. record coordinate is not monotonic increasing (no duplicates);
    3. record coordinate is not monotonic increasing (duplicates).

## Other observations module wise functions
```
data_information_calc(in_file, in_variable_standard_name, valid_qc_values, in_depth_index,
                      first_date_str, last_date_str, verbose)
```
Compute total, valid, invalid, no QC and filled data number for a depth sliced field.

#### **Mandatory inputs**
* **in_dir**: Input directory;
* **in_variable_standard_name**: input field **standard_name**;
* **valid_qc_values**: quality flags values to use (space separated string with 0 to 9 values, for example: "0 1 2");
* **in_depth_index**: input file depth index to check.

#### **Optional inputs**
* **first_date_str** (default **None**): start date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format;
* **last_date_str** (default **None**): end date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format;
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **information array**, containing
**total records number**, **no qc records number**, **last no qc record index**, 
**valid records number**, **last valid record index**, 
**filled records number**, **last filled record index**, 
**invalid records number**, **last invalid record index**.

#### Module wise dependencies
find_variable_name

```
depth_aggregator(in_file, depth_array_str, out_file, verbose)
```
Aggregate instantaneous depth variable and average horizontal coordinates

#### **Mandatory inputs**
* **in_file**: Input dataset;
* **depth_array_str** depth array string to compute aggregation;
* **out_file** output dataset.

#### **Optional inputs**
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **netCDF-4 dataset**, containing:
    1. probe average latitude;
    2. probe average longitude;
    3. field unique rounded depths;
    4. original time sampling;
    5. RAW data.

```
depth_calc(in_file, in_variable_standard_name, verbose)
```
Compute rounded depth array for non floating platforms.

#### **Mandatory inputs**
* **in_file**: Input dataset.

#### **Optional inputs**
* **in_variable_standard_name**: input field **standard_name**;
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **info array** with checks if:
   1. depth is not time dependent;
   2. increases of depth in all levels is above a specific threshold (small floating);
   3. all depth levels have more than 1% of good data;
   4. depth variable is positive (under water).
* **depth array** with platform standard depth levels.


```
insitu_tac_platforms_finder(in_list, longitude_mean, latitude_mean,
                            in_fields_standard_name_str,
                            first_date_str, last_date_str, verbose)
```
CMEMS IN SITU TAC surrounding datasets finder.

#### **Mandatory inputs**
* **in_list**: List of datasets to check;
* **longitude_mean**: average longitude for surrounding search;
* **latitude_mean**: average latitude for surrounding search;

#### **Optional inputs**
* **in_fields_standard_name_str** input variables standard_name attributes (space separated string);
* **first_date_str** (default **None**): start date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format;
* **last_date_str** (default **None**): end date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format;
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **surrounding datasets list**; list of all datasets that can be merged;
* **info array** containing the merged dataset:
    1. platform_code;
    2. platform name;
    3. WMO;
    4. device type;
    5. data provider;
    6. web link.


```
insitu_tac_timeseries_extractor(in_file, in_variable_standard_name, out_file,
                                first_date_str, last_date_str, verbose)
```
Extract a field from CMEMS INSITU TAC insitu observations netCDF files.

#### **Mandatory inputs**
* **in_file**: Input dataset;
* **in_variable_standard_name**: input field **standard_name**;
* **out_file**: output dataset.

#### **Optional inputs**
* **first_date_str** (default **None**): start date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format;
* **last_date_str** (default **None**): end date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format;
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **netCDF-4 dataset**, containing:
    1. platform instantaneous latitude, longitude and depth dimension variables;
    2. platform time;
    3. selected field and Q/C variables;
    4. global attributes containing original datasets and pre processing information.

#### Module wise dependencies
find_variable_name

```
mean_variance_nc_variable(in_file, in_variable_standard_name, out_file,
                                first_date_str, last_date_str, verbose)
```
Compute variable average and variance over time dimension.

#### **Mandatory inputs**
* **in_file**: Input dataset;
* **in_variable_standard_name**: input field **standard_name**.

#### **Optional inputs**
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **field average**;
* **field variance**.

#### Module wise dependencies
find_variable_name

```
quality_check_applier(in_file, in_variable_standard_name, valid_qc_values, out_file,
                      iteration, verbose)
```
Apply a specific quality check to a variable stored in a netCDF dataset.

#### **Mandatory inputs**
* **in_file**: Input file;
* **in_variable_standard_name**: input field **standard_name**;
* **valid_qc_values**: quality flags values to use (space separated string with 0 to 9 values, for example: "0 1 2");
* **out_file**: output file;

#### **Optional inputs**
* **iteration** (default **-1**): iteration number for reprocessed datasets Q/C application;
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **netCDF-4 dataset**, containing:
    1. platform instantaneous latitude, longitude and depth dimension variables;
    2. platform time;
    3. DAC quality checked time series;
    4. global attributes containing original datasets and pre processing information.

#### Module wise dependencies
find_variable_name

```
time_averager(in_file, average_step_str, out_file, in_variable_standard_name,
              half_time_step_shift, verbose)
```
Compute custom weighted mean in oversampled observation files.

#### **Mandatory inputs**
* **in_file**: Input dataset;
* **average_step_str**: time average to compute. Accepted values:
    1. hh:mm:ss;
    2. MM (months number, 1 to 12. 12: yearly average).
* **out_file**: output dataset.

#### **Optional inputs**
* **variable_standard_name**: input field **standard_name**;
* **half_time_step_shift** (default **False**): half time step average shifting;
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **netCDF-4 dataset**, containing:
    1. probe latitude;
    2. probe longitude;
    3. field depths;
    4. time counter and boundaries;
    5. time averaged fields;
    7. global attributes containing original datasets and post process specs.

### Module wise dependencies
find_variable_name

```
time_from_index(in_file, in_index, verbose)
```
Find time value from index from a netCDF observation file.

#### **Mandatory inputs**
* **in_file**: Input dataset;
* **in_index**: input file time index to find.

#### **Optional inputs**
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **time** at the corresponded index.

```
time_series_post_processing(in_file, in_variable_standard_name, update_mode,
                            routine_qc_iterations, monthly_mean_climatology_file,
                            out_file, verbose)
```
Reprocess a field in insitu observations netCDF files.

#### **Mandatory inputs**
* **in_file**: Input dataset;
* **variable_standard_name**: input field **standard_name**;
* **update_mode**: creation or update mode execution switch;
* **routine_qc_iterations**: Routine quality check iterations number (N, integer). Options:
    1. N = -1 for original DAC quality controls only (NO QC));
    2. N = 0 for gross check quality controls only (NO_SPIKES_QC);
    3. N >= 1 for N statistic quality check iterations (STATISTIC_QC_N);
* **climatology_file**: historical climatology file:
    1. input in update mode;
    2. output in creation mode;
    3. ignored if iterations number is equal to 0;
* **out_file**: Input dataset;

#### **Optional inputs**
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **netCDF-4 dataset**, containing:
    1. probe latitude;
    2. probe longitude;
    3. field depths;
    4. time counter;
    5. RAW field data;
    6. Reprocessing Quality Flag (same size of the data + N = routine_qc_iterations);
    7. Filtered data;
    8. Monthly climatology, standard deviation, trend and density for filtered data;
    9. Samples for density function;
    10. Rejected data percentage profile;
    11. global attributes containing original datasets and post process specs.
* if **update_mode**, platform climatology netCDF-4 dataset containing:
    1. probe latitude;
    2. probe longitude;
    3. field depths;
    4. Climatological time and boundaries;
    7. Monthly climatology, standard deviation, trend and density for filtered data;
    8. Samples for density function;
    9. Rejected data percentage profile;
    10. global attributes containing original datasets and post process specs.

### Module wise dependencies
find_variable_name, time_calc

```
unique_values_nc_variable(in_file, in_variable_standard_name, verbose)
```
Compute variable unique values over time dimension.

#### **Mandatory inputs**
* **in_file**: Input dataset;
* **in_variable_standard_name**: input field **standard_name**;

#### **Optional inputs**
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **unique values** of the selected field.

### Module wise dependencies
find_variable_name

```
rejection_statistics(rejection_csv_file, routine_qc_iterations, out_statistics_file,
                     verbose)
```
Compute SOURCE global rejection statistics rejection_process.csv produced by obs_postpro.py.

#### **Mandatory inputs**
* **rejection_csv_file**: Rejection CSV file produced by obs_postpro.py;
* **routine_qc_iterations**: Routine quality check iterations number (N, integer). Options:
    2. N = 0 for gross check quality controls only (GROSS_QC);
    3. N >= 1 for N statistic quality check iterations (STATISTIC_QC_N);
* **out_statistics_file**: output statistics CSV file.

#### **Optional inputs**
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **CSV file** with global platforms statistics.

## Other model data module wise functions
```
model_datasets_concatenator(in_list, in_variable_standard_name, in_csv_file, out_dir,
                            grid_observation_distance, out_variable_standard_name, mesh_mask_file,
                            first_date_str, last_date_str, verbose)
```
Process model data to in situ platform locations.

#### **Mandatory inputs**
* **in_list**: List of model datasets to concatenate;
* **in_variable_standard_name**: input field **standard_name**;
* **in_csv_file**: CSV location table with columns:
    1. output concatenated dataset file basename;
    2. longitude of location;
    3. latitude of location;
* **out_dir**: output directory;
* **grid_observation_distance**: grid-to-observation maximum acceptable distance (km);

#### **Optional inputs**
* **out_variable_standard_name** (default **None**): input field **standard_name** for renaming;
* **mesh_mask_file** (default **None**): model mesh mask file (if not provided land points are taken using model datasets themselves);
* **first_date_str** (default **None**): start date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format.;
* **last_date_str** (default **None**): end date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format;
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **model database** in netCDF-4 format, divided by variable standard names, containing:
    1. model nearest latitude;
    2. model nearest longitude;
    3. full model depths;
    4. time counter and boundaries;
    5. model data time series;
    6. global attributes containing original datasets and post process specs.
#### Module wise dependencies
find_variable_name

```
vertical_interpolation(in_file, depth_array_str, out_file, verbose)
```
Process model data to in situ platform locations.

#### **Mandatory inputs**
* **in_file**: Input dataset;
* **depth_array_str** depth array string to compute interpolation;
* **out_file**: output file;

#### **Optional inputs**
* **verbose**(default **True**): verbosity switch.

#### **Outputs**
* **netCDF-4 dataset**, containing:
    1. model nearest latitude;
    2. model nearest longitude;
    3. in situ ported model depths;
    4. time counter and boundaries;
    5. model data time series;
    6. global attributes containing original datasets and post process specs.
