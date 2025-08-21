from variables import work_dir
from SOURCE import *

from variables import obs_InDir as InDir
from variables import obs_InFields as InFields
from variables import obs_WorkDir as WorkDir
from variables import obs_PreDir as PreDir
from variables import obs_OutDir as OutDir
from variables import obs_CliDir as CliDir
from variables import obs_MerDir as MerDir
from variables import obs_HisDir as HisDir
from variables import sel_QF
from variables import pr_mode
from variables import str_date
from variables import end_date
from variables import Reg
from variables import it_nu
from variables import Temp_res
from variables import msk_med

print("Observations !!")
#print(WorkDir)
#print(InDir)

# check folders
tools.checks.check_directory(WorkDir)
tools.checks.check_directory(PreDir)
tools.checks.check_directory(OutDir)
tools.checks.check_directory(CliDir)
tools.checks.check_directory(MerDir)
tools.checks.check_directory(HisDir)


# to pre-processing the observation based-data in the SOURCE internal netCDF format
obs_postpro.insitu_tac_pre_processing.insitu_tac_pre_processing (
  InDir,
  InFields,
  WorkDir,
  PreDir,
  sel_QF,
  pr_mode,
  str_date,
  end_date,
  Reg,
  True
)


# to re-processing observational based-data to aggregate depth levels,remove duplicates and re-order record coordinate (time)
obs_postpro.obs_postpro.obs_postpro (
  PreDir,
  PreDir,
  InFields,
  WorkDir,
  OutDir,
  it_nu,
  '',
  str_date,
  end_date,
  Reg,
  msk_med
)


# Create history database
tools.utilities.create_history_data(OutDir,HisDir,pr_mode)


if pr_mode:
  # in update mode
  # Merge the Metadata Database
  obs_postpro.metadata_merger.metadata_merger(HisDir,OutDir,MerDir)


# Merge the Database
real_time_concatenator.real_time_concatenator (
  HisDir+Temp_res,
  OutDir+Temp_res,
  WorkDir,
  MerDir+Temp_res,
  InFields
)



