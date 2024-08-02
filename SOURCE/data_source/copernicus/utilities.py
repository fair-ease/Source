

def split_in_daily_file (start_date):
  pass
  delta = datetime.timedelta( days = 1 )
  while start_date <= end_date:
    globals()["start_date"] += delta



def download_index (input_dir,dataset,data):
  import copernicusmarine as cm
  cm.get(
    dataset_id = dataset,
    dataset_version = data["config"]["dataset_version"],
    output_directory = input_dir,
    no_directories = data["config"]["no_directories"],
    force_download = data["config"]["force_download"],
    overwrite_output_data = data["config"]["overwrite_output_data"],
    index_parts = data["config"]["index_parts"],
    disable_progress_bar = data["config"]["disable_progress_bar"],
  )


def filter_downloads(info,filters):
  counter = 1
  string = "info["
  for key,value in filters.items():
    string_cond = key + ' = ' + value
    exec(string_cond)
    string = string + key
    if counter < len(filters):
      string = string + ' & '
      counter = counter + 1
  string = string + ']'
  return eval(string)


def download_files_list (subset):
  import os
  import numpy as np
  with open('list_files_to_download.txt','w') as list_txt:
    for i in np.arange(0,np.size(subset.file_name)):
      list_txt.write('history/'+str(os.path.join(os.path.split(subset.file_name.iloc[i])[0].split('/')[-1::][0],os.path.split(subset.file_name.iloc[i])[1]))+'\n')


def download_data (dataset,download_dir):
  import copernicusmarine as cm
  cm.get(
    dataset_id = dataset,
    dataset_part = "history",
    file_list = "list_files_to_download.txt",
    output_directory = download_dir,
    force_download = "True",
    no_directories = "True",
    overwrite_output_data = "True",
    disable_progress_bar = "False",
  )
