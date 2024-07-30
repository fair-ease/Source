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



