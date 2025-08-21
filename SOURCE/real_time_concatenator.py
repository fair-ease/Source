# -*- coding: utf-8 -*-
import sys
import os
import shlex
import shutil
import numpy as np
import pandas as pd
import time
import calendar
from SOURCE import pointwise_datasets_concatenator, depth_aggregator, \
    time_check, duplicated_records_remover, records_monotonicity_fixer

# Global variables
sleep_time = 0.1  # seconds


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


# Functional version
def real_time_concatenator(in_csv_dir=None, in_dir_1=None, in_dir_2=None, work_dir=None, out_dir=None,
                           in_fields_standard_name_str=None, first_date_str=None, last_date_str=None, verbose=True):
    """
    Script to concatenate two pointwise datasets directories, one by one, copying also to output directory
    the files from one that are not present to the other.

    Input arguments:

        1) In situ information CSV directory: Directory with (almost) the sequent files):
            a) Devices information CSV file, with the sequent header:
                Device ID, Name;
            b) Organizations information CSV file, with the sequent header:
                Organization ID, Name, Country, Link;
            c) Variables information CSV file, with the sequent header:
                Variable ID, long_name, standard_name, units;
            d) Probes information CSV file, with the sequent header:
                Probe ID, platform_code, name, WMO, device type ID, organization ID, variable IDs,
                average longitudes, average latitudes, record starts, record ends, sampling times (ddd hh:mm:ss form),
                depth levels, quality controls, ancillary notes, weblink;

        2) First input directory;

        3) Second input directory;

        4) Base working directory;

        5) Output directory;

        6) Input variables standard_name attributes to process space separated string (OPTIONAL);

        7) Start date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format (OPTIONAL);

        8) End date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS (OPTIONAL);

        9) Verbosity switch (OPTIONAL).

    Written Oct 15, 2018 by Paolo Oliveri
    """
    if __name__ == '__main__':
        return
    start_run_time = calendar.timegm(time.gmtime())
    print('-------------------------' + ' ' + __file__ + ' -------------------------')
    print(' Local time is ' + time.strftime("%a %b %d %H:%M:%S %Z %Y", time.gmtime()))
    print(' -------------------------')
    print(' ONE TO ONE DATABASES REAL TIME CONCATENATOR.')
    print(' -------------------------')
    if in_csv_dir is None or in_dir_1 is None or in_dir_2 is None or work_dir is None or out_dir is None:
        time.sleep(sleep_time)
        print(' ERROR: 5 of 9 maximum arguments (4 optionals) not provided.', file=sys.stderr)
        print(' 1) In situ information CSV directory;', file=sys.stderr)
        print(' 2) First input datasets directory;', file=sys.stderr)
        print(' 3) Second input datasets directory;', file=sys.stderr)
        print(' 4) Working directory;', file=sys.stderr)
        print(' 5) Output datasets directory;', file=sys.stderr)
        print(' 6) Input fields standard_name space separated string to process'
              ' (for example: "sea_water_temperature sea_water_salinity");',
              ' (default: all available field for each dataset);', file=sys.stderr)
        print(' 7) (optional) First date to evaluate in YYYYMMDD format'
              ' (default: first recorded date for each dataset);', file=sys.stderr)
        print(' 8) (optional) Last date to evaluate in YYYYMMDD format'
              ' (default: last recorded date for each dataset);', file=sys.stderr)
        print(' 9) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
        time.sleep(sleep_time)
        return

    print(' Loading variables information CSV file...')
    try:
        in_variables_data = open(in_csv_dir + '/variables.csv', 'rb')
    except FileNotFoundError:
        time.sleep(sleep_time)
        print(' Error. Wrong or empty variables CSV file.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    in_variables_data = \
        pd.read_csv(in_variables_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if in_variables_data.ndim == 1:
        in_variables_data = in_variables_data[np.newaxis, :]

    variable_ids = in_variables_data[:, 0]
    variable_standard_names = in_variables_data[:, 1]

    print(' Loading probes information CSV file...')
    try:
        in_probes_data = open(in_csv_dir + '/probes.csv', 'rb')
    except FileNotFoundError:
        time.sleep(sleep_time)
        print(' Error. Wrong or empty probes CSV file.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    in_probes_data =\
        pd.read_csv(in_probes_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if in_probes_data.ndim == 1:
        in_probes_data = in_probes_data[np.newaxis, :]

    probes_platform_codes = in_probes_data[:, 1]
    probes_names = in_probes_data[:, 2]
    probes_variables_ids = [standard_names.split(';') for standard_names in in_probes_data[:, 6]]
    probes_standard_names = probes_variables_ids
    for index in range(len(probes_variables_ids)):
        for index_id in range(len(variable_ids)):
            try:
                probe_variable_index = probes_standard_names[index].index(variable_ids[index_id])
                probes_standard_names[index][probe_variable_index] = variable_standard_names[index_id]
            except ValueError:
                continue
    probes_depths = [[depths.split(' ') for depths in split_depths.split(';')]
                     for split_depths in in_probes_data[:, 12]]

    try:
        first_date = time.strptime(first_date_str, '%Y%m%d')
    except (IndexError, TypeError, ValueError):
        try:
            first_date = time.strptime(first_date_str, '%Y-%m-%d %H:%M:%S')
        except (IndexError, TypeError, ValueError):
            first_date_str = None
            first_date = None

    try:
        last_date = time.strptime(last_date_str, '%Y%m%d')
    except (IndexError, TypeError, ValueError):
        try:
            last_date = time.strptime(last_date_str, '%Y-%m-%d %H:%M:%S')
        except (IndexError, TypeError, ValueError):
            last_date_str = None
            last_date = None

    if (in_fields_standard_name_str is None) or (in_fields_standard_name_str == 'None') or \
            (in_fields_standard_name_str == '') or len(in_fields_standard_name_str.split(' ')) < 1:
        in_dir_set_1 = set([element for element in os.listdir(in_dir_1)
                            if os.path.isdir(os.path.join(in_dir_1, element))])
        in_dir_set_2 = set([element for element in os.listdir(in_dir_2)
                            if os.path.isdir(os.path.join(in_dir_2, element))])
        in_fields_standard_name_list = list(in_dir_set_1.union(in_dir_set_2))
        if (in_fields_standard_name_list is None) or len(in_fields_standard_name_list) < 1:
            time.sleep(sleep_time)
            print(' Error. Wrong input fields string or input directories.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            return
    else:
        in_fields_standard_name_list = shlex.split(in_fields_standard_name_str)

    print(' First input directory = ' + in_dir_1)
    print(' Second input directory = ' + in_dir_2)
    print(' Working directory = ' + work_dir)
    print(' Output directory = ' + out_dir)
    print(' Input variables to process standard_name string = ' + str(in_fields_standard_name_str))
    print(' First date to process = ' + str(first_date_str) +
          ' (if None it will be the first available date on each device)')
    print(' Last date to process = ' + str(last_date_str) +
          ' (if None it will be the last available date on each device)')
    print(' verbosity switch = ' + str(verbose))
    print(' -------------------------')
    print(' Starting process...')
    print(' -------------------------')

    if not os.path.exists(work_dir):
        print(' Creating working directory.')
        print(' -------------------------')
        os.makedirs(work_dir)
    if not os.listdir(work_dir):
        pass
    else:
        time.sleep(sleep_time)
        print(' Warning: existing files in working directory.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
    if not os.path.exists(out_dir):
        print(' Creating output directory.')
        print(' -------------------------')
        os.makedirs(out_dir)
    if not os.listdir(out_dir):
        pass
    else:
        time.sleep(sleep_time)
        print(' Warning: existing files or directories in output directory.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')

    if (first_date_str is not None) and (last_date_str is not None):
        if first_date > last_date:
            time.sleep(sleep_time)
            print(' Error: selected first date is greater than last date. Exiting.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            return

    for insitu_device in range(probes_platform_codes.shape[0]):
        device_platform_code = probes_platform_codes[insitu_device]
        device_name = probes_names[insitu_device]
        print(' Concatenating device ' + device_platform_code + ' (' + device_name + ') ...')
        device_standard_names = probes_standard_names[insitu_device]
        for variable_standard_name in device_standard_names:
            if variable_standard_name not in in_fields_standard_name_list:
                continue
            in_field_dir_1 = in_dir_1 + '/' + variable_standard_name + '/'
            in_field_dir_2 = in_dir_2 + '/' + variable_standard_name + '/'
            out_field_dir = out_dir + '/' + variable_standard_name + '/'
            if not os.path.exists(out_field_dir):
                print(' Creating output ' + variable_standard_name + ' directory.')
                print(' -------------------------')
                os.makedirs(out_field_dir)
            try:
                file_list_1 = [file for file in os.listdir(in_field_dir_1) if file.endswith('.nc')]
            except FileNotFoundError:
                file_list_1 = list()

            try:
                file_list_2 = [file for file in os.listdir(in_field_dir_2) if file.endswith('.nc')]
            except FileNotFoundError:
                file_list_2 = list()

            if not file_list_1 and not file_list_2:
                time.sleep(sleep_time)
                print(' Error. No processable files in both input directories for standard_name ' + variable_standard_name +
                      '.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                continue
            try:
                in_dataset = [file_name for file_name in file_list_1 if device_platform_code in file_name][0]
                in_file_1_presence = True

            except IndexError:
                in_file_1_presence = False
            try:
                in_dataset = [file_name for file_name in file_list_2 if device_platform_code in file_name][0]
                in_file_2_presence = True
            except IndexError:
                in_file_2_presence = False
            in_file_1 = in_field_dir_1 + '/' + in_dataset
            in_file_2 = in_field_dir_2 + '/' + in_dataset
            out_file = out_field_dir + in_dataset
            if in_file_1_presence and in_file_2_presence:
                merged_file = work_dir + '/' + in_dataset.replace('.nc', '_merged.nc')
                print(' Concatenating dataset ' + in_dataset + '.')
                try:
                    variable_name_index = device_standard_names.index(variable_standard_name)
                except ValueError:
                    continue
                pointwise_datasets_concatenator.pointwise_datasets_concatenator(
                    [in_file_1, in_file_2], merged_file, in_fields_standard_name_str=in_fields_standard_name_str,
                    first_date_str=first_date_str, last_date_str=last_date_str,
                    verbose=verbose)
                if not os.path.isfile(merged_file):
                    time.sleep(sleep_time)
                    print(' Warning: concatenated dataset could not be produced.', file=sys.stderr)
                    time.sleep(sleep_time)
                    print(' -------------------------')
                    continue
                probe_depth = ' '.join(probes_depths[insitu_device][variable_name_index])
                print('Output depths for this dataset: ' + probe_depth + ' meters.')
                depth_aggregated_file = merged_file.replace('_merged.nc', '_depth-aggregated.nc')
                depth_aggregator.depth_aggregator(merged_file, probe_depth, depth_aggregated_file,
                                                  first_date_str, last_date_str, verbose=verbose)
                if not os.path.isfile(depth_aggregated_file):
                    time.sleep(sleep_time)
                    print(' Warning:' + print_prefix + ' depth aggregated file not produced.', file=sys.stderr)
                    time.sleep(sleep_time)
                    print(print_prefix + ' -------------------------')
                    continue
                try:
                    merged_time_step_check = time_check.time_check(depth_aggregated_file, verbose=False)
                except ValueError:
                    time.sleep(sleep_time)
                    print(' Warning: concatenated dataset is empty.', file=sys.stderr)
                    time.sleep(sleep_time)
                    print(' -------------------------')
                    continue
                if merged_time_step_check == 1:
                    time.sleep(sleep_time)
                    print(' Warning: duplicated time records in merged dataset.', file=sys.stderr)
                    time.sleep(sleep_time)
                elif merged_time_step_check == 2:
                    time.sleep(sleep_time)
                    print(' Warning: wrong positioning records in merged dataset.', file=sys.stderr)
                    time.sleep(sleep_time)
                elif merged_time_step_check == 3:
                    time.sleep(sleep_time)
                    print(' Warning: duplicated entries and wrong positioning records in merged dataset.', file=sys.stderr)
                    time.sleep(sleep_time)
                merged_no_duplicates_file = depth_aggregated_file.replace('_depth-aggregated.nc', '_no_duplicates.nc')
                if (merged_time_step_check == 1) or (merged_time_step_check == 3):
                    print(' Removing time step duplicates.')
                    duplicated_records_remover.duplicated_records_remover(depth_aggregated_file,
                                                                          merged_no_duplicates_file, verbose=verbose)
                else:
                    shutil.copy2(depth_aggregated_file, merged_no_duplicates_file)
                merged_monotonic_file = merged_file.replace('.nc', '_monotonic.nc')
                if (merged_time_step_check == 2) or (merged_time_step_check == 3):
                    print(' Fixing time step monotonicity.')
                    records_monotonicity_fixer.records_monotonicity_fixer(merged_no_duplicates_file,
                                                                          merged_monotonic_file, verbose=verbose)
                else:
                    shutil.copy2(merged_no_duplicates_file, merged_monotonic_file)
                print(' Copying file to output directory.')
                shutil.copy2(merged_monotonic_file, out_file)
            elif not in_file_1_presence:
                print(' Dataset ' + in_dataset + ' is not present in first datasets directory.')
                print(' Copying it from second datasets directory to output directory.')
                shutil.copy2(in_file_2, out_file)
            elif not in_file_2_presence:
                print(' Dataset ' + in_dataset + ' is not present in second datasets directory.')
                print(' Copying it from first datasets directory to output directory.')
                shutil.copy2(in_file_1, out_file)

            # break  # to merge only the first archive in the list

        # break  # to merge only the first standard_name in the list

    print(' -------------------------')
    total_run_time = time.gmtime(calendar.timegm(time.gmtime()) - start_run_time)
    print(' Finished! Total elapsed time is: '
          + str(int(np.floor(calendar.timegm(total_run_time) / 86400.))) + ' days '
          + time.strftime('%H:%M:%S', total_run_time) + ' hh:mm:ss')


# Stand-alone version
if os.path.basename(sys.argv[0]) == os.path.basename(__file__):
    # Load input arguments
    try:
        in_csv_dir = sys.argv[1]
        in_dir_1 = sys.argv[2]
        in_dir_2 = sys.argv[3]
        work_dir = sys.argv[4]
        out_dir = sys.argv[5]
    except (IndexError, ValueError):
        in_csv_dir = None
        in_dir_1 = None
        in_dir_2 = None
        work_dir = None
        out_dir = None

    try:
        in_fields_standard_name_str = sys.argv[6]
    except (IndexError, ValueError):
        in_fields_standard_name_str = None

    try:
        time.strptime(sys.argv[7], '%Y%m%d')
        first_date_str = sys.argv[7]
    except (IndexError, ValueError):
        try:
            time.strptime(sys.argv[7], '%Y-%m-%d %H:%M:%S')
            first_date_str = sys.argv[7]
        except (IndexError, ValueError):
            first_date_str = None

    try:
        time.strptime(sys.argv[8], '%Y%m%d')
        last_date_str = sys.argv[8]
    except (IndexError, ValueError):
        try:
            time.strptime(sys.argv[8], '%Y-%m-%d %H:%M:%S')
            last_date_str = sys.argv[8]
        except (IndexError, ValueError):
            last_date_str = None

    try:
        verbose = string_to_bool(sys.argv[9])
    except (IndexError, ValueError):
        verbose = True

    real_time_concatenator(in_csv_dir, in_dir_1, in_dir_2, work_dir, out_dir, in_fields_standard_name_str,
                           first_date_str, last_date_str, verbose)
