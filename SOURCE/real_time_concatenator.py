# -*- coding: utf-8 -*-
import sys
import os
import shutil
import numpy as np
import time
import calendar
from SOURCE import duplicated_records_remover, pointwise_datasets_concatenator, records_monotonicity_fixer, time_check

# Global variables
sleep_time = 0.1  # seconds


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


# Load input arguments
try:
    in_dir_1 = sys.argv[1]
    in_dir_2 = sys.argv[2]
    work_dir = sys.argv[3]
    out_dir = sys.argv[4]
except (IndexError, ValueError):
    in_dir_1 = None
    in_dir_2 = None
    work_dir = None
    out_dir = None

try:
    in_fields_standard_name_str = sys.argv[5]
except (IndexError, ValueError):
    in_fields_standard_name_str = None

try:
    time.strptime(sys.argv[6], '%Y%m%d')
    first_date_str = sys.argv[6]
except (IndexError, ValueError):
    try:
        time.strptime(sys.argv[6], '%Y-%m-%d %H:%M:%S')
        first_date_str = sys.argv[6]
    except (IndexError, ValueError):
        first_date_str = None

try:
    time.strptime(sys.argv[7], '%Y%m%d')
    last_date_str = sys.argv[7]
except (IndexError, ValueError):
    try:
        time.strptime(sys.argv[7], '%Y-%m-%d %H:%M:%S')
        last_date_str = sys.argv[7]
    except (IndexError, ValueError):
        last_date_str = None

try:
    verbose = string_to_bool(sys.argv[8])
except (IndexError, ValueError):
    verbose = True


# Functional version
def real_time_concatenator(in_dir_1=None, in_dir_2=None, work_dir=None, out_dir=None, in_fields_standard_name_str=None,
                           first_date_str=None, last_date_str=None, verbose=True):
    """
    Script to concatenate two pointwise datasets directories, one by one, copying also to output directory
    the files from one that are not present to the other.

    Input arguments:

        1) First input directory;

        2) Second input directory;

        3) Base working directory;

        4) Output directory;

        5) Input variables standard_name attributes to process space separated string (OPTIONAL);

        6) Start date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS format (OPTIONAL);

        7) End date in YYYYMMDD or in YYYY-MM-DD HH:MM:SS (OPTIONAL);

        8) Verbosity switch (OPTIONAL).

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
    if in_dir_1 is None or in_dir_2 is None or work_dir is None or out_dir is None:
        time.sleep(sleep_time)
        print(' ERROR: 4 of 8 maximum arguments (4 optionals) not provided.', file=sys.stderr)
        print(' 1) First input datasets directory;', file=sys.stderr)
        print(' 2) Second input datasets directory;', file=sys.stderr)
        print(' 3) Working directory;', file=sys.stderr)
        print(' 4) Output datasets directory;', file=sys.stderr)
        print(' 5) (optional) Input fields standard_name space separated string to process'
              ' (for example: "sea_water_temperature sea_water_salinity");',
              ' (default: all available field for each dataset);', file=sys.stderr)
        print(' 6) (optional) First date to evaluate in YYYYMMDD format'
              ' (default: first recorded date for each dataset);', file=sys.stderr)
        print(' 7) (optional) Last date to evaluate in YYYYMMDD format'
              ' (default: last recorded date for each dataset);', file=sys.stderr)
        print(' 8) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
        time.sleep(sleep_time)
        return

    if first_date_str is not None:
        try:
            first_date = time.strptime(first_date_str, '%Y%m%d')
        except ValueError:
            first_date = time.strptime(first_date_str, '%Y-%m-%d %H:%M:%S')

    if last_date_str is not None:
        try:
            last_date = time.strptime(last_date_str, '%Y%m%d')
        except ValueError:
            last_date = time.strptime(last_date_str, '%Y-%m-%d %H:%M:%S')

    if (in_fields_standard_name_str == '') or (in_fields_standard_name_str == 'None'):
        in_fields_standard_name_str = None

    print(' First input directory = ' + in_dir_1)
    print(' Second input directory = ' + in_dir_2)
    print(' Working directory = ' + work_dir)
    print(' Output directory = ' + out_dir)
    print(' Input variables to process standard_name string = ' + str(in_fields_standard_name_str) +
          ' (if None all available fields will be taken for each dataset)')
    print(' First date to process = ' + str(first_date_str) +
          ' (if None it will be the first available date on each device)')
    print(' Last date to process = ' + str(last_date_str) +
          ' (if None it will be the last available date on each device)')
    print(' verbosity switch = ' + str(verbose))
    print(' -------------------------')
    print(' Starting process...')
    print(' -------------------------')

    try:
        file_list_1 = [file for file in os.listdir(in_dir_1) if file.endswith('.nc')]
    except FileNotFoundError:
        file_list_1 = list()

    try:
        file_list_2 = [file for file in os.listdir(in_dir_2) if file.endswith('.nc')]
    except FileNotFoundError:
        file_list_2 = list()

    if not file_list_1 and not file_list_2:
        time.sleep(sleep_time)
        print(' Error. No processable files in both input directories.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return

    union_file_list = list(set(file_list_1) | set(file_list_2))

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

    if in_fields_standard_name_str is not None:
        in_fields_standard_name_list = in_fields_standard_name_str.split(' ')
        if not in_fields_standard_name_list or (in_fields_standard_name_list == list()):
            time.sleep(sleep_time)
            print(' Error: wrong or empty input variables string.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            return

    if first_date_str is not None:
        try:
            first_date = time.strptime(first_date_str, '%Y%m%d')
        except ValueError:
            first_date = time.strptime(first_date_str, '%Y-%m-%d %H:%M:%S')

    if last_date_str is not None:
        try:
            last_date = time.strptime(last_date_str, '%Y%m%d')
        except ValueError:
            last_date = time.strptime(last_date_str, '%Y-%m-%d %H:%M:%S')

    if (first_date_str is not None) and (last_date_str is not None):
        if first_date > last_date:
            time.sleep(sleep_time)
            print(' Error: selected first date is greater than last date. Exiting.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
            return

    for in_dataset in union_file_list:
        in_file_1 = in_dir_1 + '/' + in_dataset
        in_file_1_presence = True
        if not os.path.isfile(in_file_1):
            in_file_1_presence = False
        in_file_2 = in_dir_2 + '/' + in_dataset
        in_file_2_presence = True
        if not os.path.isfile(in_file_2):
            in_file_2_presence = False
        out_file = out_dir + '/' + in_dataset
        if in_file_1_presence and in_file_2_presence:
            merged_file = work_dir + '/' + in_dataset.replace('.nc', '_merged.nc')
            print(' Concatenating dataset ' + in_dataset + '.')
            pointwise_datasets_concatenator.pointwise_datasets_concatenator(
                [in_file_1, in_file_2], merged_file, in_fields_standard_name_str=in_fields_standard_name_str,
                first_date_str=first_date_str, last_date_str=last_date_str, verbose=verbose)
            if not os.path.isfile(merged_file):
                time.sleep(sleep_time)
                print(' Warning: concatenated dataset could not be produced.', file=sys.stderr)
                time.sleep(sleep_time)
                print(' -------------------------')
                continue
            merged_time_step_check = time_check.time_check(merged_file, verbose=False)
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
            merged_no_duplicates_file = merged_file.replace('.nc', '_no_duplicates.nc')
            if (merged_time_step_check == 1) or (merged_time_step_check == 3):
                print(' Removing time step duplicates.')
                duplicated_records_remover.duplicated_records_remover(merged_file, merged_no_duplicates_file,
                                                                      verbose=verbose)
            else:
                shutil.copy2(merged_file, merged_no_duplicates_file)
            merged_monotonic_file = merged_file.replace('.nc', '_monotonic.nc')
            if (merged_time_step_check == 2) or (merged_time_step_check == 3):
                print(' Fixing time step monotonicity.')
                records_monotonicity_fixer.records_monotonicity_fixer(merged_no_duplicates_file, merged_monotonic_file,
                                                                      verbose=verbose)
            else:
                shutil.copy2(merged_no_duplicates_file, merged_monotonic_file)
            print(' Copying file to output directory.')
            shutil.copy2(merged_monotonic_file, out_file)
        elif not in_file_1_presence:
            time.sleep(sleep_time)
            print(' Warning. Dataset ' + in_dataset + ' is not present in first datasets directory.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' Copying it from second datasets directory to output directory.')
            shutil.copy2(in_file_2, out_file)
        elif not in_file_2_presence:
            time.sleep(sleep_time)
            print(' Warning. Dataset ' + in_dataset + ' is not present in second datasets directory.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' Copying it from first datasets directory to output directory.')
            shutil.copy2(in_file_1, out_file)

        # break  # to post process only the first archive in the list

    print(' -------------------------')
    total_run_time = time.gmtime(calendar.timegm(time.gmtime()) - start_run_time)
    print(' Finished! Total elapsed time is: '
          + str(int(np.floor(calendar.timegm(total_run_time) / 86400.))) + ' days '
          + time.strftime('%H:%M:%S', total_run_time) + ' hh:mm:ss')


# Stand alone version
if os.path.basename(sys.argv[0]) == os.path.basename(__file__):
    # Load input arguments
    try:
        in_dir_1 = sys.argv[1]
        in_dir_2 = sys.argv[2]
        work_dir = sys.argv[3]
        out_dir = sys.argv[4]
    except (IndexError, ValueError):
        in_dir_1 = None
        in_dir_2 = None
        work_dir = None
        out_dir = None

    try:
        in_fields_standard_name_str = sys.argv[5]
    except (IndexError, ValueError):
        in_fields_standard_name_str = None

    try:
        time.strptime(sys.argv[6], '%Y%m%d')
        first_date_str = sys.argv[6]
    except (IndexError, ValueError):
        try:
            time.strptime(sys.argv[6], '%Y-%m-%d %H:%M:%S')
            first_date_str = sys.argv[6]
        except (IndexError, ValueError):
            first_date_str = None

    try:
        time.strptime(sys.argv[7], '%Y%m%d')
        last_date_str = sys.argv[7]
    except (IndexError, ValueError):
        try:
            time.strptime(sys.argv[7], '%Y-%m-%d %H:%M:%S')
            last_date_str = sys.argv[7]
        except (IndexError, ValueError):
            last_date_str = None

    try:
        verbose = string_to_bool(sys.argv[8])
    except (IndexError, ValueError):
        verbose = True

    real_time_concatenator(in_dir_1, in_dir_2, work_dir, out_dir, in_fields_standard_name_str,
                           first_date_str, last_date_str, verbose)
