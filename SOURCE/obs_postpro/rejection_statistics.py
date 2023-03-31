# -*- coding: utf-8 -*-
import sys
import os
import pandas as pd
import numpy as np
import time
import calendar

# Global variables
sleep_time = 0.1  # seconds


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


# Functional version
def rejection_statistics(rejection_csv_file=None, routine_qc_iterations=None, out_statistics_file=None, verbose=True):
    """
    Script to compute SOURCE global rejection statistics rejection_process.csv produced by obs_postpro.py

    Input arguments:

        1) Rejection CSV file, with the sequent header:
                SOURCE_platform_code, variable standard_names, data_total, filled_data,
                global_range_check_rejection, spike_test_rejection, statistics_rejections;

        2) Routine QC iterations;

        3) Output CSV file name;

        4) verbosity switch (OPTIONAL).

    Output:

        1) Global platforms statistics rejection CSV file.

    Written Jun 24, 2021 by Paolo Oliveri
    """
    # if __name__ == '__main__':
    #     return
    start_run_time = calendar.timegm(time.gmtime())
    print('-------------------------' + ' ' + __file__ + ' -------------------------')
    print(' Local time is ' + time.strftime("%a %b %d %H:%M:%S %Z %Y", time.gmtime()))
    print(' -------------------------')
    print(' Compute SOURCE global statistics from rejection_process.csv.')
    print(' -------------------------')
    if rejection_csv_file is None or routine_qc_iterations is None or out_statistics_file is None:
        time.sleep(sleep_time)
        print(' ERROR: 3 of 4 maximum arguments (1 optional) not provided.', file=sys.stderr)
        print(' 1) rejection_process.py CSV file;', file=sys.stderr)
        print(' 2) Routine quality check iterations number (N, integer), options:', file=sys.stderr)
        print('     a) N = 0 for gross check quality controls only (GROSS_QC);', file=sys.stderr)
        print('     b) N >= 1 for N statistic quality check iterations (STATISTIC_QC_N);', file=sys.stderr)
        print(' 3) output statistics CSV file;', file=sys.stderr)
        print(' 4) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
        time.sleep(sleep_time)
        return

    try:
        if routine_qc_iterations < 0:
            time.sleep(sleep_time)
            print(' Error: Routine quality check iterations number must be an integer greater or equal than 0.',
                  file=sys.stderr)
            time.sleep(sleep_time)
            return
    except TypeError:
        time.sleep(sleep_time)
        print(' Error: Routine quality check iterations number must be an integer greater or equal than 0.',
              file=sys.stderr)
        time.sleep(sleep_time)
        return

    print(' rejection_process.csv file = ' + rejection_csv_file)
    print(' Statistic quality check iterations number = ' + str(routine_qc_iterations))
    print(' Output statistics CSV file = ' + out_statistics_file)
    print(' verbosity switch = ' + str(verbose))
    print(' -------------------------')
    print(' Starting process...')
    print(' -------------------------')

    print(' Loading rejection_process.csv...')
    try:
        rejection_csv_data = open(rejection_csv_file, 'rb')
    except FileNotFoundError:
        time.sleep(sleep_time)
        print(' Error. Wrong or empty rejection_process.csv file.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return
    rejection_csv_data =\
        pd.read_csv(rejection_csv_data, na_filter=False, dtype=object, quotechar='"', delimiter=',').values
    if rejection_csv_data.ndim == 1:
        rejection_csv_data = rejection_csv_data[np.newaxis, :]

    rejection_csv_standard_names = [standard_names.split(';') for standard_names in rejection_csv_data[:, 1]]
    unique_standard_names = \
        list({standard_name for standard_names in rejection_csv_standard_names for standard_name in standard_names})
    rejection_csv_data_totals = [data_totals.split(';') for data_totals in rejection_csv_data[:, 2]]
    rejection_csv_data_totals_array = np.zeros([len(rejection_csv_data_totals),
                                                len(max(rejection_csv_data_totals, key=lambda x: len(x)))])
    for i, j in enumerate(rejection_csv_data_totals):
        rejection_csv_data_totals_array[i][0:len(j)] = j
    rejection_csv_filled_data = [filled_data.split(';') for filled_data in rejection_csv_data[:, 3]]
    rejection_csv_filled_data_array = np.zeros([len(rejection_csv_filled_data),
                                                len(max(rejection_csv_filled_data, key=lambda x: len(x)))])
    for i, j in enumerate(rejection_csv_filled_data):
        rejection_csv_filled_data_array[i][0:len(j)] = j

    rejection_csv_range_check = [range_check.split(';') for range_check in rejection_csv_data[:, 4]]
    rejection_csv_range_check_array = np.zeros([len(rejection_csv_range_check),
                                                len(max(rejection_csv_range_check, key=lambda x: len(x)))])
    for i, j in enumerate(rejection_csv_range_check):
        rejection_csv_range_check_array[i][0:len(j)] = j
    rejection_csv_spike_test = [spike_test.split(';') for spike_test in rejection_csv_data[:, 5]]
    rejection_csv_spike_test_array = np.zeros([len(rejection_csv_spike_test),
                                               len(max(rejection_csv_spike_test, key=lambda x: len(x)))])
    for i, j in enumerate(rejection_csv_spike_test):
        rejection_csv_spike_test_array[i][0:len(j)] = j
    rejection_csv_stuck_value_test = [stuck_value_test.split(';') for stuck_value_test in rejection_csv_data[:, 6]]
    rejection_csv_stuck_value_test_array = \
        np.zeros([len(rejection_csv_stuck_value_test),
                  len(max(rejection_csv_stuck_value_test, key=lambda x: len(x)))])
    for i, j in enumerate(rejection_csv_stuck_value_test):
        rejection_csv_stuck_value_test_array[i][0:len(j)] = j
    if routine_qc_iterations > 0:
        rejection_csv_statistic_test_array = dict()
        for iteration in range(1, routine_qc_iterations + 1):
            rejection_csv_statistic_test = \
                [statistic_test.split(';') for statistic_test in rejection_csv_data[:, 6 + iteration]]
            rejection_csv_statistic_test_array[iteration] = \
                np.zeros([len(rejection_csv_statistic_test),
                          len(max(rejection_csv_statistic_test, key=lambda x: len(x)))])
            for i, j in enumerate(rejection_csv_statistic_test):
                rejection_csv_statistic_test_array[iteration][i][0:len(j)] = j

    out_statistics_data = np.array([['standard_names', 'filled_data']], dtype=object)
    out_statistics_data = np.append(out_statistics_data, np.array([['range_check_rejection_percentage']]), axis=1)
    out_statistics_data = np.append(out_statistics_data, np.array([['spike_test_rejection_percentage']]), axis=1)
    out_statistics_data = np.append(out_statistics_data, np.array([['stuck_value_rejection_percentage']]), axis=1)
    if routine_qc_iterations >= 1:
        for iteration in range(1, routine_qc_iterations + 1):
            out_statistics_data = \
                np.append(out_statistics_data, np.array([['statistic_rejection_' + str(iteration) +
                                                          '_percentage']]), axis=1)
    print(' Writing statistics CSV file header...')
    np.savetxt(out_statistics_file, out_statistics_data, fmt='"%s"', delimiter=',', comments='')

    if verbose:
        print(' -------------------------')
        print(' Local time is ' + time.strftime("%Y-%m-%d %H:%M:%S %Z", time.gmtime()))

    print(' -------------------------')
    print(' Analyzing rejection_process.csv file ' + rejection_csv_file)
    out_statistics_filled_data = list()
    out_statistics_range_check = list()
    out_statistics_spike_test = list()
    out_statistics_stuck_value_test = list()
    out_statistics_statistic_test = dict()
    for index in range(len(unique_standard_names)):
        print(' -------------------------')
        data_total = np.sum(rejection_csv_data_totals_array[:, index])
        filled_data = np.sum(rejection_csv_filled_data_array[:, index])
        out_statistics_filled_data.append(np.around(filled_data / data_total * 100, decimals=2))
        range_check = np.sum(rejection_csv_range_check_array[:, index])
        out_statistics_range_check.append(np.around(range_check / (data_total - filled_data) * 100, decimals=2))
        spike_test = np.sum(rejection_csv_spike_test_array[:, index])
        out_statistics_spike_test.append(np.around(spike_test / (data_total - filled_data) * 100, decimals=2))
        stuck_value_test = np.sum(rejection_csv_stuck_value_test_array[:, index])
        out_statistics_stuck_value_test.append(np.around(stuck_value_test / (data_total - filled_data) * 100, decimals=2))
        if routine_qc_iterations > 0:
            for iteration in range(1, routine_qc_iterations + 1):
                if index == 0:
                    out_statistics_statistic_test[iteration] = list()
                statistic_test = np.sum(rejection_csv_statistic_test_array[iteration][:, index])
                out_statistics_statistic_test[iteration].append(
                    np.around(statistic_test / (data_total - filled_data) * 100, decimals=2))

    print(' -------------------------')
    print(' Building statistics CSV file...')
    print(' -------------------------')
    out_standard_names = ';'.join(unique_standard_names)
    out_filled_data = ';'.join(map(str, out_statistics_filled_data))
    out_range_check = ';'.join(map(str, out_statistics_range_check))
    out_spike_test = ';'.join(map(str, out_statistics_spike_test))
    out_stuck_value_test = ';'.join(map(str, out_statistics_stuck_value_test))
    out_statistics_line = np.array([[out_standard_names, out_filled_data,
                                     out_range_check, out_spike_test, out_stuck_value_test]], dtype=object)
    if routine_qc_iterations > 0:
        for iteration in range(1, routine_qc_iterations + 1):
            out_statistics = ';'.join(map(str, out_statistics_statistic_test[iteration]))
            out_statistics_line = np.append(out_statistics_line, np.array([[out_statistics]]), axis=1)
    out_statistics_data = np.append(out_statistics_data, out_statistics_line, axis=0)
    print(' Writing output statistics CSV file...')
    np.savetxt(out_statistics_file, out_statistics_data, fmt='"%s"', delimiter=',', comments='')

    print(' -------------------------')
    total_run_time = time.gmtime(calendar.timegm(time.gmtime()) - start_run_time)
    print(' Finished! Total elapsed time is: '
          + str(int(np.floor(calendar.timegm(total_run_time) / 86400.))) + ' days '
          + time.strftime('%H:%M:%S', total_run_time) + ' hh:mm:ss')


# Stand alone version
if os.path.basename(sys.argv[0]) == os.path.basename(__file__):
    # Load input arguments
    try:
        rejection_csv_file = sys.argv[1]
        routine_qc_iterations = int(sys.argv[2])
        out_statistics_file = sys.argv[3]
    except (IndexError, ValueError):
        rejection_csv_file = None
        routine_qc_iterations = None
        out_statistics_file = None

    try:
        verbose = string_to_bool(sys.argv[4])
    except (IndexError, ValueError):
        verbose = True

    rejection_statistics(rejection_csv_file, routine_qc_iterations, out_statistics_file, verbose)
