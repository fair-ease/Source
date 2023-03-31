# -*- coding: utf-8 -*-
import sys
import os
import time
import numpy as np
import netCDF4
import calendar

# Global variables
sleep_time = 0.1  # seconds


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


# Functional version
def time_from_index(in_file=None, in_index=None, verbose=True):
    # if __name__ == '__main__':
    #     return
    if verbose:
        print(' -------------------------' + ' ' + __file__ + ' -------------------------')
        print(' Script to find time value from index from a netCDF observation file.')
        print(' -------------------------')
    if in_file is None or in_index is None:
        time.sleep(sleep_time)
        print(' Error: 2 of 3 maximum arguments (1 optional) not provided.', file=sys.stderr)
        print(' 1) input file;', file=sys.stderr)
        print(' 2) input file time index to find;', file=sys.stderr)
        print(' 3) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
        time.sleep(sleep_time)
        return
    if verbose:
        print(' Input file = ' + in_file)
        print(' Input index to find = ' + str(in_index))
        print(' Verbosity switch = ' + str(verbose))
        print(' -------------------------')

    in_data = netCDF4.Dataset(in_file, mode='r')
    try:
        in_time = in_data.variables['time']
    except KeyError:
        in_time = in_data.variables['TIME']
    time_reference = in_time.units
    if 'days' in time_reference:
        in_time_data = np.round(in_time[...] * 86400.)
    elif 'seconds' in time_reference:
        in_time_data = np.round(in_time[...])
    time_reference = time_reference[time_reference.find('since ') + len('since '):]
    reference_data = np.abs(calendar.timegm(time.strptime(time_reference, '%Y-%m-%dT%H:%M:%SZ')))

    if np.ma.is_masked(in_time_data[in_index]):
        in_time_data_value = -1
    else:
        try:
            in_time_data_value = int(np.round(in_time_data[in_index] - reference_data))
        except IndexError:
            in_time_data_value = -1

    if in_time_data_value > -1:
        out_time = time.gmtime(in_time_data_value)
        out_time_str = time.strftime('%Y-%m-%d %H:%M:%S', out_time)

        if verbose:
            print(' At time counter index ' + str(in_index) + ' date and time is: ' + out_time_str)
    else:
        if verbose:
            time.sleep(sleep_time)
            print(' Error. Time counter index ' + str(in_index) + ' is greater than time series size.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')

    in_data.close()

    return in_time_data_value


# Stand alone version
if os.path.basename(sys.argv[0]) == os.path.basename(__file__):
    # Load input arguments
    try:
        in_file = sys.argv[1]
        in_index = int(sys.argv[2])
    except (IndexError, ValueError):
        in_file = None
        in_index = None

    try:
        verbose = string_to_bool(sys.argv[3])
    except (IndexError, ValueError):
        verbose = True

    in_time_data_value = time_from_index(in_file, in_index, verbose)
    print(in_time_data_value)
