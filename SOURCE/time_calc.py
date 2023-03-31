import sys
import os
import time
import numpy as np
import netCDF4

# Global variables
sleep_time = 0.1  # seconds


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


# Functional version
def time_calc(in_file=None, verbose=True):
    # if __name__ == '__main__':
    #     return
    if verbose:
        print(' -------------------------' + ' ' + __file__ + ' -------------------------')
        print(' Script to compute most probable record sampling in netCDF files.')
        print(' -------------------------')
    if in_file is None:
        time.sleep(sleep_time)
        print(' Error: 1 of 2 maximum arguments (1 optional) not provided.', file=sys.stderr)
        print(' 1) input file;', file=sys.stderr)
        print(' 2) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
        time.sleep(sleep_time)
        return
    if verbose:
        print(' Input file = ' + in_file)
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
    if np.ma.is_masked(in_time_data):
        in_time_data = in_time_data[np.invert(in_time_data.mask)]
    in_time_data = np.sort(in_time_data)
    time_step_array = in_time_data[1:] - in_time_data[: -1]
    (time_step_values, time_step_counts) = np.unique(time_step_array, return_counts=True)
    if (len(time_step_values) > 0) and (0 in time_step_values):
        zero_value_index = np.where(time_step_values == 0)[0][0]
        time_step_values = np.delete(time_step_values, zero_value_index)
        time_step_counts = np.delete(time_step_counts, zero_value_index)
    if verbose:
        for i in range(time_step_values.shape[0]):
            if time_step_counts[i] > 1:
                print(' Value: ' + str(int(time_step_values[i]))
                      + ' seconds, ' + str(time_step_counts[i]) + ' times.')
    time_step = time_step_values[np.argmax(time_step_counts)]
    if verbose:
        print(' Most representative time step: '
              + time.strftime("%H:%M:%S", time.gmtime(time_step)))

    record_time_seconds = int(np.round(time_step))

    in_data.close()
    return record_time_seconds


# Stand alone version
if os.path.basename(sys.argv[0]) == os.path.basename(__file__):
    # Load input arguments
    try:
        in_file = sys.argv[1]

    except (IndexError, ValueError):
        in_file = None

    try:
        verbose = string_to_bool(sys.argv[2])
    except (IndexError, ValueError):
        verbose = True

    time_step = time_calc(in_file, verbose)
    print(time_step)
