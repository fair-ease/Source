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
def time_check(in_file=None, verbose=True):
    # if __name__ == '__main__':
    #     return
    if verbose:
        print(' -------------------------' + ' ' + __file__ + ' -------------------------')
        print(' Script to compute time step verification in netCDF files.')
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
    time_step_array = in_time_data[1:] - in_time_data[: -1]
    (time_step_values, time_step_counts) = np.unique(time_step_array, return_counts=True)

    if verbose:
        for i in range(time_step_values.shape[0]):
            if time_step_counts[i] > 1:
                print(' Value: ' + str(int(time_step_values[i]))
                      + ' seconds, ' + str(time_step_counts[i]) + ' times.')
    zero_indices = np.argwhere(time_step_values == 0.0)
    time_step_counts = np.delete(time_step_counts, zero_indices)
    time_step_values = np.delete(time_step_values, zero_indices)
    time_step = time_step_values[np.argmax(time_step_counts)]

    if verbose:
        print(' Most representative time step: '
              + time.strftime("%H:%M:%S", time.gmtime(time_step)))

    unique_indices = np.unique(in_time_data, return_index=True)[1]
    duplicated_indices = np.unique(list(set(np.arange(in_time_data.shape[0])) - set(unique_indices)))
    is_unique = True
    if duplicated_indices.shape[0] > 0:
        is_unique = False
        unique_time_data = np.delete(in_time_data, duplicated_indices)
    else:
        unique_time_data = np.copy(in_time_data)
    sorted_unique_time_data = np.sort(unique_time_data)
    is_monotonic_increasing = True
    if not np.allclose(sorted_unique_time_data, unique_time_data):
        is_monotonic_increasing = False

    if not is_unique and not is_monotonic_increasing:
        if verbose:
            time.sleep(sleep_time)
            print(' Warning: duplicated entries and wrong positioning in time array.',
                  file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
        status = 3
    elif not is_monotonic_increasing:
        if verbose:
            time.sleep(sleep_time)
            print(' Warning: wrong positioning in time array.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
        status = 2
    elif not is_unique:
        if verbose:
            time.sleep(sleep_time)
            print(' Warning: duplicated entries in time array.', file=sys.stderr)
            time.sleep(sleep_time)
            print(' -------------------------')
        status = 1
    else:
        if verbose:
            print(' Time is monotonically increases without duplicates'
                  ' between one time step and another.')
        status = 0

    in_data.close()
    return status


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

    status = time_check(in_file, verbose)
    print(status)
