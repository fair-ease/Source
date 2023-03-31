# -*- coding: utf-8 -*-
import sys
import os
import time
import numpy as np
import netCDF4
from SOURCE import find_variable_name

# Global variables
sleep_time = 0.1  # seconds


def string_to_bool(string):
    if string == 'True':
        return True
    elif string == 'False':
        return False


# Functional version
def mean_variance_nc_variable(in_file=None, in_variable_standard_name=None, verbose=True):
    if __name__ == '__main__':
        return
    if verbose:
        print(' -------------------------' + ' ' + __file__ + ' -------------------------')
        print(' Script to compute variable average and variance over time dimension.')
        print(' -------------------------')
    if in_file is None or in_variable_standard_name is None:
        time.sleep(sleep_time)
        print(' ERROR: 2 of 3 maximum arguments (1 optional) not provided.', file=sys.stderr)
        print(' 1) input file;', file=sys.stderr)
        print(' 2) input variable standard_name;', file=sys.stderr)
        print(' 3) (optional) verbosity switch (True or False) (default: True).', file=sys.stderr)
        time.sleep(sleep_time)
        return
    if verbose:
        print(' Input file = ' + in_file)
        print(' Input variable standard_name = ' + in_variable_standard_name)
        print(' Verbosity switch = ' + str(verbose))
        print(' -------------------------')

    in_data = netCDF4.Dataset(in_file, mode='r')

    in_variable_name = find_variable_name.find_variable_name(in_file, 'standard_name',
                                                             in_variable_standard_name, verbose=False)
    try:
        in_variable = in_data.variables[in_variable_name]
    except KeyError:
        time.sleep(sleep_time)
        print(' Error. Input variable is not present in input dataset.', file=sys.stderr)
        time.sleep(sleep_time)
        print(' -------------------------')
        return

    in_variable_data = in_variable[...]
    in_variable_rounded_mean = np.around(np.mean(in_variable_data), decimals=3)
    in_variable_rounded_variance = np.around(np.var(in_variable_data), decimals=3)
    if verbose:
        print(' Mean of ' + in_variable_name + ' = ' + str(in_variable_rounded_mean))
        print(' Variance of ' + in_variable_name + ' = ' + str(in_variable_rounded_variance))

    in_data.close()

    return in_variable_rounded_mean, in_variable_rounded_variance


# Stand alone version
if os.path.basename(sys.argv[0]) == os.path.basename(__file__):
    # Load input arguments
    try:
        in_file = sys.argv[1]
        in_variable_standard_name = sys.argv[2]

    except (IndexError, ValueError):
        in_file = None
        in_variable_standard_name = None

    try:
        verbose = string_to_bool(sys.argv[3])
    except (IndexError, ValueError):
        verbose = True

    [in_variable_rounded_mean, in_variable_rounded_variance] = \
        mean_variance_nc_variable(in_file, in_variable_standard_name, verbose)
    print(in_variable_rounded_mean + ' ' + in_variable_rounded_variance)
